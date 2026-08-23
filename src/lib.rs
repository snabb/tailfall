use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::Duration;

use globset::{GlobBuilder, GlobMatcher};
use notify::{RecursiveMode, Watcher};
use same_file::Handle;

const RECONCILIATION_INTERVAL: Duration = Duration::from_secs(1);
const READ_BUFFER_SIZE: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutputMode {
    Headers,
    Raw,
}

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Notify(notify::Error),
    InvalidOperand(String),
    InvalidPattern(String),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "{error}"),
            Self::Notify(error) => write!(formatter, "{error}"),
            Self::InvalidOperand(error) | Self::InvalidPattern(error) => {
                write!(formatter, "{error}")
            }
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<notify::Error> for Error {
    fn from(error: notify::Error) -> Self {
        Self::Notify(error)
    }
}

#[derive(Debug)]
enum WatchSpec {
    Directory {
        root: PathBuf,
    },
    Glob {
        root: PathBuf,
        matcher: GlobMatcher,
        recursive: bool,
    },
}

impl WatchSpec {
    fn from_operand(operand: Option<&OsStr>) -> Result<Self, Error> {
        let operand = operand
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        let absolute_operand = make_absolute(&operand)?;

        if fs::metadata(&absolute_operand)
            .map(|metadata| metadata.is_dir())
            .unwrap_or(false)
        {
            return Ok(Self::Directory {
                root: absolute_operand,
            });
        }

        let pattern = operand
            .to_str()
            .ok_or_else(|| Error::InvalidOperand("the pattern must be valid UTF-8".to_owned()))?;
        let has_magic = pattern_has_magic(pattern);
        let root_candidate = if has_magic {
            static_prefix(&absolute_operand)
        } else {
            absolute_operand
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from("."))
        };
        let root = existing_directory_ancestor(&root_candidate)?;
        let relative_pattern = absolute_operand
            .strip_prefix(&root)
            .unwrap_or(&absolute_operand);
        let relative_pattern = relative_pattern
            .to_str()
            .ok_or_else(|| Error::InvalidOperand("the pattern must be valid UTF-8".to_owned()))?;

        let matcher = GlobBuilder::new(relative_pattern)
            .literal_separator(true)
            .build()
            .map_err(|error| Error::InvalidPattern(error.to_string()))?
            .compile_matcher();

        let components: Vec<_> = Path::new(relative_pattern).components().collect();
        let recursive = root != root_candidate
            || relative_pattern.contains("**")
            || components
                .iter()
                .take(components.len().saturating_sub(1))
                .any(|component| component_has_magic(*component));

        Ok(Self::Glob {
            root,
            matcher,
            recursive,
        })
    }

    fn root(&self) -> &Path {
        match self {
            Self::Directory { root } | Self::Glob { root, .. } => root,
        }
    }

    fn recursive_mode(&self) -> RecursiveMode {
        match self {
            Self::Directory { .. } => RecursiveMode::NonRecursive,
            Self::Glob { recursive, .. } => {
                if *recursive {
                    RecursiveMode::Recursive
                } else {
                    RecursiveMode::NonRecursive
                }
            }
        }
    }

    fn matching_paths(&self) -> Result<Vec<PathBuf>, Error> {
        let mut paths = Vec::new();
        match self {
            Self::Directory { root } => collect_paths(root, root, false, None, &mut paths)?,
            Self::Glob {
                root,
                matcher,
                recursive,
            } => collect_paths(root, root, *recursive, Some(matcher), &mut paths)?,
        }
        paths.sort();
        Ok(paths)
    }
}

#[derive(Debug)]
struct FileState {
    identity: Handle,
    offset: u64,
}

pub fn run(operand: Option<&OsStr>, output_mode: OutputMode) -> Result<(), Error> {
    let spec = WatchSpec::from_operand(operand)?;
    let (sender, receiver) = mpsc::channel();
    let mut watcher = notify::recommended_watcher(sender)?;
    watcher.watch(spec.root(), spec.recursive_mode())?;

    let stdout = io::stdout();
    let writer = BufWriter::new(stdout.lock());
    let mut engine = TailEngine::new(spec, output_mode, writer);
    engine.initialize()?;

    loop {
        match receiver.recv_timeout(RECONCILIATION_INTERVAL) {
            Ok(Ok(_event)) => {
                while receiver.try_recv().is_ok() {}
                if !reconcile_or_exit(&mut engine)? {
                    return Ok(());
                }
            }
            Ok(Err(error)) => {
                eprintln!("tailfany: watcher error: {error}");
                if !reconcile_or_exit(&mut engine)? {
                    return Ok(());
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                if !reconcile_or_exit(&mut engine)? {
                    return Ok(());
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                return Err(Error::InvalidOperand(
                    "the filesystem watcher stopped unexpectedly".to_owned(),
                ));
            }
        }
    }
}

fn reconcile_or_exit<W: Write>(engine: &mut TailEngine<W>) -> Result<bool, Error> {
    match engine.reconcile() {
        Err(Error::Io(error)) if error.kind() == io::ErrorKind::BrokenPipe => Ok(false),
        result => result.map(|()| true),
    }
}

struct TailEngine<W> {
    spec: WatchSpec,
    output_mode: OutputMode,
    files: BTreeMap<PathBuf, FileState>,
    last_output_path: Option<PathBuf>,
    writer: W,
}

impl<W: Write> TailEngine<W> {
    fn new(spec: WatchSpec, output_mode: OutputMode, writer: W) -> Self {
        Self {
            spec,
            output_mode,
            files: BTreeMap::new(),
            last_output_path: None,
            writer,
        }
    }

    fn initialize(&mut self) -> Result<(), Error> {
        for path in self.spec.matching_paths()? {
            if let Some(state) = self.open_at_end(&path)? {
                self.files.insert(path, state);
            }
        }
        Ok(())
    }

    fn reconcile(&mut self) -> Result<(), Error> {
        let paths = self.spec.matching_paths()?;
        let present: BTreeSet<_> = paths.iter().cloned().collect();

        for path in paths {
            let previous = self.files.get(&path);
            if let Some(state) = Self::read_new_data(
                &mut self.writer,
                self.output_mode,
                &mut self.last_output_path,
                &path,
                previous,
            )? {
                self.files.insert(path, state);
            }
        }

        self.files.retain(|path, _| present.contains(path));
        self.writer.flush().map_err(Error::Io)
    }

    fn open_at_end(&self, path: &Path) -> Result<Option<FileState>, Error> {
        let file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                eprintln!("tailfany: cannot open {}: {error}", path.display());
                return Ok(None);
            }
        };
        let offset = file.metadata()?.len();
        let identity = Handle::from_file(file.try_clone()?).map_err(Error::Io)?;
        Ok(Some(FileState { identity, offset }))
    }

    fn read_new_data(
        writer: &mut W,
        output_mode: OutputMode,
        last_output_path: &mut Option<PathBuf>,
        path: &Path,
        previous: Option<&FileState>,
    ) -> Result<Option<FileState>, Error> {
        let mut file = match OpenOptions::new().read(true).open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                eprintln!("tailfany: cannot open {}: {error}", path.display());
                return Ok(None);
            }
        };

        let length = file.metadata()?.len();
        let identity = Handle::from_file(file.try_clone()?).map_err(Error::Io)?;
        let mut offset = previous.map_or(0, |state| state.offset);
        if previous.is_some_and(|state| state.identity != identity) || length < offset {
            offset = 0;
        }

        file.seek(SeekFrom::Start(offset))?;
        let mut buffer = [0_u8; READ_BUFFER_SIZE];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            if output_mode == OutputMode::Headers && last_output_path.as_deref() != Some(path) {
                writeln!(writer, "==> {} <==", path.display())?;
                *last_output_path = Some(path.to_path_buf());
            }
            writer.write_all(&buffer[..read])?;
            offset += read as u64;
        }

        Ok(Some(FileState { identity, offset }))
    }
}

fn collect_paths(
    directory: &Path,
    root: &Path,
    recursive: bool,
    matcher: Option<&GlobMatcher>,
    paths: &mut Vec<PathBuf>,
) -> Result<(), Error> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_file() {
            let matches = matcher
                .map(|matcher| {
                    path.strip_prefix(root)
                        .map(|relative| matcher.is_match(relative))
                        .unwrap_or(false)
                })
                .unwrap_or(true);
            if matches {
                paths.push(path);
            }
        } else if recursive && file_type.is_dir() {
            collect_paths(&path, root, true, matcher, paths)?;
        }
    }
    Ok(())
}

fn make_absolute(path: &Path) -> Result<PathBuf, Error> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn existing_directory_ancestor(path: &Path) -> Result<PathBuf, Error> {
    let mut candidate = path.to_path_buf();
    loop {
        if candidate.is_dir() {
            return Ok(candidate);
        }
        if !candidate.pop() {
            return Err(Error::InvalidOperand(format!(
                "no existing directory for {}",
                path.display()
            )));
        }
    }
}

fn static_prefix(path: &Path) -> PathBuf {
    let mut prefix = PathBuf::new();
    for component in path.components() {
        if component_has_magic(component) {
            break;
        }
        prefix.push(component.as_os_str());
    }
    if prefix.as_os_str().is_empty() {
        PathBuf::from(std::path::MAIN_SEPARATOR.to_string())
    } else {
        prefix
    }
}

fn component_has_magic(component: Component<'_>) -> bool {
    component
        .as_os_str()
        .to_string_lossy()
        .chars()
        .any(|character| matches!(character, '*' | '?' | '[' | '{'))
}

fn pattern_has_magic(pattern: &str) -> bool {
    pattern
        .chars()
        .any(|character| matches!(character, '*' | '?' | '[' | '{'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, OpenOptions};
    use std::io::Write;

    fn output(engine: &TailEngine<Vec<u8>>) -> String {
        String::from_utf8(engine.writer.clone()).expect("UTF-8 test output")
    }

    #[test]
    fn existing_file_starts_at_end_and_appends_are_emitted() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("application.log");
        fs::write(&file, b"old\n").unwrap();
        let spec = WatchSpec::from_operand(Some(temp.path().as_os_str())).unwrap();
        let mut engine = TailEngine::new(spec, OutputMode::Raw, Vec::new());

        engine.initialize().unwrap();
        assert!(engine.writer.is_empty());

        OpenOptions::new()
            .append(true)
            .open(&file)
            .unwrap()
            .write_all(b"new\n")
            .unwrap();
        engine.reconcile().unwrap();
        assert_eq!(output(&engine), "new\n");
    }

    #[test]
    fn new_file_is_read_from_the_beginning() {
        let temp = tempfile::tempdir().unwrap();
        let spec = WatchSpec::from_operand(Some(temp.path().as_os_str())).unwrap();
        let mut engine = TailEngine::new(spec, OutputMode::Raw, Vec::new());
        engine.initialize().unwrap();

        fs::write(temp.path().join("new.log"), b"created\n").unwrap();
        engine.reconcile().unwrap();
        assert_eq!(output(&engine), "created\n");
    }

    #[test]
    fn headers_can_be_disabled() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("application.log");
        fs::write(&file, b"old").unwrap();
        let spec = WatchSpec::from_operand(Some(temp.path().as_os_str())).unwrap();
        let mut engine = TailEngine::new(spec, OutputMode::Headers, Vec::new());
        engine.initialize().unwrap();
        fs::write(&file, b"oldnew").unwrap();
        engine.reconcile().unwrap();

        assert!(output(&engine).contains("==> "));
        assert!(output(&engine).ends_with("new"));
    }

    #[test]
    fn headers_are_repeated_only_when_output_switches_files() {
        let temp = tempfile::tempdir().unwrap();
        let first = temp.path().join("first.log");
        let second = temp.path().join("second.log");
        fs::write(&first, b"old").unwrap();
        fs::write(&second, b"old").unwrap();
        let spec = WatchSpec::from_operand(Some(temp.path().as_os_str())).unwrap();
        let mut engine = TailEngine::new(spec, OutputMode::Headers, Vec::new());
        engine.initialize().unwrap();

        OpenOptions::new()
            .append(true)
            .open(&first)
            .unwrap()
            .write_all(b"first\n")
            .unwrap();
        engine.reconcile().unwrap();
        OpenOptions::new()
            .append(true)
            .open(&first)
            .unwrap()
            .write_all(b"second\n")
            .unwrap();
        engine.reconcile().unwrap();
        assert_eq!(output(&engine).matches("==> ").count(), 1);

        OpenOptions::new()
            .append(true)
            .open(&second)
            .unwrap()
            .write_all(b"other\n")
            .unwrap();
        engine.reconcile().unwrap();
        assert_eq!(output(&engine).matches("==> ").count(), 2);

        OpenOptions::new()
            .append(true)
            .open(&first)
            .unwrap()
            .write_all(b"third\n")
            .unwrap();
        engine.reconcile().unwrap();
        assert_eq!(output(&engine).matches("==> ").count(), 3);
    }

    #[test]
    fn truncation_resets_the_offset() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("application.log");
        fs::write(&file, b"old contents").unwrap();
        let spec = WatchSpec::from_operand(Some(temp.path().as_os_str())).unwrap();
        let mut engine = TailEngine::new(spec, OutputMode::Raw, Vec::new());
        engine.initialize().unwrap();

        fs::write(&file, b"replacement").unwrap();
        engine.reconcile().unwrap();
        assert_eq!(output(&engine), "replacement");
    }

    #[test]
    fn replacement_file_is_followed_from_zero() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("application.log");
        fs::write(&file, b"old contents").unwrap();
        let spec = WatchSpec::from_operand(Some(temp.path().as_os_str())).unwrap();
        let mut engine = TailEngine::new(spec, OutputMode::Raw, Vec::new());
        engine.initialize().unwrap();

        fs::remove_file(&file).unwrap();
        fs::write(&file, b"replacement").unwrap();
        engine.reconcile().unwrap();
        assert_eq!(output(&engine), "replacement");
    }

    #[test]
    fn glob_matches_only_requested_files() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("one.log"), b"one").unwrap();
        fs::write(temp.path().join("two.txt"), b"two").unwrap();
        let pattern = temp.path().join("*.log");
        let spec = WatchSpec::from_operand(Some(pattern.as_os_str())).unwrap();
        let mut engine = TailEngine::new(spec, OutputMode::Raw, Vec::new());
        engine.initialize().unwrap();

        OpenOptions::new()
            .append(true)
            .open(temp.path().join("one.log"))
            .unwrap()
            .write_all(b"-new")
            .unwrap();
        OpenOptions::new()
            .append(true)
            .open(temp.path().join("two.txt"))
            .unwrap()
            .write_all(b"-new")
            .unwrap();
        engine.reconcile().unwrap();
        assert_eq!(output(&engine), "-new");
    }

    #[test]
    fn explicit_recursive_glob_reaches_nested_files() {
        let temp = tempfile::tempdir().unwrap();
        let nested = temp.path().join("nested");
        fs::create_dir(&nested).unwrap();
        let file = nested.join("application.log");
        fs::write(&file, b"old").unwrap();
        let pattern = temp.path().join("**/*.log");
        let spec = WatchSpec::from_operand(Some(pattern.as_os_str())).unwrap();
        let mut engine = TailEngine::new(spec, OutputMode::Raw, Vec::new());
        engine.initialize().unwrap();

        fs::write(&file, b"oldnew").unwrap();
        engine.reconcile().unwrap();
        assert_eq!(output(&engine), "new");
    }

    #[test]
    fn directory_operand_does_not_recurse() {
        let temp = tempfile::tempdir().unwrap();
        let nested = temp.path().join("nested");
        fs::create_dir(&nested).unwrap();
        let file = nested.join("application.log");
        fs::write(&file, b"old").unwrap();
        let spec = WatchSpec::from_operand(Some(temp.path().as_os_str())).unwrap();
        let mut engine = TailEngine::new(spec, OutputMode::Raw, Vec::new());
        engine.initialize().unwrap();

        fs::write(&file, b"oldnew").unwrap();
        engine.reconcile().unwrap();
        assert!(engine.writer.is_empty());
    }
}
