use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use globset::{GlobBuilder, GlobMatcher};
use notify::{
    EventKind, RecursiveMode, Watcher,
    event::{CreateKind, ModifyKind, RemoveKind},
};
use same_file::Handle;

const RECONCILIATION_INTERVAL: Duration = Duration::from_secs(1);
const EVENT_RECONCILIATION_INTERVAL: Duration = Duration::from_millis(250);
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

    fn matching_paths(&self, verbose: bool) -> Result<Vec<PathBuf>, Error> {
        let mut paths = Vec::new();
        match self {
            Self::Directory { root } => {
                collect_paths(root, root, false, None, &mut paths, verbose)?
            }
            Self::Glob {
                root,
                matcher,
                recursive,
            } => collect_paths(root, root, *recursive, Some(matcher), &mut paths, verbose)?,
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

pub fn run(operand: Option<&OsStr>, output_mode: OutputMode, verbose: bool) -> Result<(), Error> {
    let spec = WatchSpec::from_operand(operand)?;
    let recursive = matches!(spec.recursive_mode(), RecursiveMode::Recursive);
    let changed = Arc::new(AtomicBool::new(false));
    let callback_changed = Arc::clone(&changed);
    let callback_verbose = verbose;
    let mut watcher =
        notify::recommended_watcher(move |result: Result<notify::Event, notify::Error>| {
            let should_reconcile = match result {
                Ok(event) => event_requires_reconciliation(&event, recursive),
                Err(error) => {
                    if callback_verbose {
                        eprintln!("tailfall: watcher error: {error}");
                    }
                    true
                }
            };
            if should_reconcile {
                callback_changed.store(true, Ordering::Release);
            }
        })?;
    if let Err(error) = watcher.watch(spec.root(), spec.recursive_mode())
        && verbose
    {
        eprintln!("tailfall: watcher error: {error}");
    }

    let stdout = io::stdout();
    let writer = BufWriter::new(stdout.lock());
    let mut engine = TailEngine::new(spec, output_mode, verbose, writer);
    engine.initialize()?;
    let mut next_reconcile = std::time::Instant::now() + RECONCILIATION_INTERVAL;

    loop {
        let now = std::time::Instant::now();
        let wait = next_reconcile
            .saturating_duration_since(now)
            .min(EVENT_RECONCILIATION_INTERVAL);
        if !wait.is_zero() {
            std::thread::sleep(wait);
        }

        let event_pending = changed.swap(false, Ordering::Acquire);
        if event_pending || std::time::Instant::now() >= next_reconcile {
            if !reconcile_result_or_exit(engine.reconcile())? {
                return Ok(());
            }
            next_reconcile = std::time::Instant::now()
                + if event_pending {
                    EVENT_RECONCILIATION_INTERVAL
                } else {
                    RECONCILIATION_INTERVAL
                };
        }
    }
}

fn event_requires_reconciliation(event: &notify::Event, recursive: bool) -> bool {
    match event.kind {
        // Reconciliation opens every tracked file.  Reacting to access events would make the
        // watcher trigger itself indefinitely.
        EventKind::Access(_) => false,
        // Metadata changes do not affect either the file contents or its matching path.
        EventKind::Modify(ModifyKind::Metadata(_)) => false,
        // A non-recursive watch cannot discover matching files inside a new directory.
        EventKind::Create(CreateKind::Folder) | EventKind::Remove(RemoveKind::Folder) => recursive,
        _ => true,
    }
}

fn reconcile_result_or_exit(result: Result<(), Error>) -> Result<bool, Error> {
    match result {
        Err(Error::Io(error)) if error.kind() == io::ErrorKind::BrokenPipe => Ok(false),
        result => result.map(|()| true),
    }
}

struct TailEngine<W> {
    spec: WatchSpec,
    output_mode: OutputMode,
    verbose: bool,
    files: BTreeMap<PathBuf, FileState>,
    startup_paths: BTreeSet<PathBuf>,
    last_output_path: Option<PathBuf>,
    writer: W,
}

impl<W: Write> TailEngine<W> {
    fn new(spec: WatchSpec, output_mode: OutputMode, verbose: bool, writer: W) -> Self {
        Self {
            spec,
            output_mode,
            verbose,
            files: BTreeMap::new(),
            startup_paths: BTreeSet::new(),
            last_output_path: None,
            writer,
        }
    }

    fn initialize(&mut self) -> Result<(), Error> {
        let paths = self.spec.matching_paths(self.verbose)?;
        self.startup_paths.extend(paths.iter().cloned());
        for path in paths {
            if let Some(state) = self.open_at_end(&path)? {
                self.files.insert(path, state);
            }
        }
        Ok(())
    }

    fn reconcile(&mut self) -> Result<(), Error> {
        let paths = self.spec.matching_paths(self.verbose)?;
        let present: BTreeSet<_> = paths.iter().cloned().collect();

        for path in paths {
            let previous = self.files.get(&path);
            if let Some(state) = Self::read_new_data(
                &mut self.writer,
                self.output_mode,
                &mut self.last_output_path,
                &path,
                previous,
                self.startup_paths.contains(&path),
                self.verbose,
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
                if self.verbose {
                    eprintln!("tailfall: cannot open {}: {error}", path.display());
                }
                return Ok(None);
            }
        };
        let offset = match file.metadata() {
            Ok(metadata) => metadata.len(),
            Err(error) => {
                report_file_error(self.verbose, path, "inspect", &error);
                return Ok(None);
            }
        };
        let identity = match file.try_clone().and_then(Handle::from_file) {
            Ok(identity) => identity,
            Err(error) => {
                report_file_error(self.verbose, path, "inspect", &error);
                return Ok(None);
            }
        };
        Ok(Some(FileState { identity, offset }))
    }

    fn read_new_data(
        writer: &mut W,
        output_mode: OutputMode,
        last_output_path: &mut Option<PathBuf>,
        path: &Path,
        previous: Option<&FileState>,
        was_present_at_startup: bool,
        verbose: bool,
    ) -> Result<Option<FileState>, Error> {
        let mut file = match OpenOptions::new().read(true).open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                if verbose {
                    eprintln!("tailfall: cannot open {}: {error}", path.display());
                }
                return Ok(None);
            }
        };

        let metadata = match file.metadata() {
            Ok(metadata) => metadata,
            Err(error) => {
                report_file_error(verbose, path, "inspect", &error);
                return Ok(None);
            }
        };
        let length = metadata.len();
        let identity = match file.try_clone().and_then(Handle::from_file) {
            Ok(identity) => identity,
            Err(error) => {
                report_file_error(verbose, path, "inspect", &error);
                return Ok(None);
            }
        };
        // A path found in the startup inventory may have been temporarily unreadable.  If it
        // becomes readable later, start at EOF rather than replaying its old contents.
        let mut offset = previous.map_or_else(
            || {
                if was_present_at_startup { length } else { 0 }
            },
            |state| state.offset,
        );
        if previous.is_some_and(|state| state.identity != identity) || length < offset {
            offset = 0;
        }

        if let Err(error) = file.seek(SeekFrom::Start(offset)) {
            report_file_error(verbose, path, "seek", &error);
            return Ok(None);
        }
        let mut buffer = [0_u8; READ_BUFFER_SIZE];
        loop {
            let read = match file.read(&mut buffer) {
                Ok(read) => read,
                Err(error) => {
                    report_file_error(verbose, path, "read", &error);
                    return Ok(None);
                }
            };
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
    verbose: bool,
) -> Result<(), Error> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) => {
            report_path_error(verbose, directory, "read directory", &error);
            return Ok(());
        }
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                report_path_error(verbose, directory, "inspect directory entry", &error);
                continue;
            }
        };
        let path = entry.path();
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) => {
                report_path_error(verbose, &path, "inspect", &error);
                continue;
            }
        };
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
            collect_paths(&path, root, true, matcher, paths, verbose)?;
        }
    }
    Ok(())
}

fn report_file_error(verbose: bool, path: &Path, operation: &str, error: &io::Error) {
    if verbose {
        eprintln!("tailfall: cannot {operation} {}: {error}", path.display());
    }
}

fn report_path_error(verbose: bool, path: &Path, operation: &str, error: &io::Error) {
    if verbose {
        eprintln!("tailfall: cannot {operation} {}: {error}", path.display());
    }
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
    fn irrelevant_watcher_events_are_filtered() {
        assert!(!event_requires_reconciliation(
            &notify::Event::new(EventKind::Access(notify::event::AccessKind::Read)),
            false,
        ));
        assert!(!event_requires_reconciliation(
            &notify::Event::new(EventKind::Modify(ModifyKind::Metadata(
                notify::event::MetadataKind::Any,
            ))),
            false,
        ));
        assert!(!event_requires_reconciliation(
            &notify::Event::new(EventKind::Create(CreateKind::Folder)),
            false,
        ));
        assert!(!event_requires_reconciliation(
            &notify::Event::new(EventKind::Remove(RemoveKind::Folder)),
            false,
        ));
        assert!(event_requires_reconciliation(
            &notify::Event::new(EventKind::Create(CreateKind::Folder)),
            true,
        ));
        assert!(event_requires_reconciliation(
            &notify::Event::new(EventKind::Modify(ModifyKind::Data(
                notify::event::DataChange::Any,
            ))),
            false,
        ));
        assert!(event_requires_reconciliation(
            &notify::Event::new(EventKind::Modify(ModifyKind::Name(
                notify::event::RenameMode::Any,
            ))),
            false,
        ));
        assert!(event_requires_reconciliation(
            &notify::Event::new(EventKind::Other),
            false,
        ));
    }

    #[test]
    fn existing_file_starts_at_end_and_appends_are_emitted() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("application.log");
        fs::write(&file, b"old\n").unwrap();
        let spec = WatchSpec::from_operand(Some(temp.path().as_os_str())).unwrap();
        let mut engine = TailEngine::new(spec, OutputMode::Raw, false, Vec::new());

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
        let mut engine = TailEngine::new(spec, OutputMode::Raw, false, Vec::new());
        engine.initialize().unwrap();

        fs::write(temp.path().join("new.log"), b"created\n").unwrap();
        engine.reconcile().unwrap();
        assert_eq!(output(&engine), "created\n");
    }

    #[test]
    fn startup_file_that_could_not_be_opened_starts_at_end() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("existing.log");
        fs::write(&file, b"old\n").unwrap();

        let pattern = temp.path().join("*.log");
        let spec = WatchSpec::from_operand(Some(pattern.as_os_str())).unwrap();
        let mut engine = TailEngine::new(spec, OutputMode::Raw, false, Vec::new());
        engine.initialize().unwrap();
        engine.files.remove(&file);
        engine.reconcile().unwrap();

        assert!(engine.writer.is_empty());
    }

    #[test]
    fn headers_can_be_disabled() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("application.log");
        fs::write(&file, b"old").unwrap();
        let spec = WatchSpec::from_operand(Some(temp.path().as_os_str())).unwrap();
        let mut engine = TailEngine::new(spec, OutputMode::Headers, false, Vec::new());
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
        let mut engine = TailEngine::new(spec, OutputMode::Headers, false, Vec::new());
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
        let mut engine = TailEngine::new(spec, OutputMode::Raw, false, Vec::new());
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
        let mut engine = TailEngine::new(spec, OutputMode::Raw, false, Vec::new());
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
        let mut engine = TailEngine::new(spec, OutputMode::Raw, false, Vec::new());
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
        let mut engine = TailEngine::new(spec, OutputMode::Raw, false, Vec::new());
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
        let mut engine = TailEngine::new(spec, OutputMode::Raw, false, Vec::new());
        engine.initialize().unwrap();

        fs::write(&file, b"oldnew").unwrap();
        engine.reconcile().unwrap();
        assert!(engine.writer.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn inaccessible_recursive_directory_is_skipped() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let accessible = temp.path().join("accessible.log");
        fs::write(&accessible, b"accessible").unwrap();
        let inaccessible = temp.path().join("inaccessible");
        fs::create_dir(&inaccessible).unwrap();
        fs::write(inaccessible.join("hidden.log"), b"hidden").unwrap();

        let mut permissions = fs::metadata(&inaccessible).unwrap().permissions();
        permissions.set_mode(0o000);
        fs::set_permissions(&inaccessible, permissions).unwrap();

        let pattern = temp.path().join("**/*.log");
        let spec = WatchSpec::from_operand(Some(pattern.as_os_str())).unwrap();
        let paths = spec.matching_paths(false).unwrap();

        // The test may run as root, in which case mode 000 is still readable.
        if paths.iter().any(|path| path.ends_with("hidden.log")) {
            let mut permissions = fs::metadata(&inaccessible).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&inaccessible, permissions).unwrap();
            return;
        }
        assert!(paths.iter().any(|path| path.ends_with("accessible.log")));
        assert!(!paths.iter().any(|path| path.ends_with("hidden.log")));

        let mut permissions = fs::metadata(&inaccessible).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&inaccessible, permissions).unwrap();
    }
}
