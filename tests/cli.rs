use std::fs;
use std::io::Read;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn native_watcher_follows_a_file_created_after_startup() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut child = Command::new(env!("CARGO_BIN_EXE_tailfany"))
        .arg("--no-headers")
        .arg(directory.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("start tailfany");
    let mut stdout = child.stdout.take().expect("child stdout");
    let (sender, receiver) = mpsc::channel();
    let reader = thread::spawn(move || {
        let mut buffer = [0_u8; 4096];
        loop {
            let count = stdout.read(&mut buffer).expect("read child stdout");
            if count == 0 {
                break;
            }
            if sender.send(buffer[..count].to_vec()).is_err() {
                break;
            }
        }
    });

    // Give the child time to install its watcher and take the initial snapshot.
    thread::sleep(Duration::from_millis(100));
    fs::write(directory.path().join("created.log"), b"created data\n")
        .expect("create matching file");

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut output = Vec::new();
    while Instant::now() < deadline && !output.windows(13).any(|window| window == b"created data\n")
    {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if let Ok(chunk) = receiver.recv_timeout(remaining.min(Duration::from_millis(250))) {
            output.extend(chunk);
        }
    }

    child.kill().expect("stop tailfany");
    child.wait().expect("wait for tailfany");
    reader.join().expect("join stdout reader");

    assert_eq!(output, b"created data\n");
}
