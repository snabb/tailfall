# tailfall

`tailfall` follows new data in all matching files, including files that
appear after the command starts. It is useful for watching a directory where
log files are created or rotated over time.

```sh
tailfall '*.log'
tailfall 'logs/*.log'
tailfall .
tailfall
```

Quote glob patterns so the shell does not expand them before `tailfall` sees
them.

Files that exist during the initial scan start at their current end. A file
discovered later is read from its beginning. File truncation and replacement
are detected and followed correctly. A directory operand watches only its
direct regular files; use an explicit recursive glob such as `**/*.log` to
watch nested directories.

Filename headers are printed by default, similar to multi-file `tail`:

```text
==> application.log <==
new log data
```

Use `--no-headers` when the output is being consumed as a raw byte stream.
Unreadable files and directories are skipped silently; use `--verbose` to
report filesystem errors while continuing to follow the other matches.
Stop the command with Ctrl-C.

## Installation

With Rust installed:

```sh
cargo install --path .
```

The filesystem watcher uses the native backend available on each supported
platform. Linux is the primary target; macOS and Windows are covered by CI.

## Development

```sh
cargo fmt --check
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
```
