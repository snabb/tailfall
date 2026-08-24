use std::ffi::OsString;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "tailfall",
    version,
    about = "Follow new data in matching files, including files created later"
)]
struct Args {
    /// Glob pattern or directory to watch. Defaults to the current directory.
    #[arg(value_name = "PATTERN_OR_DIRECTORY")]
    operand: Option<OsString>,

    /// Do not print a filename header before output from each file.
    #[arg(long)]
    no_headers: bool,
}

fn main() {
    let args = Args::parse();
    let mode = if args.no_headers {
        tailfall::OutputMode::Raw
    } else {
        tailfall::OutputMode::Headers
    };

    if let Err(error) = tailfall::run(args.operand.as_deref(), mode) {
        eprintln!("tailfall: {error}");
        std::process::exit(1);
    }
}
