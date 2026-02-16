use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "rootsys-merge")]
#[command(about = "Merge integration outputs", long_about = None)]
struct Args {
    /// Input JSONL files emitted by the integration pipeline.
    #[arg(long, required = true)]
    inputs: Vec<PathBuf>,
    /// Output JSONL file.
    #[arg(long)]
    output: PathBuf,
    /// Deduplicate by (source, interface, record_id).
    #[arg(long, default_value_t = true)]
    dedupe: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let stats = fabric::merge_jsonl_files(&args.inputs, &args.output, args.dedupe)?;

    println!(
        "total: {} | written: {} | duplicates: {}",
        stats.total, stats.written, stats.duplicates
    );

    Ok(())
}
