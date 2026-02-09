use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use clap::Parser;
use clap::ValueEnum;
use common::PayloadFormat;
use drivers::{
    BinaryFileDriver, DbConfig, DbDriver, DbKind, ExternalSystem, InputSource, JsonlDriver,
    RestConfig, RestDriver, TextLineDriver,
};
use runtime::{DbKind as RuntimeDbKind, DriverKind, ExternalInterface, IntegrationPipeline};

#[derive(Debug, Parser)]
#[command(name = "rootsys-shell")]
#[command(about = "Data integration pipeline runner", long_about = None)]
struct Args {
    #[arg(long)]
    interface: PathBuf,
    #[arg(long)]
    input: Option<PathBuf>,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    dlq: Option<PathBuf>,
    #[arg(long)]
    source: Option<String>,
    #[arg(long, value_enum, default_value_t = InputFormat::Auto)]
    format: InputFormat,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum InputFormat {
    Auto,
    Jsonl,
    Text,
    Binary,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let interface = ExternalInterface::load(&args.interface)?;
    let source = args.source.clone().unwrap_or_else(|| interface.name.clone());

    let driver_kind = match args.format {
        InputFormat::Auto => interface.driver.kind,
        InputFormat::Jsonl => DriverKind::Jsonl,
        InputFormat::Text => DriverKind::Text,
        InputFormat::Binary => DriverKind::Binary,
    };

    let metadata = metadata_from_interface(&interface);

    let mut driver: Box<dyn ExternalSystem> = match driver_kind {
        DriverKind::Jsonl => {
            let input_source = resolve_input(&args.input, &interface)?;
            Box::new(JsonlDriver::new(input_source, metadata))
        }
        DriverKind::Text => {
            let input_source = resolve_input(&args.input, &interface)?;
            Box::new(TextLineDriver::new(input_source, metadata))
        }
        DriverKind::Binary => {
            let input_source = resolve_input(&args.input, &interface)?;
            Box::new(BinaryFileDriver::new(input_source, metadata))
        }
        DriverKind::Rest => {
            let config = rest_config_from_interface(&interface)?;
            Box::new(RestDriver::new(config, metadata))
        }
        DriverKind::Db => {
            let config = db_config_from_interface(&interface)?;
            Box::new(DbDriver::new(config, metadata))
        }
    };
    let records = driver.fetch()?;

    let pipeline = IntegrationPipeline::new(interface);
    let outcome = pipeline.integrate(&source, records);

    write_jsonl(&args.output, &outcome.records)?;

    if !outcome.dead_letters.is_empty() {
        let dlq_path = args
            .dlq
            .unwrap_or_else(|| with_suffix(&args.output, "dlq"));
        write_jsonl(&dlq_path, &outcome.dead_letters)?;
    }

    println!(
        "records: {} | dead_letters: {}",
        outcome.records.len(),
        outcome.dead_letters.len()
    );

    Ok(())
}

/// Write JSONL output to disk.
fn write_jsonl<T: serde::Serialize>(path: &PathBuf, rows: &[T]) -> Result<(), std::io::Error> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);

    for row in rows {
        let line = serde_json::to_string(row).unwrap_or_else(|_| "{}".to_string());
        writeln!(writer, "{}", line)?;
    }

    Ok(())
}

/// Append a suffix before the extension (used for DLQ files).
fn with_suffix(path: &PathBuf, suffix: &str) -> PathBuf {
    let mut new_path = path.clone();
    if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
        let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
        let filename = if ext.is_empty() {
            format!("{}.{}", stem, suffix)
        } else {
            format!("{}.{}.{}", stem, suffix, ext)
        };
        new_path.set_file_name(filename);
    }
    new_path
}

/// Resolve input for file-based drivers (supports "-" for stdin).
fn resolve_input(
    input: &Option<PathBuf>,
    interface: &ExternalInterface,
) -> Result<InputSource, Box<dyn std::error::Error>> {
    if let Some(path) = input {
        return Ok(InputSource::from_path(path.clone()));
    }

    if let Some(path) = &interface.driver.input {
        return Ok(InputSource::from_str(path));
    }

    Err("input path is required (use --input or interface.driver.input)".into())
}

/// Build default metadata from the interface driver configuration.
fn metadata_from_interface(interface: &ExternalInterface) -> common::RecordMetadata {
    common::RecordMetadata {
        content_type: interface.driver.content_type.clone(),
        filename: interface.driver.filename.clone(),
    }
}

/// Build REST driver config from the interface definition.
fn rest_config_from_interface(
    interface: &ExternalInterface,
) -> Result<RestConfig, Box<dyn std::error::Error>> {
    let rest = interface
        .driver
        .rest
        .as_ref()
        .ok_or("rest driver config is required")?;

    Ok(RestConfig {
        url: rest.url.clone(),
        method: rest.method.clone(),
        headers: rest.headers.clone(),
        body: rest.body.clone(),
        timeout_ms: rest.timeout_ms,
        response_format: rest.response_format.unwrap_or(PayloadFormat::Unknown),
        items_pointer: rest.items_pointer.clone(),
    })
}

/// Build DB driver config from the interface definition.
fn db_config_from_interface(
    interface: &ExternalInterface,
) -> Result<DbConfig, Box<dyn std::error::Error>> {
    let db = interface
        .driver
        .db
        .as_ref()
        .ok_or("db driver config is required")?;

    let kind = match db.kind {
        RuntimeDbKind::Sqlite => DbKind::Sqlite,
        RuntimeDbKind::Postgres => DbKind::Postgres,
        RuntimeDbKind::Mysql => DbKind::Mysql,
    };

    Ok(DbConfig {
        kind,
        connection: db.connection.clone(),
        query: db.query.clone(),
    })
}
