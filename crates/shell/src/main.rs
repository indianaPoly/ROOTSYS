use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use clap::Parser;
use clap::ValueEnum;
use common::PayloadFormat;
use drivers::{
    ApiKeyAuthConfig, ApiKeyLocation, BinaryFileDriver,
    CursorPaginationConfig as DriverCursorPaginationConfig, DbConfig, DbDriver, DbKind,
    ExternalSystem, InputSource, JsonlDriver, OAuth2ClientCredentialsAuthConfig,
    PagePaginationConfig as DriverPagePaginationConfig, PostgresTlsMode as DriverPostgresTlsMode,
    RestConfig, RestDriver, RestPaginationConfig as DriverRestPaginationConfig,
    RestPaginationKind as DriverRestPaginationKind, RestRetryConfig as DriverRestRetryConfig,
    TextLineDriver,
};
use runtime::{
    ApiKeyLocation as RuntimeApiKeyLocation, ContractRegistry, DbKind as RuntimeDbKind, DriverKind,
    ExternalInterface, IntegrationPipeline, PostgresTlsMode as RuntimePostgresTlsMode,
    RestAuthKind, RestPaginationKind as RuntimeRestPaginationKind,
};

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
    #[arg(long, default_value = "system/contracts/reference/allowlist.json")]
    contract_registry: PathBuf,
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
    let contract_registry = ContractRegistry::load(&args.contract_registry)?;
    interface.validate_against_registry(&contract_registry)?;
    let source = args
        .source
        .clone()
        .unwrap_or_else(|| interface.name.clone());

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
        let dlq_path = args.dlq.unwrap_or_else(|| with_suffix(&args.output, "dlq"));
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
        source_details: None,
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
        api_key_auth: rest.auth.as_ref().and_then(|auth| {
            if auth.kind == RestAuthKind::ApiKey {
                auth.api_key.as_ref().map(|api_key| ApiKeyAuthConfig {
                    location: match api_key.location {
                        RuntimeApiKeyLocation::Header => ApiKeyLocation::Header,
                        RuntimeApiKeyLocation::Query => ApiKeyLocation::Query,
                    },
                    name: api_key.name.clone(),
                    value: api_key.value.clone(),
                })
            } else {
                None
            }
        }),
        oauth2_auth: rest.auth.as_ref().and_then(|auth| {
            if auth.kind == RestAuthKind::OAuth2ClientCredentials {
                auth.oauth2_client_credentials.as_ref().map(|oauth2| {
                    OAuth2ClientCredentialsAuthConfig {
                        token_url: oauth2.token_url.clone(),
                        client_id: oauth2.client_id.clone(),
                        client_secret: oauth2.client_secret.clone(),
                        scope: oauth2.scope.clone(),
                    }
                })
            } else {
                None
            }
        }),
        pagination: rest
            .pagination
            .as_ref()
            .and_then(|pagination| match pagination.kind {
                RuntimeRestPaginationKind::Cursor => {
                    pagination
                        .cursor
                        .as_ref()
                        .map(|cursor| DriverRestPaginationConfig {
                            kind: DriverRestPaginationKind::Cursor,
                            cursor: Some(DriverCursorPaginationConfig {
                                cursor_param: cursor.cursor_param.clone(),
                                cursor_path: cursor.cursor_path.clone(),
                                initial_cursor: cursor.initial_cursor.clone(),
                                max_pages: cursor.max_pages,
                            }),
                            page: None,
                        })
                }
                RuntimeRestPaginationKind::Page => {
                    pagination
                        .page
                        .as_ref()
                        .map(|page| DriverRestPaginationConfig {
                            kind: DriverRestPaginationKind::Page,
                            cursor: None,
                            page: Some(DriverPagePaginationConfig {
                                page_param: page.page_param.clone(),
                                page_size_param: page.page_size_param.clone(),
                                page_size: page.page_size,
                                initial_page: page.initial_page,
                                max_pages: page.max_pages,
                            }),
                        })
                }
            }),
        retry: rest.retry.as_ref().map(|retry| DriverRestRetryConfig {
            max_attempts: retry.max_attempts,
            base_delay_ms: retry.base_delay_ms,
            max_delay_ms: retry.max_delay_ms,
            jitter_percent: retry.jitter_percent,
        }),
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
        postgres_tls_mode: db.postgres_tls_mode.map(|mode| match mode {
            RuntimePostgresTlsMode::Disable => DriverPostgresTlsMode::Disable,
            RuntimePostgresTlsMode::Require => DriverPostgresTlsMode::Require,
        }),
        pool_min_connections: db.pool.as_ref().and_then(|pool| pool.min_connections),
        pool_max_connections: db.pool.as_ref().and_then(|pool| pool.max_connections),
    })
}
