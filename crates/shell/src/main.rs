use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::Parser;
use clap::ValueEnum;
use common::{DeadLetter, ExternalRecord, InterfaceRef, PayloadFormat, RecordMetadata};
use drivers::{
    ApiKeyAuthConfig, ApiKeyLocation, BinaryFileDriver,
    CircuitBreakerConfig as DriverCircuitBreakerConfig,
    CursorPaginationConfig as DriverCursorPaginationConfig, DbConfig, DbDriver, DbKind,
    DbRetryConfig as DriverDbRetryConfig, ExternalSystem, InputSource, JsonlDriver,
    KafkaStreamConfig as DriverKafkaStreamConfig, OAuth2ClientCredentialsAuthConfig,
    PagePaginationConfig as DriverPagePaginationConfig, PostgresTlsMode as DriverPostgresTlsMode,
    RestConfig, RestDriver, RestPaginationConfig as DriverRestPaginationConfig,
    RestPaginationKind as DriverRestPaginationKind, RestRetryConfig as DriverRestRetryConfig,
    StreamConfig as DriverStreamConfig, StreamDriver, StreamSourceKind as DriverStreamSourceKind,
    StreamStartOffset as DriverStreamStartOffset, TextLineDriver,
};
use runtime::{
    ApiKeyLocation as RuntimeApiKeyLocation, ContractRegistry, DbKind as RuntimeDbKind, DriverKind,
    ExternalInterface, IntegrationPipeline, PostgresTlsMode as RuntimePostgresTlsMode,
    RestAuthKind, RestPaginationKind as RuntimeRestPaginationKind,
    StreamSourceKind as RuntimeStreamSourceKind, StreamStartOffset as RuntimeStreamStartOffset,
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
    #[arg(long, value_enum, default_value_t = DlqSinkKind::File)]
    dlq_sink: DlqSinkKind,
    #[arg(long, default_value = "dead_letters")]
    dlq_table: String,
    #[arg(long)]
    source: Option<String>,
    #[arg(long)]
    replay_dlq: Option<PathBuf>,
    #[arg(long, value_enum, default_value_t = DlqSinkKind::File)]
    replay_dlq_source: DlqSinkKind,
    #[arg(long, default_value = "dead_letters")]
    replay_dlq_table: String,
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

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum DlqSinkKind {
    File,
    Sqlite,
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
    let records = if let Some(replay_dlq_path) = &args.replay_dlq {
        load_replay_records(&args, replay_dlq_path)?
    } else {
        let mut driver = build_external_driver(&args, &interface, driver_kind, metadata)?;
        driver.fetch()?
    };

    let pipeline = IntegrationPipeline::new(interface);
    let outcome = pipeline.integrate(&source, records);

    write_jsonl(&args.output, &outcome.records)?;

    if !outcome.dead_letters.is_empty() {
        let dlq_sink = build_dlq_sink(&args);
        dlq_sink.write(&outcome.dead_letters)?;
    }

    println!(
        "records: {} | dead_letters: {}",
        outcome.records.len(),
        outcome.dead_letters.len()
    );

    Ok(())
}

fn build_external_driver(
    args: &Args,
    interface: &ExternalInterface,
    driver_kind: DriverKind,
    metadata: RecordMetadata,
) -> Result<Box<dyn ExternalSystem>, Box<dyn Error>> {
    let driver: Box<dyn ExternalSystem> = match driver_kind {
        DriverKind::Jsonl => {
            let input_source = resolve_input(&args.input, interface)?;
            Box::new(JsonlDriver::new(input_source, metadata))
        }
        DriverKind::Text => {
            let input_source = resolve_input(&args.input, interface)?;
            Box::new(TextLineDriver::new(input_source, metadata))
        }
        DriverKind::Binary => {
            let input_source = resolve_input(&args.input, interface)?;
            Box::new(BinaryFileDriver::new(input_source, metadata))
        }
        DriverKind::Rest => {
            let config = rest_config_from_interface(interface)?;
            Box::new(RestDriver::new(config, metadata))
        }
        DriverKind::Db => {
            let config = db_config_from_interface(interface)?;
            Box::new(DbDriver::new(config, metadata))
        }
        DriverKind::Stream => {
            let config = stream_config_from_interface(interface)?;
            Box::new(StreamDriver::new(config, metadata))
        }
    };
    Ok(driver)
}

trait DlqSink {
    fn write(&self, rows: &[common::DeadLetter]) -> Result<(), Box<dyn Error>>;
}

struct FileDlqSink {
    path: PathBuf,
}

impl FileDlqSink {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl DlqSink for FileDlqSink {
    fn write(&self, rows: &[common::DeadLetter]) -> Result<(), Box<dyn Error>> {
        write_jsonl(&self.path, rows)?;
        Ok(())
    }
}

struct SqliteDlqSink {
    path: PathBuf,
    table: String,
}

impl SqliteDlqSink {
    fn new(path: PathBuf, table: String) -> Self {
        Self { path, table }
    }

    fn validated_table_name(&self) -> Result<&str, Box<dyn Error>> {
        if is_valid_sqlite_identifier(&self.table) {
            Ok(&self.table)
        } else {
            Err(format!(
                "invalid --dlq-table value '{}': use [A-Za-z_][A-Za-z0-9_]*",
                self.table
            )
            .into())
        }
    }

    fn ensure_table(&self, connection: &rusqlite::Connection) -> Result<(), rusqlite::Error> {
        let table = self.table.as_str();
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at_unix_ms INTEGER NOT NULL,
                source TEXT NOT NULL,
                interface_name TEXT NOT NULL,
                interface_version TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                reason_codes_json TEXT NOT NULL,
                lineage_json TEXT,
                errors_json TEXT NOT NULL
            )",
            table
        );
        connection.execute(&sql, [])?;
        Ok(())
    }
}

impl DlqSink for SqliteDlqSink {
    fn write(&self, rows: &[common::DeadLetter]) -> Result<(), Box<dyn Error>> {
        let table = self.validated_table_name()?.to_string();
        let mut connection = rusqlite::Connection::open(&self.path)?;
        self.ensure_table(&connection)?;

        let insert_sql = format!(
            "INSERT INTO {} (
                created_at_unix_ms,
                source,
                interface_name,
                interface_version,
                payload_json,
                metadata_json,
                reason_codes_json,
                lineage_json,
                errors_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            table
        );

        let tx = connection.transaction()?;
        {
            let mut statement = tx.prepare(&insert_sql)?;
            for row in rows {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;

                let payload_json = serde_json::to_string(&row.payload)?;
                let metadata_json = serde_json::to_string(&row.metadata)?;
                let reason_codes_json = serde_json::to_string(&row.reason_codes)?;
                let lineage_json = serde_json::to_string(&row.lineage)?;
                let errors_json = serde_json::to_string(&row.errors)?;

                statement.execute(rusqlite::params![
                    now,
                    &row.source,
                    &row.interface.name,
                    &row.interface.version,
                    payload_json,
                    metadata_json,
                    reason_codes_json,
                    lineage_json,
                    errors_json,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }
}

fn build_dlq_sink(args: &Args) -> Box<dyn DlqSink> {
    match args.dlq_sink {
        DlqSinkKind::File => {
            let path = args
                .dlq
                .clone()
                .unwrap_or_else(|| with_suffix(&args.output, "dlq"));
            Box::new(FileDlqSink::new(path))
        }
        DlqSinkKind::Sqlite => {
            let path = args
                .dlq
                .clone()
                .unwrap_or_else(|| with_suffix(&args.output, "dlq.db"));
            Box::new(SqliteDlqSink::new(path, args.dlq_table.clone()))
        }
    }
}

fn load_replay_records(args: &Args, path: &PathBuf) -> Result<Vec<ExternalRecord>, Box<dyn Error>> {
    let dead_letters = match args.replay_dlq_source {
        DlqSinkKind::File => load_dead_letters_from_file(path)?,
        DlqSinkKind::Sqlite => load_dead_letters_from_sqlite(path, &args.replay_dlq_table)?,
    };

    Ok(dead_letters_to_external_records(dead_letters))
}

fn load_dead_letters_from_file(path: &PathBuf) -> Result<Vec<DeadLetter>, Box<dyn Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut dead_letters = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let dead_letter: DeadLetter = serde_json::from_str(&line)?;
        dead_letters.push(dead_letter);
    }

    Ok(dead_letters)
}

fn load_dead_letters_from_sqlite(
    path: &PathBuf,
    table: &str,
) -> Result<Vec<DeadLetter>, Box<dyn Error>> {
    if !is_valid_sqlite_identifier(table) {
        return Err(format!(
            "invalid --replay-dlq-table value '{}': use [A-Za-z_][A-Za-z0-9_]*",
            table
        )
        .into());
    }

    let connection = rusqlite::Connection::open(path)?;

    let sql_with_lineage = format!(
        "SELECT source, interface_name, interface_version, payload_json, metadata_json, reason_codes_json, lineage_json, errors_json
         FROM {} ORDER BY id ASC",
        table
    );

    if let Ok(mut statement) = connection.prepare(&sql_with_lineage) {
        let rows = statement.query_map([], |row| {
            let source: String = row.get(0)?;
            let interface_name: String = row.get(1)?;
            let interface_version: String = row.get(2)?;
            let payload_json: String = row.get(3)?;
            let metadata_json: String = row.get(4)?;
            let reason_codes_json: String = row.get(5)?;
            let lineage_json: Option<String> = row.get(6)?;
            let errors_json: String = row.get(7)?;

            let payload = serde_json::from_str(&payload_json).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    payload_json.len(),
                    rusqlite::types::Type::Text,
                    Box::new(err),
                )
            })?;
            let metadata = serde_json::from_str(&metadata_json).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    metadata_json.len(),
                    rusqlite::types::Type::Text,
                    Box::new(err),
                )
            })?;
            let reason_codes = serde_json::from_str(&reason_codes_json).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    reason_codes_json.len(),
                    rusqlite::types::Type::Text,
                    Box::new(err),
                )
            })?;
            let lineage = if let Some(lineage_json) = lineage_json {
                Some(serde_json::from_str(&lineage_json).map_err(|err| {
                    rusqlite::Error::FromSqlConversionFailure(
                        lineage_json.len(),
                        rusqlite::types::Type::Text,
                        Box::new(err),
                    )
                })?)
            } else {
                None
            };
            let errors: Vec<common::ValidationMessage> = serde_json::from_str(&errors_json)
                .map_err(|err| {
                    rusqlite::Error::FromSqlConversionFailure(
                        errors_json.len(),
                        rusqlite::types::Type::Text,
                        Box::new(err),
                    )
                })?;

            Ok(DeadLetter {
                source,
                interface: InterfaceRef {
                    name: interface_name,
                    version: interface_version,
                },
                payload,
                metadata,
                reason_codes,
                lineage,
                errors,
            })
        })?;

        let mut dead_letters = Vec::new();
        for row in rows {
            dead_letters.push(row?);
        }
        return Ok(dead_letters);
    }

    let sql_legacy = format!(
        "SELECT source, interface_name, interface_version, payload_json, metadata_json, errors_json
         FROM {} ORDER BY id ASC",
        table
    );
    let mut statement = connection.prepare(&sql_legacy)?;

    let rows = statement.query_map([], |row| {
        let source: String = row.get(0)?;
        let interface_name: String = row.get(1)?;
        let interface_version: String = row.get(2)?;
        let payload_json: String = row.get(3)?;
        let metadata_json: String = row.get(4)?;
        let errors_json: String = row.get(5)?;

        let payload = serde_json::from_str(&payload_json).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                payload_json.len(),
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })?;
        let metadata = serde_json::from_str(&metadata_json).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                metadata_json.len(),
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })?;
        let errors: Vec<common::ValidationMessage> =
            serde_json::from_str(&errors_json).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    errors_json.len(),
                    rusqlite::types::Type::Text,
                    Box::new(err),
                )
            })?;

        Ok(DeadLetter {
            source,
            interface: InterfaceRef {
                name: interface_name,
                version: interface_version,
            },
            payload,
            metadata,
            reason_codes: dedupe_reason_codes_from_errors(&errors),
            lineage: None,
            errors,
        })
    })?;

    let mut dead_letters = Vec::new();
    for row in rows {
        dead_letters.push(row?);
    }

    Ok(dead_letters)
}

fn dead_letters_to_external_records(dead_letters: Vec<DeadLetter>) -> Vec<ExternalRecord> {
    dead_letters
        .into_iter()
        .map(|dead_letter| ExternalRecord {
            payload: dead_letter.payload,
            metadata: dead_letter.metadata,
        })
        .collect()
}

fn dedupe_reason_codes_from_errors(errors: &[common::ValidationMessage]) -> Vec<String> {
    let mut codes = Vec::new();
    for error in errors {
        if !codes.iter().any(|code| code == &error.code) {
            codes.push(error.code.clone());
        }
    }
    codes
}

fn is_valid_sqlite_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };

    if !(first == '_' || first.is_ascii_alphabetic()) {
        return false;
    }

    chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
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
        circuit_breaker: rest.circuit_breaker.as_ref().map(|circuit_breaker| {
            DriverCircuitBreakerConfig {
                failure_threshold: circuit_breaker.failure_threshold,
                open_timeout_ms: circuit_breaker.open_timeout_ms,
            }
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
        retry: db.retry.as_ref().map(|retry| DriverDbRetryConfig {
            max_attempts: retry.max_attempts,
            base_delay_ms: retry.base_delay_ms,
            max_delay_ms: retry.max_delay_ms,
            jitter_percent: retry.jitter_percent,
        }),
        circuit_breaker: db.circuit_breaker.as_ref().map(|circuit_breaker| {
            DriverCircuitBreakerConfig {
                failure_threshold: circuit_breaker.failure_threshold,
                open_timeout_ms: circuit_breaker.open_timeout_ms,
            }
        }),
    })
}

fn stream_config_from_interface(
    interface: &ExternalInterface,
) -> Result<DriverStreamConfig, Box<dyn std::error::Error>> {
    let stream = interface
        .driver
        .stream
        .as_ref()
        .ok_or("stream driver config is required")?;

    let source = match stream.source {
        RuntimeStreamSourceKind::Kafka => DriverStreamSourceKind::Kafka,
    };

    let kafka = stream.kafka.as_ref().map(|kafka| DriverKafkaStreamConfig {
        brokers: kafka.brokers.clone(),
        topic: kafka.topic.clone(),
        group_id: kafka.group_id.clone(),
        format: kafka.format,
        max_batch_records: kafka.max_batch_records,
        poll_timeout_ms: kafka.poll_timeout_ms,
        start_offset: kafka.start_offset.map(|offset| match offset {
            RuntimeStreamStartOffset::Earliest => DriverStreamStartOffset::Earliest,
            RuntimeStreamStartOffset::Latest => DriverStreamStartOffset::Latest,
        }),
        mvp_input: InputSource::from_str(&kafka.mvp_input),
    });

    Ok(DriverStreamConfig { source, kafka })
}

#[cfg(test)]
mod tests {
    use super::{
        build_dlq_sink, dead_letters_to_external_records, is_valid_sqlite_identifier, with_suffix,
        Args, DlqSinkKind, InputFormat,
    };
    use common::{
        DeadLetter, DlqLineage, InterfaceRef, Payload, RecordMetadata, ValidationMessage,
    };
    use std::path::PathBuf;

    #[test]
    fn with_suffix_appends_before_extension() {
        let path = PathBuf::from("/tmp/output.jsonl");
        assert_eq!(
            with_suffix(&path, "dlq"),
            PathBuf::from("/tmp/output.dlq.jsonl")
        );
    }

    #[test]
    fn with_suffix_handles_extensionless_path() {
        let path = PathBuf::from("/tmp/output");
        assert_eq!(with_suffix(&path, "dlq"), PathBuf::from("/tmp/output.dlq"));
    }

    #[test]
    fn default_sqlite_dlq_path_uses_db_suffix() {
        let args = Args {
            interface: PathBuf::from("/tmp/interface.json"),
            input: None,
            output: PathBuf::from("/tmp/output.jsonl"),
            dlq: None,
            dlq_sink: DlqSinkKind::Sqlite,
            dlq_table: "dead_letters".to_string(),
            source: None,
            replay_dlq: None,
            replay_dlq_source: DlqSinkKind::File,
            replay_dlq_table: "dead_letters".to_string(),
            format: InputFormat::Auto,
            contract_registry: PathBuf::from("/tmp/allowlist.json"),
        };

        let _ = build_dlq_sink(&args);
        assert_eq!(
            with_suffix(&args.output, "dlq.db"),
            PathBuf::from("/tmp/output.dlq.db.jsonl")
        );
    }

    #[test]
    fn sqlite_identifier_validation_accepts_safe_name() {
        assert!(is_valid_sqlite_identifier("dead_letters"));
        assert!(is_valid_sqlite_identifier("dlq2"));
    }

    #[test]
    fn sqlite_identifier_validation_rejects_unsafe_name() {
        assert!(!is_valid_sqlite_identifier(""));
        assert!(!is_valid_sqlite_identifier("2dead_letters"));
        assert!(!is_valid_sqlite_identifier("dead-letters"));
        assert!(!is_valid_sqlite_identifier("dead letters"));
    }

    #[test]
    fn dead_letter_conversion_preserves_payload_and_metadata() {
        let dead_letters = vec![DeadLetter {
            source: "mes".to_string(),
            interface: InterfaceRef {
                name: "mes".to_string(),
                version: "v1".to_string(),
            },
            payload: Payload::from_text("hello".to_string()),
            metadata: RecordMetadata {
                content_type: Some("text/plain".to_string()),
                filename: Some("in.txt".to_string()),
                source_details: None,
            },
            reason_codes: vec!["TEST_ERROR".to_string()],
            lineage: Some(DlqLineage {
                rejected_at_unix_ms: 1,
                pipeline_stage: "integration".to_string(),
                driver_kind: "jsonl".to_string(),
                record_id_policy: "hash_fallback".to_string(),
                source_type: Some("file".to_string()),
                source_locator: Some("/tmp/input.jsonl".to_string()),
            }),
            errors: vec![ValidationMessage::new(
                "TEST_ERROR",
                Some("/x".to_string()),
                "bad".to_string(),
            )],
        }];

        let records = dead_letters_to_external_records(dead_letters);
        assert_eq!(records.len(), 1);
        assert!(matches!(&records[0].payload, Payload::Text(text) if text == "hello"));
        assert_eq!(records[0].metadata.filename.as_deref(), Some("in.txt"));
    }

    #[test]
    fn dedupe_reason_codes_from_errors_returns_unique_codes() {
        let errors = vec![
            ValidationMessage::new("A", Some("/a".to_string()), "a".to_string()),
            ValidationMessage::new("B", Some("/b".to_string()), "b".to_string()),
            ValidationMessage::new("A", Some("/c".to_string()), "c".to_string()),
        ];

        let codes = super::dedupe_reason_codes_from_errors(&errors);
        assert_eq!(codes, vec!["A".to_string(), "B".to_string()]);
    }
}
