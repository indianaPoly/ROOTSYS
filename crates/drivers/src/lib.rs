use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::PathBuf;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use common::{ExternalRecord, Payload, PayloadFormat, RecordMetadata};
use mysql::prelude::Queryable;
use thiserror::Error;

/// Errors returned while fetching data from external systems.
#[derive(Debug, Error)]
pub enum DriverError {
    #[error("failed to open input: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid json on line {line}: {source}")]
    InvalidJson {
        line: usize,
        source: serde_json::Error,
    },
    #[error("http request failed: {0}")]
    Http(#[from] ureq::Error),
    #[error("http status {status}: {body}")]
    HttpStatus { status: u16, body: String },
    #[error("invalid response: {0}")]
    InvalidResponse(String),
    #[error("db error: {0}")]
    Db(#[from] rusqlite::Error),
    #[error("postgres error: {0}")]
    Postgres(#[from] postgres::Error),
    #[error("mysql error: {0}")]
    Mysql(#[from] mysql::Error),
    #[error("unsupported db kind: {0}")]
    UnsupportedDbKind(String),
}

/// Input source for file-based drivers.
#[derive(Debug, Clone)]
pub enum InputSource {
    File(PathBuf),
    Stdin,
}

impl InputSource {
    /// Create a source from a path, using "-" for stdin.
    pub fn from_path(path: PathBuf) -> Self {
        if path.to_string_lossy() == "-" {
            InputSource::Stdin
        } else {
            InputSource::File(path)
        }
    }

    /// Create a source from a string, using "-" for stdin.
    pub fn from_str(path: &str) -> Self {
        if path == "-" {
            InputSource::Stdin
        } else {
            InputSource::File(PathBuf::from(path))
        }
    }

    fn filename(&self) -> Option<String> {
        match self {
            InputSource::File(path) => path
                .file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.to_string()),
            InputSource::Stdin => None,
        }
    }

    /// Open the input as a buffered reader (stdin or file).
    fn open_bufread(&self) -> Result<Box<dyn BufRead>, DriverError> {
        match self {
            InputSource::File(path) => Ok(Box::new(BufReader::new(File::open(path)?))),
            InputSource::Stdin => Ok(Box::new(BufReader::new(std::io::stdin()))),
        }
    }

    /// Read the entire input into memory.
    fn read_all(&self) -> Result<Vec<u8>, DriverError> {
        let mut buffer = Vec::new();
        match self {
            InputSource::File(path) => {
                let mut file = File::open(path)?;
                file.read_to_end(&mut buffer)?;
            }
            InputSource::Stdin => {
                let mut stdin = std::io::stdin();
                stdin.read_to_end(&mut buffer)?;
            }
        }
        Ok(buffer)
    }
}

/// Driver interface for fetching raw external records.
pub trait ExternalSystem {
    fn fetch(&mut self) -> Result<Vec<ExternalRecord>, DriverError>;
}

/// JSONL (newline-delimited JSON) file driver.
pub struct JsonlDriver {
    source: InputSource,
    metadata: RecordMetadata,
}

impl JsonlDriver {
    /// Create a JSONL driver for the given source.
    pub fn new(source: InputSource, metadata: RecordMetadata) -> Self {
        Self { source, metadata }
    }
}

impl ExternalSystem for JsonlDriver {
    fn fetch(&mut self) -> Result<Vec<ExternalRecord>, DriverError> {
        let reader = self.source.open_bufread()?;
        let mut records = Vec::new();

        let metadata =
            metadata_from_source(self.metadata.clone(), &self.source, "application/x-ndjson");

        for (idx, line) in reader.lines().enumerate() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let payload: serde_json::Value =
                serde_json::from_str(&line).map_err(|source| DriverError::InvalidJson {
                    line: idx + 1,
                    source,
                })?;
            records.push(ExternalRecord {
                payload: Payload::from_json(payload),
                metadata: metadata.clone(),
            });
        }

        Ok(records)
    }
}

pub struct TextLineDriver {
    source: InputSource,
    metadata: RecordMetadata,
}

impl TextLineDriver {
    /// Create a text line driver for the given source.
    pub fn new(source: InputSource, metadata: RecordMetadata) -> Self {
        Self { source, metadata }
    }
}

impl ExternalSystem for TextLineDriver {
    fn fetch(&mut self) -> Result<Vec<ExternalRecord>, DriverError> {
        let reader = self.source.open_bufread()?;
        let mut records = Vec::new();
        let metadata = metadata_from_source(self.metadata.clone(), &self.source, "text/plain");

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            records.push(ExternalRecord {
                payload: Payload::from_text(line),
                metadata: metadata.clone(),
            });
        }

        Ok(records)
    }
}

pub struct BinaryFileDriver {
    source: InputSource,
    metadata: RecordMetadata,
}

impl BinaryFileDriver {
    /// Create a binary file driver for the given source.
    pub fn new(source: InputSource, metadata: RecordMetadata) -> Self {
        Self { source, metadata }
    }
}

impl ExternalSystem for BinaryFileDriver {
    fn fetch(&mut self) -> Result<Vec<ExternalRecord>, DriverError> {
        let buffer = self.source.read_all()?;

        Ok(vec![ExternalRecord {
            payload: Payload::from_bytes(buffer),
            metadata: metadata_from_source(
                self.metadata.clone(),
                &self.source,
                "application/octet-stream",
            ),
        }])
    }
}

/// REST driver configuration.
#[derive(Debug, Clone)]
pub struct RestConfig {
    pub url: String,
    pub method: Option<String>,
    pub headers: BTreeMap<String, String>,
    pub body: Option<String>,
    pub timeout_ms: Option<u64>,
    pub response_format: PayloadFormat,
    pub items_pointer: Option<String>,
    pub api_key_auth: Option<ApiKeyAuthConfig>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKeyLocation {
    Header,
    Query,
}

#[derive(Debug, Clone)]
pub struct ApiKeyAuthConfig {
    pub location: ApiKeyLocation,
    pub name: String,
    pub value: String,
}

/// REST driver that fetches records from HTTP endpoints.
pub struct RestDriver {
    config: RestConfig,
    metadata: RecordMetadata,
}

impl RestDriver {
    /// Create a REST driver with the given configuration.
    pub fn new(config: RestConfig, metadata: RecordMetadata) -> Self {
        Self { config, metadata }
    }
}

impl ExternalSystem for RestDriver {
    fn fetch(&mut self) -> Result<Vec<ExternalRecord>, DriverError> {
        let agent = if let Some(timeout_ms) = self.config.timeout_ms {
            let duration = Duration::from_millis(timeout_ms);
            ureq::AgentBuilder::new()
                .timeout_read(duration)
                .timeout_write(duration)
                .build()
        } else {
            ureq::Agent::new()
        };

        let method = self
            .config
            .method
            .clone()
            .unwrap_or_else(|| "GET".to_string());

        let mut request = agent.request(&method, &self.config.url);

        if let Some(auth) = &self.config.api_key_auth {
            match auth.location {
                ApiKeyLocation::Header => {
                    request = request.set(&auth.name, &auth.value);
                }
                ApiKeyLocation::Query => {
                    request = request.query(&auth.name, &auth.value);
                }
            }
        }

        for (key, value) in &self.config.headers {
            request = request.set(key, value);
        }

        let response = match &self.config.body {
            Some(body) => request.send_string(body),
            None => request.call(),
        };

        let response = match response {
            Ok(response) => response,
            Err(ureq::Error::Status(status, response)) => {
                let body = response.into_string().unwrap_or_default();
                return Err(DriverError::HttpStatus { status, body });
            }
            Err(err) => return Err(DriverError::Http(err)),
        };

        let content_type = response
            .header("content-type")
            .map(|value| value.to_string());
        let mut bytes = Vec::new();
        response.into_reader().read_to_end(&mut bytes)?;

        let metadata = metadata_with_content_type(
            self.metadata.clone(),
            content_type.clone(),
            "application/octet-stream",
            None,
        );

        let response_format = match self.config.response_format {
            PayloadFormat::Unknown => infer_format(&bytes, content_type.as_deref()),
            other => other,
        };

        match response_format {
            PayloadFormat::Json => json_bytes_to_records(&bytes, &metadata, &self.config),
            PayloadFormat::Text => text_bytes_to_records(&bytes, &metadata),
            PayloadFormat::Binary | PayloadFormat::Unknown => Ok(vec![ExternalRecord {
                payload: Payload::from_bytes(bytes),
                metadata,
            }]),
        }
    }
}

/// Supported database kinds for DbDriver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbKind {
    Sqlite,
    Postgres,
    Mysql,
}

/// DB driver configuration.
#[derive(Debug, Clone)]
pub struct DbConfig {
    pub kind: DbKind,
    pub connection: String,
    pub query: String,
}

/// DB driver that fetches records via SQL queries.
pub struct DbDriver {
    config: DbConfig,
    metadata: RecordMetadata,
}

impl DbDriver {
    /// Create a DB driver with the given configuration.
    pub fn new(config: DbConfig, metadata: RecordMetadata) -> Self {
        Self { config, metadata }
    }
}

impl ExternalSystem for DbDriver {
    fn fetch(&mut self) -> Result<Vec<ExternalRecord>, DriverError> {
        match self.config.kind {
            DbKind::Sqlite => fetch_sqlite(&self.config, &self.metadata),
            DbKind::Postgres => fetch_postgres(&self.config, &self.metadata),
            DbKind::Mysql => fetch_mysql(&self.config, &self.metadata),
        }
    }
}

/// Fetch records from a sqlite database and map each row to a JSON object.
fn fetch_sqlite(
    config: &DbConfig,
    metadata: &RecordMetadata,
) -> Result<Vec<ExternalRecord>, DriverError> {
    let connection = rusqlite::Connection::open(&config.connection)?;
    let mut stmt = connection.prepare(&config.query)?;
    let column_names: Vec<String> = stmt
        .column_names()
        .iter()
        .map(|name| name.to_string())
        .collect();

    let mut rows = stmt.query([])?;
    let mut records = Vec::new();
    let metadata = metadata_with_content_type(metadata.clone(), None, "application/json", None);

    while let Some(row) = rows.next()? {
        let mut map = serde_json::Map::new();
        for (idx, name) in column_names.iter().enumerate() {
            let value_ref = row.get_ref(idx)?;
            let value = sqlite_value_to_json(value_ref);
            map.insert(name.clone(), value);
        }

        records.push(ExternalRecord {
            payload: Payload::from_json(serde_json::Value::Object(map)),
            metadata: metadata.clone(),
        });
    }

    Ok(records)
}

/// Fetch records from a postgres database and map each row to a JSON object.
fn fetch_postgres(
    config: &DbConfig,
    metadata: &RecordMetadata,
) -> Result<Vec<ExternalRecord>, DriverError> {
    let mut client = postgres::Client::connect(&config.connection, postgres::NoTls)?;
    let rows = client.query(&config.query, &[])?;

    let metadata = metadata_with_content_type(metadata.clone(), None, "application/json", None);

    let mut records = Vec::new();
    for row in rows {
        let mut map = serde_json::Map::new();
        for (idx, column) in row.columns().iter().enumerate() {
            let value = postgres_value_to_json(&row, idx, column.type_())?;
            map.insert(column.name().to_string(), value);
        }

        records.push(ExternalRecord {
            payload: Payload::from_json(serde_json::Value::Object(map)),
            metadata: metadata.clone(),
        });
    }

    Ok(records)
}

/// Fetch records from a mysql database and map each row to a JSON object.
fn fetch_mysql(
    config: &DbConfig,
    metadata: &RecordMetadata,
) -> Result<Vec<ExternalRecord>, DriverError> {
    let pool = mysql::Pool::new(config.connection.as_str())?;
    let mut conn = pool.get_conn()?;
    let result = conn.query_iter(&config.query)?;
    let columns: Vec<String> = result
        .columns()
        .as_ref()
        .iter()
        .map(|col| col.name_str().to_string())
        .collect();

    let metadata = metadata_with_content_type(metadata.clone(), None, "application/json", None);

    let mut records = Vec::new();
    for row_result in result {
        let row = row_result?;
        let mut map = serde_json::Map::new();
        let values = row.unwrap();
        for (idx, value) in values.into_iter().enumerate() {
            let name = columns
                .get(idx)
                .cloned()
                .unwrap_or_else(|| format!("col_{}", idx + 1));
            map.insert(name, mysql_value_to_json(value));
        }

        records.push(ExternalRecord {
            payload: Payload::from_json(serde_json::Value::Object(map)),
            metadata: metadata.clone(),
        });
    }

    Ok(records)
}

/// Convert sqlite value types to JSON-friendly values.
fn sqlite_value_to_json(value: rusqlite::types::ValueRef<'_>) -> serde_json::Value {
    match value {
        rusqlite::types::ValueRef::Null => serde_json::Value::Null,
        rusqlite::types::ValueRef::Integer(value) => serde_json::Value::Number(value.into()),
        rusqlite::types::ValueRef::Real(value) => serde_json::Number::from_f64(value)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        rusqlite::types::ValueRef::Text(value) => {
            serde_json::Value::String(String::from_utf8_lossy(value).to_string())
        }
        rusqlite::types::ValueRef::Blob(value) => serde_json::Value::String(STANDARD.encode(value)),
    }
}

/// Convert postgres row values into JSON-compatible values.
fn postgres_value_to_json(
    row: &postgres::Row,
    idx: usize,
    ty: &postgres::types::Type,
) -> Result<serde_json::Value, DriverError> {
    use postgres::types::Type;

    let value = match *ty {
        Type::BOOL => option_bool_to_json(row.try_get::<_, Option<bool>>(idx)?),
        Type::INT2 => option_i64_to_json(row.try_get::<_, Option<i16>>(idx)?.map(i64::from)),
        Type::INT4 => option_i64_to_json(row.try_get::<_, Option<i32>>(idx)?.map(i64::from)),
        Type::INT8 => option_i64_to_json(row.try_get::<_, Option<i64>>(idx)?),
        Type::FLOAT4 => option_f64_to_json(row.try_get::<_, Option<f32>>(idx)?.map(f64::from)),
        Type::FLOAT8 => option_f64_to_json(row.try_get::<_, Option<f64>>(idx)?),
        Type::NUMERIC => option_string_to_json(row.try_get::<_, Option<String>>(idx)?),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::CHAR => {
            option_string_to_json(row.try_get::<_, Option<String>>(idx)?)
        }
        Type::JSON | Type::JSONB => {
            let json = row.try_get::<_, Option<serde_json::Value>>(idx)?;
            json.unwrap_or(serde_json::Value::Null)
        }
        Type::BYTEA => {
            let bytes = row.try_get::<_, Option<Vec<u8>>>(idx)?;
            match bytes {
                Some(bytes) => serde_json::Value::String(STANDARD.encode(bytes)),
                None => serde_json::Value::Null,
            }
        }
        Type::DATE => {
            let value = row.try_get::<_, Option<chrono::NaiveDate>>(idx)?;
            option_string_to_json(value.map(|v| v.to_string()))
        }
        Type::TIME => {
            let value = row.try_get::<_, Option<chrono::NaiveTime>>(idx)?;
            option_string_to_json(value.map(|v| v.to_string()))
        }
        Type::TIMESTAMP => {
            let value = row.try_get::<_, Option<chrono::NaiveDateTime>>(idx)?;
            option_string_to_json(value.map(|v| v.to_string()))
        }
        Type::TIMESTAMPTZ => {
            let value = row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(idx)?;
            option_string_to_json(value.map(|v| v.to_rfc3339()))
        }
        Type::UUID | Type::INET | Type::CIDR | Type::MACADDR | Type::MACADDR8 | Type::OID => {
            option_string_to_json(row.try_get::<_, Option<String>>(idx)?)
        }
        _ => postgres_fallback_to_json(row, idx, ty)?,
    };

    Ok(value)
}

/// Convert mysql values into JSON-compatible values.
fn mysql_value_to_json(value: mysql::Value) -> serde_json::Value {
    match value {
        mysql::Value::NULL => serde_json::Value::Null,
        mysql::Value::Bytes(bytes) => match String::from_utf8(bytes.clone()) {
            Ok(text) => serde_json::Value::String(text),
            Err(_) => serde_json::Value::String(STANDARD.encode(bytes)),
        },
        mysql::Value::Int(value) => serde_json::Value::Number(value.into()),
        mysql::Value::UInt(value) => serde_json::Value::Number(value.into()),
        mysql::Value::Float(value) => option_f64_to_json(Some(f64::from(value))),
        mysql::Value::Double(value) => option_f64_to_json(Some(value)),
        mysql::Value::Date(year, month, day, hour, minute, second, micro) => {
            let text = format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}Z",
                year, month, day, hour, minute, second, micro
            );
            serde_json::Value::String(text)
        }
        mysql::Value::Time(negative, days, hours, minutes, seconds, micro) => {
            let sign = if negative { "-" } else { "" };
            let text = format!(
                "{}{:03}:{:02}:{:02}.{:06}",
                sign,
                days * 24 + u32::from(hours),
                minutes,
                seconds,
                micro
            );
            serde_json::Value::String(text)
        }
    }
}

/// Fallback conversion for postgres types without a direct mapping.
fn postgres_fallback_to_json(
    row: &postgres::Row,
    idx: usize,
    ty: &postgres::types::Type,
) -> Result<serde_json::Value, DriverError> {
    if let Ok(value) = row.try_get::<_, Option<String>>(idx) {
        return Ok(option_string_to_json(value));
    }

    if let Ok(value) = row.try_get::<_, Option<Vec<u8>>>(idx) {
        return Ok(match value {
            Some(bytes) => serde_json::Value::String(STANDARD.encode(bytes)),
            None => serde_json::Value::Null,
        });
    }

    Err(DriverError::InvalidResponse(format!(
        "unsupported postgres type {}",
        ty.name()
    )))
}

fn option_f64_to_json(value: Option<f64>) -> serde_json::Value {
    match value {
        Some(value) => serde_json::Number::from_f64(value)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        None => serde_json::Value::Null,
    }
}

fn option_i64_to_json(value: Option<i64>) -> serde_json::Value {
    match value {
        Some(value) => serde_json::Value::Number(value.into()),
        None => serde_json::Value::Null,
    }
}

fn option_bool_to_json(value: Option<bool>) -> serde_json::Value {
    match value {
        Some(value) => serde_json::Value::Bool(value),
        None => serde_json::Value::Null,
    }
}

fn option_string_to_json(value: Option<String>) -> serde_json::Value {
    match value {
        Some(value) => serde_json::Value::String(value),
        None => serde_json::Value::Null,
    }
}

/// Convert JSON response bytes to external records (array or single object).
fn json_bytes_to_records(
    bytes: &[u8],
    metadata: &RecordMetadata,
    config: &RestConfig,
) -> Result<Vec<ExternalRecord>, DriverError> {
    let value: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|err| DriverError::InvalidResponse(err.to_string()))?;

    let target = if let Some(pointer) = &config.items_pointer {
        value.pointer(pointer).ok_or_else(|| {
            DriverError::InvalidResponse(format!("json pointer not found: {pointer}"))
        })?
    } else {
        &value
    };

    match target {
        serde_json::Value::Array(items) => Ok(items
            .iter()
            .cloned()
            .map(|item| ExternalRecord {
                payload: Payload::from_json(item),
                metadata: metadata.clone(),
            })
            .collect()),
        _ => Ok(vec![ExternalRecord {
            payload: Payload::from_json(target.clone()),
            metadata: metadata.clone(),
        }]),
    }
}

/// Convert UTF-8 text response bytes to an external record.
fn text_bytes_to_records(
    bytes: &[u8],
    metadata: &RecordMetadata,
) -> Result<Vec<ExternalRecord>, DriverError> {
    let text = String::from_utf8(bytes.to_vec())
        .map_err(|err| DriverError::InvalidResponse(err.to_string()))?;
    Ok(vec![ExternalRecord {
        payload: Payload::from_text(text),
        metadata: metadata.clone(),
    }])
}

/// Infer payload format based on content type or content inspection.
fn infer_format(bytes: &[u8], content_type: Option<&str>) -> PayloadFormat {
    if let Some(content_type) = content_type {
        let content_type = content_type.to_ascii_lowercase();
        if content_type.contains("application/json") || content_type.contains("text/json") {
            return PayloadFormat::Json;
        }
        if content_type.starts_with("text/") {
            return PayloadFormat::Text;
        }
    }

    if serde_json::from_slice::<serde_json::Value>(bytes).is_ok() {
        return PayloadFormat::Json;
    }
    if std::str::from_utf8(bytes).is_ok() {
        return PayloadFormat::Text;
    }
    PayloadFormat::Binary
}

/// Merge default metadata for file-based sources.
fn metadata_from_source(
    metadata: RecordMetadata,
    source: &InputSource,
    default_content_type: &str,
) -> RecordMetadata {
    metadata_with_content_type(metadata, None, default_content_type, source.filename())
}

/// Fill missing metadata fields with inferred values.
fn metadata_with_content_type(
    mut metadata: RecordMetadata,
    inferred_content_type: Option<String>,
    default_content_type: &str,
    filename: Option<String>,
) -> RecordMetadata {
    if metadata.content_type.is_none() {
        metadata.content_type =
            inferred_content_type.or_else(|| Some(default_content_type.to_string()));
    }
    if metadata.filename.is_none() {
        metadata.filename = filename;
    }
    metadata
}
