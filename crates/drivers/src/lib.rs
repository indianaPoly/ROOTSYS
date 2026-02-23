use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use common::{ExternalRecord, Payload, PayloadFormat, RecordMetadata, SourceDetails};
use mysql::prelude::Queryable;
use thiserror::Error;

const DEFAULT_REST_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_REST_MAX_PAGES: u32 = 100;
const DEFAULT_REST_RETRY_MAX_ATTEMPTS: u32 = 3;
const DEFAULT_REST_RETRY_BASE_DELAY_MS: u64 = 100;
const DEFAULT_REST_RETRY_MAX_DELAY_MS: u64 = 2_000;
const DEFAULT_REST_RETRY_JITTER_PERCENT: u32 = 20;
const DEFAULT_REST_CIRCUIT_FAILURE_THRESHOLD: u32 = 5;
const DEFAULT_REST_CIRCUIT_OPEN_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_DB_POOL_MIN_CONNECTIONS: u32 = 1;
const DEFAULT_DB_POOL_MAX_CONNECTIONS: u32 = 10;
const DEFAULT_DB_RETRY_MAX_ATTEMPTS: u32 = 3;
const DEFAULT_DB_RETRY_BASE_DELAY_MS: u64 = 100;
const DEFAULT_DB_RETRY_MAX_DELAY_MS: u64 = 2_000;
const DEFAULT_DB_RETRY_JITTER_PERCENT: u32 = 20;
const DEFAULT_DB_CIRCUIT_FAILURE_THRESHOLD: u32 = 5;
const DEFAULT_DB_CIRCUIT_OPEN_TIMEOUT_MS: u64 = 30_000;

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
    #[error("circuit breaker is open for {driver}")]
    CircuitBreakerOpen { driver: String },
}

#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    pub failure_threshold: Option<u32>,
    pub open_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
enum CircuitState {
    Closed { consecutive_failures: u32 },
    Open { opened_at: Instant },
    HalfOpen,
}

#[derive(Debug, Clone, Copy)]
struct CircuitBreaker {
    failure_threshold: u32,
    open_timeout: Duration,
    state: CircuitState,
}

impl CircuitBreaker {
    fn new(
        config: &CircuitBreakerConfig,
        default_failure_threshold: u32,
        default_open_timeout_ms: u64,
    ) -> Self {
        let failure_threshold = config
            .failure_threshold
            .unwrap_or(default_failure_threshold)
            .max(1);
        let open_timeout_ms = config
            .open_timeout_ms
            .unwrap_or(default_open_timeout_ms)
            .max(1);

        Self {
            failure_threshold,
            open_timeout: Duration::from_millis(open_timeout_ms),
            state: CircuitState::Closed {
                consecutive_failures: 0,
            },
        }
    }

    fn allow_call(&mut self) -> bool {
        match self.state {
            CircuitState::Closed { .. } | CircuitState::HalfOpen => true,
            CircuitState::Open { opened_at } => {
                if Instant::now().saturating_duration_since(opened_at) >= self.open_timeout {
                    self.state = CircuitState::HalfOpen;
                    true
                } else {
                    false
                }
            }
        }
    }

    fn on_success(&mut self) {
        self.state = CircuitState::Closed {
            consecutive_failures: 0,
        };
    }

    fn on_failure(&mut self) {
        self.state = match self.state {
            CircuitState::Closed {
                consecutive_failures,
            } => {
                let next_failures = consecutive_failures.saturating_add(1);
                if next_failures >= self.failure_threshold {
                    CircuitState::Open {
                        opened_at: Instant::now(),
                    }
                } else {
                    CircuitState::Closed {
                        consecutive_failures: next_failures,
                    }
                }
            }
            CircuitState::HalfOpen | CircuitState::Open { .. } => CircuitState::Open {
                opened_at: Instant::now(),
            },
        };
    }
}

fn ensure_circuit_allows_call(
    circuit: &mut Option<CircuitBreaker>,
    driver_name: &str,
) -> Result<(), DriverError> {
    if let Some(circuit) = circuit.as_mut() {
        if !circuit.allow_call() {
            return Err(DriverError::CircuitBreakerOpen {
                driver: driver_name.to_string(),
            });
        }
    }

    Ok(())
}

fn record_circuit_success(circuit: &mut Option<CircuitBreaker>) {
    if let Some(circuit) = circuit.as_mut() {
        circuit.on_success();
    }
}

fn record_circuit_failure(circuit: &mut Option<CircuitBreaker>) {
    if let Some(circuit) = circuit.as_mut() {
        circuit.on_failure();
    }
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

    fn locator(&self) -> String {
        match self {
            InputSource::File(path) => path.to_string_lossy().to_string(),
            InputSource::Stdin => "stdin".to_string(),
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
    pub oauth2_auth: Option<OAuth2ClientCredentialsAuthConfig>,
    pub pagination: Option<RestPaginationConfig>,
    pub retry: Option<RestRetryConfig>,
    pub circuit_breaker: Option<CircuitBreakerConfig>,
}

#[derive(Debug, Clone)]
pub struct RestRetryConfig {
    pub max_attempts: Option<u32>,
    pub base_delay_ms: Option<u64>,
    pub max_delay_ms: Option<u64>,
    pub jitter_percent: Option<u32>,
}

#[derive(Debug, Clone, Copy)]
struct RestRetryPolicy {
    max_attempts: u32,
    base_delay_ms: u64,
    max_delay_ms: u64,
    jitter_percent: u32,
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

#[derive(Debug, Clone)]
pub struct OAuth2ClientCredentialsAuthConfig {
    pub token_url: String,
    pub client_id: String,
    pub client_secret: String,
    pub scope: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RestPaginationConfig {
    pub kind: RestPaginationKind,
    pub cursor: Option<CursorPaginationConfig>,
    pub page: Option<PagePaginationConfig>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestPaginationKind {
    Cursor,
    Page,
}

#[derive(Debug, Clone)]
pub struct CursorPaginationConfig {
    pub cursor_param: String,
    pub cursor_path: String,
    pub initial_cursor: Option<String>,
    pub max_pages: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct PagePaginationConfig {
    pub page_param: String,
    pub page_size_param: String,
    pub page_size: u32,
    pub initial_page: Option<u32>,
    pub max_pages: Option<u32>,
}

#[derive(Debug, Clone)]
struct CachedBearerToken {
    token: String,
    expires_at: Instant,
}

/// REST driver that fetches records from HTTP endpoints.
pub struct RestDriver {
    config: RestConfig,
    metadata: RecordMetadata,
    cached_bearer_token: Option<CachedBearerToken>,
    circuit_breaker: Option<CircuitBreaker>,
}

impl RestDriver {
    /// Create a REST driver with the given configuration.
    pub fn new(config: RestConfig, metadata: RecordMetadata) -> Self {
        let circuit_breaker = config.circuit_breaker.as_ref().map(|cfg| {
            CircuitBreaker::new(
                cfg,
                DEFAULT_REST_CIRCUIT_FAILURE_THRESHOLD,
                DEFAULT_REST_CIRCUIT_OPEN_TIMEOUT_MS,
            )
        });

        Self {
            config,
            metadata,
            cached_bearer_token: None,
            circuit_breaker,
        }
    }

    fn get_bearer_token(
        &mut self,
        agent: &ureq::Agent,
        auth: &OAuth2ClientCredentialsAuthConfig,
    ) -> Result<String, DriverError> {
        if let Some(cached) = &self.cached_bearer_token {
            if Instant::now() < cached.expires_at {
                return Ok(cached.token.clone());
            }
        }

        let mut form = vec![
            ("grant_type", "client_credentials"),
            ("client_id", auth.client_id.as_str()),
            ("client_secret", auth.client_secret.as_str()),
        ];

        if let Some(scope) = &auth.scope {
            form.push(("scope", scope.as_str()));
        }

        let response = match agent.post(&auth.token_url).send_form(&form) {
            Ok(response) => response,
            Err(ureq::Error::Status(status, response)) => {
                let body = response.into_string().unwrap_or_default();
                return Err(DriverError::HttpStatus { status, body });
            }
            Err(err) => return Err(DriverError::Http(err)),
        };

        let token_payload: serde_json::Value = serde_json::from_reader(response.into_reader())
            .map_err(|err| DriverError::InvalidResponse(err.to_string()))?;

        let token = token_payload
            .get("access_token")
            .and_then(|value| value.as_str())
            .ok_or_else(|| {
                DriverError::InvalidResponse(
                    "oauth2 token response missing access_token".to_string(),
                )
            })?
            .to_string();

        let expires_in = token_payload
            .get("expires_in")
            .and_then(|value| value.as_u64())
            .unwrap_or(300);

        let refresh_margin = 5;
        let effective_ttl = expires_in.saturating_sub(refresh_margin).max(1);

        self.cached_bearer_token = Some(CachedBearerToken {
            token: token.clone(),
            expires_at: Instant::now() + Duration::from_secs(effective_ttl),
        });

        Ok(token)
    }

    fn resolved_retry_policy(&self) -> RestRetryPolicy {
        let retry = self.config.retry.as_ref();

        let max_attempts = retry
            .and_then(|cfg| cfg.max_attempts)
            .unwrap_or(DEFAULT_REST_RETRY_MAX_ATTEMPTS)
            .max(1);
        let base_delay_ms = retry
            .and_then(|cfg| cfg.base_delay_ms)
            .unwrap_or(DEFAULT_REST_RETRY_BASE_DELAY_MS)
            .max(1);
        let requested_max_delay_ms = retry
            .and_then(|cfg| cfg.max_delay_ms)
            .unwrap_or(DEFAULT_REST_RETRY_MAX_DELAY_MS)
            .max(1);
        let max_delay_ms = std::cmp::max(requested_max_delay_ms, base_delay_ms);
        let jitter_percent = retry
            .and_then(|cfg| cfg.jitter_percent)
            .unwrap_or(DEFAULT_REST_RETRY_JITTER_PERCENT)
            .min(100);

        RestRetryPolicy {
            max_attempts,
            base_delay_ms,
            max_delay_ms,
            jitter_percent,
        }
    }

    fn should_retry_error(error: &DriverError) -> bool {
        match error {
            DriverError::Http(_) => true,
            DriverError::HttpStatus { status, .. } => {
                matches!(status, 408 | 425 | 429 | 500 | 502 | 503 | 504)
            }
            _ => false,
        }
    }

    fn ensure_circuit_allows_call(&mut self) -> Result<(), DriverError> {
        ensure_circuit_allows_call(&mut self.circuit_breaker, "rest")
    }

    fn record_circuit_success(&mut self) {
        record_circuit_success(&mut self.circuit_breaker);
    }

    fn record_circuit_failure(&mut self) {
        record_circuit_failure(&mut self.circuit_breaker);
    }

    fn delay_with_backoff_and_jitter_ms(
        attempt_index: u32,
        base_delay_ms: u64,
        max_delay_ms: u64,
        jitter_percent: u32,
    ) -> u64 {
        let exp = attempt_index.min(16);
        let factor = 2u64.saturating_pow(exp);
        let base_delay = base_delay_ms.saturating_mul(factor);
        let clamped = std::cmp::min(base_delay, max_delay_ms);

        if jitter_percent == 0 {
            return clamped;
        }

        let jitter_window = (clamped.saturating_mul(jitter_percent as u64)) / 100;
        if jitter_window == 0 {
            return clamped;
        }

        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .subsec_nanos() as u64;
        let range = jitter_window.saturating_mul(2).saturating_add(1);
        let offset = now_nanos % range;
        let signed_offset = offset as i128 - jitter_window as i128;
        let candidate = clamped as i128 + signed_offset;
        candidate.max(0) as u64
    }
}

impl ExternalSystem for RestDriver {
    fn fetch(&mut self) -> Result<Vec<ExternalRecord>, DriverError> {
        let timeout_ms = self.config.timeout_ms.unwrap_or(DEFAULT_REST_TIMEOUT_MS);
        let duration = Duration::from_millis(timeout_ms);
        let agent = ureq::AgentBuilder::new()
            .timeout_read(duration)
            .timeout_write(duration)
            .build();

        if let Some(pagination) = self.config.pagination.clone() {
            match pagination.kind {
                RestPaginationKind::Cursor => {
                    return self.fetch_cursor_paginated(&agent, &pagination);
                }
                RestPaginationKind::Page => {
                    return self.fetch_page_paginated(&agent, &pagination);
                }
            }
        }

        let (bytes, content_type) = self.execute_rest_call(&agent, None)?;

        let metadata = metadata_with_content_type(
            self.metadata.clone(),
            content_type.clone(),
            "application/octet-stream",
            None,
        );
        let metadata =
            metadata_with_source_details(metadata, "rest", Some(self.config.url.clone()));

        let response_format = match self.config.response_format {
            PayloadFormat::Unknown => infer_format(&bytes, content_type.as_deref()),
            other => other,
        };

        match response_format {
            PayloadFormat::Json => {
                let value: serde_json::Value = serde_json::from_slice(&bytes)
                    .map_err(|err| DriverError::InvalidResponse(err.to_string()))?;
                json_value_to_records(&value, &metadata, &self.config)
            }
            PayloadFormat::Text => text_bytes_to_records(&bytes, &metadata),
            PayloadFormat::Binary | PayloadFormat::Unknown => Ok(vec![ExternalRecord {
                payload: Payload::from_bytes(bytes),
                metadata,
            }]),
        }
    }
}

impl RestDriver {
    fn execute_rest_call_once(
        &mut self,
        agent: &ureq::Agent,
        cursor_query: Option<(&str, &str)>,
    ) -> Result<(Vec<u8>, Option<String>), DriverError> {
        self.ensure_circuit_allows_call()?;

        let method = self
            .config
            .method
            .clone()
            .unwrap_or_else(|| "GET".to_string());

        let mut request = agent.request(&method, &self.config.url);

        if let Some((name, value)) = cursor_query {
            request = request.query(name, value);
        }

        if let Some(oauth2) = self.config.oauth2_auth.clone() {
            let bearer = self.get_bearer_token(agent, &oauth2)?;
            request = request.set("Authorization", &format!("Bearer {bearer}"));
        }

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

        Ok((bytes, content_type))
    }

    fn execute_rest_call(
        &mut self,
        agent: &ureq::Agent,
        cursor_query: Option<(&str, &str)>,
    ) -> Result<(Vec<u8>, Option<String>), DriverError> {
        let policy = self.resolved_retry_policy();

        for attempt in 1..=policy.max_attempts {
            match self.execute_rest_call_once(agent, cursor_query) {
                Ok(result) => {
                    self.record_circuit_success();
                    return Ok(result);
                }
                Err(error) if attempt < policy.max_attempts && Self::should_retry_error(&error) => {
                    self.record_circuit_failure();
                    let delay_ms = Self::delay_with_backoff_and_jitter_ms(
                        attempt - 1,
                        policy.base_delay_ms,
                        policy.max_delay_ms,
                        policy.jitter_percent,
                    );
                    std::thread::sleep(Duration::from_millis(delay_ms));
                }
                Err(error) => {
                    self.record_circuit_failure();
                    return Err(error);
                }
            }
        }

        Err(DriverError::InvalidResponse(
            "retry policy exhausted without terminal response".to_string(),
        ))
    }

    fn fetch_cursor_paginated(
        &mut self,
        agent: &ureq::Agent,
        pagination: &RestPaginationConfig,
    ) -> Result<Vec<ExternalRecord>, DriverError> {
        let cursor = pagination.cursor.as_ref().ok_or_else(|| {
            DriverError::InvalidResponse("cursor pagination config is missing".to_string())
        })?;

        let mut records = Vec::new();
        let mut next_cursor = cursor.initial_cursor.clone();
        let mut pages = 0u32;
        let max_pages = cursor.max_pages.unwrap_or(DEFAULT_REST_MAX_PAGES);

        loop {
            if pages >= max_pages {
                break;
            }

            let query = next_cursor
                .as_ref()
                .map(|value| (cursor.cursor_param.as_str(), value.as_str()));
            let (bytes, content_type) = self.execute_rest_call(agent, query)?;
            let metadata = metadata_with_content_type(
                self.metadata.clone(),
                content_type.clone(),
                "application/octet-stream",
                None,
            );
            let metadata =
                metadata_with_source_details(metadata, "rest", Some(self.config.url.clone()));

            let response_format = match self.config.response_format {
                PayloadFormat::Unknown => infer_format(&bytes, content_type.as_deref()),
                other => other,
            };

            if response_format != PayloadFormat::Json {
                return Err(DriverError::InvalidResponse(
                    "cursor pagination requires json responses".to_string(),
                ));
            }

            let value: serde_json::Value = serde_json::from_slice(&bytes)
                .map_err(|err| DriverError::InvalidResponse(err.to_string()))?;
            records.extend(json_value_to_records(&value, &metadata, &self.config)?);

            pages += 1;

            let extracted = value.pointer(&cursor.cursor_path);
            let parsed_next = match extracted {
                Some(serde_json::Value::String(value)) if !value.is_empty() => Some(value.clone()),
                Some(serde_json::Value::Number(value)) => Some(value.to_string()),
                Some(serde_json::Value::Bool(value)) => Some(value.to_string()),
                _ => None,
            };

            match parsed_next {
                Some(new_cursor) => {
                    if next_cursor.as_ref() == Some(&new_cursor) {
                        break;
                    }
                    next_cursor = Some(new_cursor);
                }
                None => break,
            }
        }

        Ok(records)
    }

    fn fetch_page_paginated(
        &mut self,
        agent: &ureq::Agent,
        pagination: &RestPaginationConfig,
    ) -> Result<Vec<ExternalRecord>, DriverError> {
        let page_cfg = pagination.page.as_ref().ok_or_else(|| {
            DriverError::InvalidResponse("page pagination config is missing".to_string())
        })?;

        let mut records = Vec::new();
        let mut page = page_cfg.initial_page.unwrap_or(1);
        let mut pages = 0u32;
        let max_pages = page_cfg.max_pages.unwrap_or(DEFAULT_REST_MAX_PAGES);

        loop {
            if pages >= max_pages {
                break;
            }

            let method = self
                .config
                .method
                .clone()
                .unwrap_or_else(|| "GET".to_string());
            let page_value = page.to_string();
            let page_size_value = page_cfg.page_size.to_string();
            let policy = self.resolved_retry_policy();
            let mut last_error: Option<DriverError> = None;
            let mut result: Option<(Vec<u8>, Option<String>)> = None;

            for attempt in 1..=policy.max_attempts {
                self.ensure_circuit_allows_call()?;

                let mut request = agent.request(&method, &self.config.url);
                request = request.query(&page_cfg.page_param, &page_value);
                request = request.query(&page_cfg.page_size_param, &page_size_value);

                if let Some(oauth2) = self.config.oauth2_auth.clone() {
                    let bearer = self.get_bearer_token(agent, &oauth2)?;
                    request = request.set("Authorization", &format!("Bearer {bearer}"));
                }

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
                        let error = DriverError::HttpStatus { status, body };
                        if attempt < policy.max_attempts && Self::should_retry_error(&error) {
                            self.record_circuit_failure();
                            let delay_ms = Self::delay_with_backoff_and_jitter_ms(
                                attempt - 1,
                                policy.base_delay_ms,
                                policy.max_delay_ms,
                                policy.jitter_percent,
                            );
                            std::thread::sleep(Duration::from_millis(delay_ms));
                            last_error = Some(error);
                            continue;
                        }
                        self.record_circuit_failure();
                        return Err(error);
                    }
                    Err(err) => {
                        let error = DriverError::Http(err);
                        if attempt < policy.max_attempts && Self::should_retry_error(&error) {
                            self.record_circuit_failure();
                            let delay_ms = Self::delay_with_backoff_and_jitter_ms(
                                attempt - 1,
                                policy.base_delay_ms,
                                policy.max_delay_ms,
                                policy.jitter_percent,
                            );
                            std::thread::sleep(Duration::from_millis(delay_ms));
                            last_error = Some(error);
                            continue;
                        }
                        self.record_circuit_failure();
                        return Err(error);
                    }
                };

                let content_type = response
                    .header("content-type")
                    .map(|value| value.to_string());
                let mut bytes = Vec::new();
                response.into_reader().read_to_end(&mut bytes)?;
                self.record_circuit_success();
                result = Some((bytes, content_type));
                break;
            }

            let (bytes, content_type) = match result {
                Some(value) => value,
                None => {
                    return Err(last_error.unwrap_or_else(|| {
                        DriverError::InvalidResponse(
                            "retry policy exhausted without terminal response".to_string(),
                        )
                    }))
                }
            };

            let metadata = metadata_with_content_type(
                self.metadata.clone(),
                content_type.clone(),
                "application/octet-stream",
                None,
            );
            let metadata =
                metadata_with_source_details(metadata, "rest", Some(self.config.url.clone()));

            let response_format = match self.config.response_format {
                PayloadFormat::Unknown => infer_format(&bytes, content_type.as_deref()),
                other => other,
            };

            if response_format != PayloadFormat::Json {
                return Err(DriverError::InvalidResponse(
                    "page pagination requires json responses".to_string(),
                ));
            }

            let value: serde_json::Value = serde_json::from_slice(&bytes)
                .map_err(|err| DriverError::InvalidResponse(err.to_string()))?;

            let page_records = json_value_to_records(&value, &metadata, &self.config)?;
            let emitted_count = page_records.len();
            records.extend(page_records);

            pages += 1;
            if emitted_count == 0 {
                break;
            }
            page += 1;
        }

        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        delay_with_backoff_and_jitter_ms, ensure_circuit_allows_call, is_retryable_db_error,
        CircuitBreaker, CircuitBreakerConfig, CircuitState, DriverError, RestDriver,
    };

    #[test]
    fn backoff_grows_exponentially_without_jitter() {
        assert_eq!(
            RestDriver::delay_with_backoff_and_jitter_ms(0, 100, 2_000, 0),
            100
        );
        assert_eq!(
            RestDriver::delay_with_backoff_and_jitter_ms(1, 100, 2_000, 0),
            200
        );
        assert_eq!(
            RestDriver::delay_with_backoff_and_jitter_ms(2, 100, 2_000, 0),
            400
        );
    }

    #[test]
    fn backoff_respects_max_delay_without_jitter() {
        assert_eq!(
            RestDriver::delay_with_backoff_and_jitter_ms(5, 100, 500, 0),
            500
        );
    }

    #[test]
    fn jitter_stays_within_expected_window() {
        let delay = RestDriver::delay_with_backoff_and_jitter_ms(1, 100, 2_000, 20);
        assert!(delay >= 160);
        assert!(delay <= 240);
    }

    #[test]
    fn db_backoff_grows_exponentially_without_jitter() {
        assert_eq!(delay_with_backoff_and_jitter_ms(0, 100, 2_000, 0), 100);
        assert_eq!(delay_with_backoff_and_jitter_ms(1, 100, 2_000, 0), 200);
        assert_eq!(delay_with_backoff_and_jitter_ms(2, 100, 2_000, 0), 400);
    }

    #[test]
    fn sqlite_busy_is_classified_as_retryable() {
        let sqlite_err = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: rusqlite::ErrorCode::DatabaseBusy,
                extended_code: 0,
            },
            None,
        );

        let driver_err = DriverError::Db(sqlite_err);
        assert!(is_retryable_db_error(&driver_err));
    }

    #[test]
    fn sqlite_invalid_query_is_not_retryable() {
        let sqlite_err = rusqlite::Error::InvalidQuery;
        let driver_err = DriverError::Db(sqlite_err);
        assert!(!is_retryable_db_error(&driver_err));
    }

    #[test]
    fn circuit_breaker_opens_after_threshold_failures() {
        let config = CircuitBreakerConfig {
            failure_threshold: Some(2),
            open_timeout_ms: Some(100),
        };
        let mut breaker = CircuitBreaker::new(&config, 5, 30_000);

        breaker.on_failure();
        assert!(matches!(
            breaker.state,
            CircuitState::Closed {
                consecutive_failures: 1
            }
        ));

        breaker.on_failure();
        assert!(matches!(breaker.state, CircuitState::Open { .. }));
    }

    #[test]
    fn circuit_breaker_moves_to_half_open_after_timeout() {
        let config = CircuitBreakerConfig {
            failure_threshold: Some(1),
            open_timeout_ms: Some(1),
        };
        let mut breaker = CircuitBreaker::new(&config, 5, 30_000);
        breaker.on_failure();

        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(breaker.allow_call());
        assert!(matches!(breaker.state, CircuitState::HalfOpen));
    }

    #[test]
    fn ensure_circuit_rejects_when_open() {
        let config = CircuitBreakerConfig {
            failure_threshold: Some(1),
            open_timeout_ms: Some(1_000),
        };
        let mut circuit = Some(CircuitBreaker::new(&config, 5, 30_000));
        if let Some(breaker) = circuit.as_mut() {
            breaker.on_failure();
        }

        let result = ensure_circuit_allows_call(&mut circuit, "rest");
        assert!(matches!(
            result,
            Err(DriverError::CircuitBreakerOpen { .. })
        ));
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
    pub postgres_tls_mode: Option<PostgresTlsMode>,
    pub pool_min_connections: Option<u32>,
    pub pool_max_connections: Option<u32>,
    pub retry: Option<DbRetryConfig>,
    pub circuit_breaker: Option<CircuitBreakerConfig>,
}

#[derive(Debug, Clone)]
pub struct DbRetryConfig {
    pub max_attempts: Option<u32>,
    pub base_delay_ms: Option<u64>,
    pub max_delay_ms: Option<u64>,
    pub jitter_percent: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresTlsMode {
    Disable,
    Require,
}

fn resolved_pool_bounds(config: &DbConfig) -> (u32, u32) {
    let min = config
        .pool_min_connections
        .unwrap_or(DEFAULT_DB_POOL_MIN_CONNECTIONS);
    let max = config
        .pool_max_connections
        .unwrap_or(DEFAULT_DB_POOL_MAX_CONNECTIONS);
    (min, std::cmp::max(max, min))
}

#[derive(Debug, Clone, Copy)]
struct DbRetryPolicy {
    max_attempts: u32,
    base_delay_ms: u64,
    max_delay_ms: u64,
    jitter_percent: u32,
}

fn resolved_db_retry_policy(config: &DbConfig) -> DbRetryPolicy {
    let retry = config.retry.as_ref();

    let max_attempts = retry
        .and_then(|cfg| cfg.max_attempts)
        .unwrap_or(DEFAULT_DB_RETRY_MAX_ATTEMPTS)
        .max(1);
    let base_delay_ms = retry
        .and_then(|cfg| cfg.base_delay_ms)
        .unwrap_or(DEFAULT_DB_RETRY_BASE_DELAY_MS)
        .max(1);
    let requested_max_delay_ms = retry
        .and_then(|cfg| cfg.max_delay_ms)
        .unwrap_or(DEFAULT_DB_RETRY_MAX_DELAY_MS)
        .max(1);
    let max_delay_ms = std::cmp::max(requested_max_delay_ms, base_delay_ms);
    let jitter_percent = retry
        .and_then(|cfg| cfg.jitter_percent)
        .unwrap_or(DEFAULT_DB_RETRY_JITTER_PERCENT)
        .min(100);

    DbRetryPolicy {
        max_attempts,
        base_delay_ms,
        max_delay_ms,
        jitter_percent,
    }
}

fn resolved_db_circuit_breaker(config: &DbConfig) -> Option<CircuitBreaker> {
    config.circuit_breaker.as_ref().map(|cfg| {
        CircuitBreaker::new(
            cfg,
            DEFAULT_DB_CIRCUIT_FAILURE_THRESHOLD,
            DEFAULT_DB_CIRCUIT_OPEN_TIMEOUT_MS,
        )
    })
}

fn delay_with_backoff_and_jitter_ms(
    attempt_index: u32,
    base_delay_ms: u64,
    max_delay_ms: u64,
    jitter_percent: u32,
) -> u64 {
    let exp = attempt_index.min(16);
    let factor = 2u64.saturating_pow(exp);
    let base_delay = base_delay_ms.saturating_mul(factor);
    let clamped = std::cmp::min(base_delay, max_delay_ms);

    if jitter_percent == 0 {
        return clamped;
    }

    let jitter_window = (clamped.saturating_mul(jitter_percent as u64)) / 100;
    if jitter_window == 0 {
        return clamped;
    }

    let now_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .subsec_nanos() as u64;
    let range = jitter_window.saturating_mul(2).saturating_add(1);
    let offset = now_nanos % range;
    let signed_offset = offset as i128 - jitter_window as i128;
    let candidate = clamped as i128 + signed_offset;
    candidate.max(0) as u64
}

fn is_retryable_db_error(error: &DriverError) -> bool {
    match error {
        DriverError::Db(inner) => match inner {
            rusqlite::Error::SqliteFailure(err, _) => matches!(
                err.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            ),
            _ => false,
        },
        DriverError::Postgres(inner) => {
            if let Some(code) = inner.code() {
                let sqlstate = code.code();
                return sqlstate.starts_with("08")
                    || sqlstate.starts_with("40")
                    || sqlstate == "53300"
                    || sqlstate == "57P03"
                    || sqlstate == "55000";
            }

            let text = inner.to_string().to_ascii_lowercase();
            text.contains("timeout")
                || text.contains("connection")
                || text.contains("could not connect")
                || text.contains("temporar")
        }
        DriverError::Mysql(inner) => {
            let text = inner.to_string().to_ascii_lowercase();
            text.contains("lock wait timeout")
                || text.contains("deadlock")
                || text.contains("server has gone away")
                || text.contains("lost connection")
                || text.contains("timeout")
                || text.contains("too many connections")
        }
        _ => false,
    }
}

fn execute_with_db_retry<T, F>(
    config: &DbConfig,
    circuit: &mut Option<CircuitBreaker>,
    mut op: F,
) -> Result<T, DriverError>
where
    F: FnMut() -> Result<T, DriverError>,
{
    let policy = resolved_db_retry_policy(config);
    for attempt in 1..=policy.max_attempts {
        ensure_circuit_allows_call(circuit, "db")?;

        match op() {
            Ok(value) => {
                record_circuit_success(circuit);
                return Ok(value);
            }
            Err(error) if attempt < policy.max_attempts && is_retryable_db_error(&error) => {
                record_circuit_failure(circuit);
                let delay_ms = delay_with_backoff_and_jitter_ms(
                    attempt - 1,
                    policy.base_delay_ms,
                    policy.max_delay_ms,
                    policy.jitter_percent,
                );
                std::thread::sleep(Duration::from_millis(delay_ms));
            }
            Err(error) => {
                record_circuit_failure(circuit);
                return Err(error);
            }
        }
    }

    Err(DriverError::InvalidResponse(
        "db retry policy exhausted without terminal response".to_string(),
    ))
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
        let mut circuit = resolved_db_circuit_breaker(&self.config);

        match self.config.kind {
            DbKind::Sqlite => fetch_sqlite(&self.config, &self.metadata, &mut circuit),
            DbKind::Postgres => fetch_postgres(&self.config, &self.metadata, &mut circuit),
            DbKind::Mysql => fetch_mysql(&self.config, &self.metadata, &mut circuit),
        }
    }
}

/// Fetch records from a sqlite database and map each row to a JSON object.
fn fetch_sqlite(
    config: &DbConfig,
    metadata: &RecordMetadata,
    circuit: &mut Option<CircuitBreaker>,
) -> Result<Vec<ExternalRecord>, DriverError> {
    execute_with_db_retry(config, circuit, || fetch_sqlite_once(config, metadata))
}

fn fetch_sqlite_once(
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
    let metadata = metadata_with_source_details(metadata, "db/sqlite", None);

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
    circuit: &mut Option<CircuitBreaker>,
) -> Result<Vec<ExternalRecord>, DriverError> {
    execute_with_db_retry(config, circuit, || fetch_postgres_once(config, metadata))
}

fn fetch_postgres_once(
    config: &DbConfig,
    metadata: &RecordMetadata,
) -> Result<Vec<ExternalRecord>, DriverError> {
    let pg_config = config
        .connection
        .parse::<postgres::Config>()
        .map_err(|err| DriverError::InvalidResponse(format!("invalid postgres config: {err}")))?;
    let (min, max) = resolved_pool_bounds(config);

    let rows = match config.postgres_tls_mode.unwrap_or(PostgresTlsMode::Disable) {
        PostgresTlsMode::Disable => {
            let manager = r2d2_postgres::PostgresConnectionManager::new(pg_config, postgres::NoTls);
            let pool = r2d2::Pool::builder()
                .min_idle(Some(min))
                .max_size(max)
                .build(manager)
                .map_err(|err| {
                    DriverError::InvalidResponse(format!("failed to build postgres pool: {err}"))
                })?;
            let mut client = pool.get().map_err(|err| {
                DriverError::InvalidResponse(format!(
                    "failed to get postgres pooled connection: {err}"
                ))
            })?;
            client.query(&config.query, &[])?
        }
        PostgresTlsMode::Require => {
            let tls = native_tls::TlsConnector::builder().build().map_err(|err| {
                DriverError::InvalidResponse(format!(
                    "failed to initialize postgres tls connector: {err}"
                ))
            })?;
            let connector = postgres_native_tls::MakeTlsConnector::new(tls);
            let manager = r2d2_postgres::PostgresConnectionManager::new(pg_config, connector);
            let pool = r2d2::Pool::builder()
                .min_idle(Some(min))
                .max_size(max)
                .build(manager)
                .map_err(|err| {
                    DriverError::InvalidResponse(format!("failed to build postgres pool: {err}"))
                })?;
            let mut client = pool.get().map_err(|err| {
                DriverError::InvalidResponse(format!(
                    "failed to get postgres pooled connection: {err}"
                ))
            })?;
            client.query(&config.query, &[])?
        }
    };

    let metadata = metadata_with_content_type(metadata.clone(), None, "application/json", None);
    let metadata = metadata_with_source_details(metadata, "db/postgres", None);

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
    circuit: &mut Option<CircuitBreaker>,
) -> Result<Vec<ExternalRecord>, DriverError> {
    execute_with_db_retry(config, circuit, || fetch_mysql_once(config, metadata))
}

fn fetch_mysql_once(
    config: &DbConfig,
    metadata: &RecordMetadata,
) -> Result<Vec<ExternalRecord>, DriverError> {
    let (min, max) = resolved_pool_bounds(config);
    let base_opts = mysql::Opts::from_url(&config.connection).map_err(|err| {
        DriverError::InvalidResponse(format!("invalid mysql connection url: {err}"))
    })?;
    let constraints = mysql::PoolConstraints::new(min as usize, max as usize).ok_or_else(|| {
        DriverError::InvalidResponse("invalid mysql pool constraints".to_string())
    })?;
    let pool_opts = mysql::PoolOpts::new().with_constraints(constraints);
    let opts: mysql::Opts = mysql::OptsBuilder::from_opts(base_opts)
        .pool_opts(pool_opts)
        .into();
    let pool = mysql::Pool::new(opts)?;
    let mut conn = pool.get_conn()?;
    let result = conn.query_iter(&config.query)?;
    let columns: Vec<String> = result
        .columns()
        .as_ref()
        .iter()
        .map(|col| col.name_str().to_string())
        .collect();

    let metadata = metadata_with_content_type(metadata.clone(), None, "application/json", None);
    let metadata = metadata_with_source_details(metadata, "db/mysql", None);

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
fn json_value_to_records(
    value: &serde_json::Value,
    metadata: &RecordMetadata,
    config: &RestConfig,
) -> Result<Vec<ExternalRecord>, DriverError> {
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
    let metadata =
        metadata_with_content_type(metadata, None, default_content_type, source.filename());
    metadata_with_source_details(metadata, "file", Some(source.locator()))
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

fn metadata_with_source_details(
    mut metadata: RecordMetadata,
    source_type: &str,
    locator: Option<String>,
) -> RecordMetadata {
    if metadata.source_details.is_none() {
        metadata.source_details = Some(SourceDetails {
            source_type: source_type.to_string(),
            locator,
        });
    }
    metadata
}
