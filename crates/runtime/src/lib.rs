use std::collections::BTreeMap;
use std::fs;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::{DeadLetter, ExternalRecord, IntegrationRecord, InterfaceRef, Payload, PayloadFormat};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum InterfaceError {
    #[error("failed to read interface file: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse interface json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid interface definition:\n{0}")]
    Validation(#[from] ValidationErrors),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError {
    pub path: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationErrors(pub Vec<ValidationError>);

impl std::fmt::Display for ValidationErrors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() {
            return write!(f, "(no validation errors)");
        }

        writeln!(f, "{} error(s):", self.0.len())?;
        for error in &self.0 {
            writeln!(f, "- {}: {}", error.path, error.message)?;
        }
        Ok(())
    }
}

impl std::error::Error for ValidationErrors {}

/// External system interface definition for the integration pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalInterface {
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub driver: DriverSpec,
    #[serde(default)]
    pub payload_format: PayloadFormat,
    #[serde(default)]
    pub record_id_paths: Vec<String>,
    #[serde(default)]
    pub required_paths: Vec<String>,
}

/// Driver selection and connection details for an external system.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriverSpec {
    #[serde(default)]
    pub kind: DriverKind,
    #[serde(default)]
    pub input: Option<String>,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub filename: Option<String>,
    #[serde(default)]
    pub rest: Option<RestDriverConfig>,
    #[serde(default)]
    pub db: Option<DbDriverConfig>,
}

impl Default for DriverSpec {
    fn default() -> Self {
        Self {
            kind: DriverKind::Jsonl,
            input: None,
            content_type: None,
            filename: None,
            rest: None,
            db: None,
        }
    }
}

/// Supported external driver kinds.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DriverKind {
    #[default]
    Jsonl,
    Text,
    Binary,
    Rest,
    Db,
}

/// REST driver configuration payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestDriverConfig {
    pub url: String,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub body: Option<String>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub response_format: Option<PayloadFormat>,
    #[serde(default)]
    pub items_pointer: Option<String>,
}

/// DB driver configuration payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DbDriverConfig {
    #[serde(default)]
    pub kind: DbKind,
    pub connection: String,
    pub query: String,
}

/// Supported database kinds for DB drivers.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DbKind {
    #[default]
    Sqlite,
    Postgres,
    Mysql,
}

impl ExternalInterface {
    /// Load an interface definition from disk.
    pub fn load(path: &std::path::Path) -> Result<Self, InterfaceError> {
        let content = fs::read_to_string(path)?;
        let interface: ExternalInterface = serde_json::from_str(&content)?;
        interface.validate()?;
        Ok(interface)
    }

    pub fn validate(&self) -> Result<(), ValidationErrors> {
        let mut errors = Vec::new();

        if self.name.trim().is_empty() {
            errors.push(ValidationError {
                path: "/name".to_string(),
                message: "must be a non-empty string".to_string(),
            });
        }

        if self.version.trim().is_empty() {
            errors.push(ValidationError {
                path: "/version".to_string(),
                message: "must be a non-empty string".to_string(),
            });
        }

        validate_pointer_list(&mut errors, "/required_paths", &self.required_paths);
        validate_pointer_list(&mut errors, "/record_id_paths", &self.record_id_paths);

        validate_unique_list(&mut errors, "/required_paths", &self.required_paths);
        validate_unique_list(&mut errors, "/record_id_paths", &self.record_id_paths);

        match self.driver.kind {
            DriverKind::Rest => {
                if self.driver.rest.is_none() {
                    errors.push(ValidationError {
                        path: "/driver/rest".to_string(),
                        message: "rest config is required when driver.kind is 'rest'".to_string(),
                    });
                }

                if self.driver.db.is_some() {
                    errors.push(ValidationError {
                        path: "/driver/db".to_string(),
                        message: "db config must be omitted when driver.kind is 'rest'".to_string(),
                    });
                }

                if self.driver.input.is_some() {
                    errors.push(ValidationError {
                        path: "/driver/input".to_string(),
                        message: "input must be omitted when driver.kind is 'rest'".to_string(),
                    });
                }

                if let Some(rest) = &self.driver.rest {
                    if rest.url.trim().is_empty() {
                        errors.push(ValidationError {
                            path: "/driver/rest/url".to_string(),
                            message: "url is required for rest driver".to_string(),
                        });
                    }

                    if let Some(method) = &rest.method {
                        if method.trim().is_empty() {
                            errors.push(ValidationError {
                                path: "/driver/rest/method".to_string(),
                                message: "method must be a non-empty string when provided"
                                    .to_string(),
                            });
                        }
                    }

                    if let Some(timeout_ms) = rest.timeout_ms {
                        if timeout_ms == 0 {
                            errors.push(ValidationError {
                                path: "/driver/rest/timeout_ms".to_string(),
                                message: "timeout_ms must be > 0 when provided".to_string(),
                            });
                        }
                    }

                    if let Some(items_pointer) = &rest.items_pointer {
                        if let Err(message) = validate_json_pointer(items_pointer) {
                            errors.push(ValidationError {
                                path: "/driver/rest/items_pointer".to_string(),
                                message: message.to_string(),
                            });
                        }
                    }
                }
            }
            DriverKind::Db => {
                if self.driver.db.is_none() {
                    errors.push(ValidationError {
                        path: "/driver/db".to_string(),
                        message: "db config is required when driver.kind is 'db'".to_string(),
                    });
                }

                if self.driver.rest.is_some() {
                    errors.push(ValidationError {
                        path: "/driver/rest".to_string(),
                        message: "rest config must be omitted when driver.kind is 'db'".to_string(),
                    });
                }

                if self.driver.input.is_some() {
                    errors.push(ValidationError {
                        path: "/driver/input".to_string(),
                        message: "input must be omitted when driver.kind is 'db'".to_string(),
                    });
                }

                if let Some(db) = &self.driver.db {
                    if db.connection.trim().is_empty() {
                        errors.push(ValidationError {
                            path: "/driver/db/connection".to_string(),
                            message: "connection is required for db driver".to_string(),
                        });
                    }

                    if db.query.trim().is_empty() {
                        errors.push(ValidationError {
                            path: "/driver/db/query".to_string(),
                            message: "query is required for db driver".to_string(),
                        });
                    }
                }
            }
            _ => {
                if self.driver.rest.is_some() {
                    errors.push(ValidationError {
                        path: "/driver/rest".to_string(),
                        message: "rest config must be omitted when driver.kind is not 'rest'"
                            .to_string(),
                    });
                }

                if self.driver.db.is_some() {
                    errors.push(ValidationError {
                        path: "/driver/db".to_string(),
                        message: "db config must be omitted when driver.kind is not 'db'"
                            .to_string(),
                    });
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(ValidationErrors(errors))
        }
    }

    /// Convert to a lightweight interface reference.
    pub fn reference(&self) -> InterfaceRef {
        InterfaceRef {
            name: self.name.clone(),
            version: self.version.clone(),
        }
    }
}

fn validate_pointer_list(errors: &mut Vec<ValidationError>, base_path: &str, pointers: &[String]) {
    for (idx, pointer) in pointers.iter().enumerate() {
        if let Err(message) = validate_json_pointer(pointer) {
            errors.push(ValidationError {
                path: format!("{}/{}", base_path, idx),
                message: message.to_string(),
            });
        }
    }
}

fn validate_unique_list(errors: &mut Vec<ValidationError>, base_path: &str, values: &[String]) {
    let mut seen = std::collections::HashSet::new();
    for (idx, value) in values.iter().enumerate() {
        if !seen.insert(value) {
            errors.push(ValidationError {
                path: format!("{}/{}", base_path, idx),
                message: "duplicate entry".to_string(),
            });
        }
    }
}

fn validate_json_pointer(pointer: &str) -> Result<(), &'static str> {
    if pointer != pointer.trim() {
        return Err("must not contain leading/trailing whitespace");
    }

    if pointer.is_empty() {
        return Err("must be a non-empty JSON Pointer starting with '/'");
    }

    if !pointer.starts_with('/') {
        return Err("must be a JSON Pointer starting with '/'");
    }

    let bytes = pointer.as_bytes();
    let mut idx = 0;
    while idx < bytes.len() {
        if bytes[idx] == b'~' {
            if idx + 1 >= bytes.len() {
                return Err("invalid JSON Pointer escape: '~' must be followed by '0' or '1'");
            }
            let next = bytes[idx + 1];
            if next != b'0' && next != b'1' {
                return Err("invalid JSON Pointer escape: '~' must be followed by '0' or '1'");
            }
            idx += 2;
            continue;
        }
        idx += 1;
    }

    Ok(())
}

#[derive(Debug, Default)]
pub struct IntegrationOutcome {
    pub records: Vec<IntegrationRecord>,
    pub dead_letters: Vec<DeadLetter>,
}

/// Pipeline that validates and annotates raw external records.
pub struct IntegrationPipeline {
    interface: ExternalInterface,
}

impl IntegrationPipeline {
    /// Create a pipeline for a given external interface definition.
    pub fn new(interface: ExternalInterface) -> Self {
        Self { interface }
    }

    /// Validate raw records, emit normalized records or dead letters.
    pub fn integrate(&self, source: &str, records: Vec<ExternalRecord>) -> IntegrationOutcome {
        let mut outcome = IntegrationOutcome::default();

        for record in records {
            let (errors, warnings) = self.validate_and_warn(&record.payload);
            if !errors.is_empty() {
                outcome.dead_letters.push(DeadLetter {
                    source: source.to_string(),
                    interface: self.interface.reference(),
                    payload: record.payload,
                    metadata: record.metadata,
                    errors,
                });
                continue;
            }

            let record_id = self.build_record_id(&record.payload);
            let ingested_at_unix_ms = unix_ms_now();

            outcome.records.push(IntegrationRecord {
                source: source.to_string(),
                interface: self.interface.reference(),
                record_id,
                ingested_at_unix_ms,
                payload: record.payload,
                metadata: record.metadata,
                warnings,
            });
        }

        outcome
    }

    /// Validate payload and emit warnings without mutating the payload.
    fn validate_and_warn(&self, payload: &Payload) -> (Vec<String>, Vec<String>) {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        let payload_kind = payload_kind(payload);
        if !self.matches_payload_format(payload_kind) {
            errors.push(format!(
                "payload format mismatch: expected {:?}, got {:?}",
                self.interface.payload_format, payload_kind
            ));
            return (errors, warnings);
        }

        if let Payload::Json(value) = payload {
            for pointer in &self.interface.required_paths {
                if !pointer_exists(value, pointer) {
                    errors.push(format!("missing required path {pointer}"));
                }
            }

            for pointer in &self.interface.record_id_paths {
                if !pointer_exists(value, pointer) {
                    warnings.push(format!("missing record id path {pointer}"));
                }
            }
        } else if !self.interface.required_paths.is_empty()
            || !self.interface.record_id_paths.is_empty()
        {
            warnings.push("interface paths ignored for non-json payload".to_string());
        }

        (errors, warnings)
    }

    /// Build an idempotent record id using the interface key rules.
    fn build_record_id(&self, payload: &Payload) -> String {
        if let Payload::Json(value) = payload {
            let mut parts = Vec::new();
            for pointer in &self.interface.record_id_paths {
                if let Some(value) = value.pointer(pointer) {
                    parts.push(value_to_string(value));
                }
            }

            if !parts.is_empty() {
                return parts.join("|");
            }
        }

        hash_payload(payload)
    }

    /// Check payload format against interface expectations.
    fn matches_payload_format(&self, payload_kind: PayloadFormat) -> bool {
        match self.interface.payload_format {
            PayloadFormat::Unknown => true,
            expected => expected == payload_kind,
        }
    }
}

fn payload_kind(payload: &Payload) -> PayloadFormat {
    match payload {
        Payload::Json(_) => PayloadFormat::Json,
        Payload::Text(_) => PayloadFormat::Text,
        Payload::Binary { .. } => PayloadFormat::Binary,
    }
}

fn pointer_exists(payload: &serde_json::Value, pointer: &str) -> bool {
    match payload.pointer(pointer) {
        Some(value) => !value.is_null(),
        None => false,
    }
}

fn value_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(inner) => inner.clone(),
        serde_json::Value::Number(inner) => inner.to_string(),
        serde_json::Value::Bool(inner) => inner.to_string(),
        _ => serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()),
    }
}

fn hash_payload(payload: &Payload) -> String {
    let bytes = payload.to_bytes();
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn unix_ms_now() -> i64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0));
    let millis = duration.as_secs() * 1000 + u64::from(duration.subsec_millis());
    millis as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_and_validate(json: &str) -> Result<ExternalInterface, ValidationErrors> {
        let interface: ExternalInterface =
            serde_json::from_str(json).expect("interface json parses");
        interface.validate()?;
        Ok(interface)
    }

    fn has_path(errors: &ValidationErrors, path: &str) -> bool {
        errors.0.iter().any(|error| error.path == path)
    }

    #[test]
    fn fixtures_are_valid() {
        let fixtures = [
            include_str!("../../../tests/fixtures/interfaces/rest.sample.json"),
            include_str!("../../../tests/fixtures/interfaces/mes.db.json"),
            include_str!("../../../tests/fixtures/interfaces/qms.db.json"),
            include_str!("../../../tests/fixtures/interfaces/postgres.sample.json"),
            include_str!("../../../tests/fixtures/interfaces/mysql.sample.json"),
        ];

        for fixture in fixtures {
            parse_and_validate(fixture).unwrap();
        }
    }

    #[test]
    fn denies_unknown_top_level_fields() {
        let json = r#"{
            "name": "mes",
            "version": "v1",
            "unknown": true
        }"#;

        let err = serde_json::from_str::<ExternalInterface>(json).unwrap_err();
        assert!(err.is_data());
        assert!(err.to_string().contains("unknown"));
    }

    #[test]
    fn errors_when_kind_rest_without_rest_config() {
        let json = r#"{
            "name": "rest-sample",
            "version": "v1",
            "driver": { "kind": "rest" },
            "payload_format": "json"
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        let errors = interface.validate().unwrap_err();
        assert!(has_path(&errors, "/driver/rest"));
    }

    #[test]
    fn errors_when_rest_config_present_but_kind_not_rest() {
        let json = r#"{
            "name": "bad",
            "version": "v1",
            "driver": {
                "kind": "jsonl",
                "rest": { "url": "https://example.com" }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        let errors = interface.validate().unwrap_err();
        assert!(has_path(&errors, "/driver/rest"));
    }

    #[test]
    fn errors_on_invalid_required_paths_pointer() {
        let json = r#"{
            "name": "mes",
            "version": "v1",
            "driver": { "kind": "jsonl", "input": "-" },
            "required_paths": ["defect_id"]
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        let errors = interface.validate().unwrap_err();
        assert!(has_path(&errors, "/required_paths/0"));
    }

    #[test]
    fn errors_on_duplicate_pointers() {
        let json = r#"{
            "name": "mes",
            "version": "v1",
            "driver": { "kind": "jsonl", "input": "-" },
            "required_paths": ["/defect_id", "/defect_id"]
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        let errors = interface.validate().unwrap_err();
        assert!(has_path(&errors, "/required_paths/1"));
    }

    #[test]
    fn errors_on_invalid_items_pointer() {
        let json = r#"{
            "name": "rest-sample",
            "version": "v1",
            "driver": {
                "kind": "rest",
                "rest": {
                    "url": "https://api.example.com/events",
                    "items_pointer": "items"
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        let errors = interface.validate().unwrap_err();
        assert!(has_path(&errors, "/driver/rest/items_pointer"));
    }
}
