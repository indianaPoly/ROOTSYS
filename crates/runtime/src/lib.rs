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
}

/// External system interface definition for the integration pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
pub struct DbDriverConfig {
    #[serde(default)]
    pub kind: DbKind,
    pub connection: String,
    pub query: String,
}

/// Supported database kinds (only sqlite is implemented right now).
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
        let interface = serde_json::from_str(&content)?;
        Ok(interface)
    }

    /// Convert to a lightweight interface reference.
    pub fn reference(&self) -> InterfaceRef {
        InterfaceRef {
            name: self.name.clone(),
            version: self.version.clone(),
        }
    }
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
