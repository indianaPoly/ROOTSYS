use serde::{Deserialize, Serialize};
use serde_json::Value;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;

/// Reference to the external interface definition that produced a record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InterfaceRef {
    pub name: String,
    pub version: String,
}

/// Supported payload encodings for data integration.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PayloadFormat {
    Json,
    Text,
    Binary,
    #[default]
    Unknown,
}

/// Raw payload container that preserves the original data as-is.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data")]
pub enum Payload {
    Json(Value),
    Text(String),
    Binary { encoding: String, data: String },
}

impl Payload {
    /// Wrap a JSON value as a payload.
    pub fn from_json(value: Value) -> Self {
        Self::Json(value)
    }

    /// Wrap plain text as a payload.
    pub fn from_text(text: String) -> Self {
        Self::Text(text)
    }

    /// Wrap raw bytes as a payload (base64 encoded).
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self::Binary {
            encoding: "base64".to_string(),
            data: STANDARD.encode(bytes),
        }
    }

    /// Decode payload into raw bytes (JSON is serialized to UTF-8 bytes).
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Payload::Json(value) => serde_json::to_vec(value).unwrap_or_default(),
            Payload::Text(text) => text.as_bytes().to_vec(),
            Payload::Binary { encoding, data } => {
                if encoding == "base64" {
                    STANDARD.decode(data.as_bytes()).unwrap_or_default()
                } else {
                    Vec::new()
                }
            }
        }
    }
}

/// Optional metadata captured alongside raw payloads.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RecordMetadata {
    pub content_type: Option<String>,
    pub filename: Option<String>,
    #[serde(default)]
    pub source_details: Option<SourceDetails>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceDetails {
    pub source_type: String,
    #[serde(default)]
    pub locator: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ValidationMessage {
    pub code: String,
    pub path: Option<String>,
    pub message: String,
}

impl ValidationMessage {
    pub fn new(code: &str, path: Option<String>, message: String) -> Self {
        Self {
            code: code.to_string(),
            path,
            message,
        }
    }
}

/// Raw external record fetched from an external system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalRecord {
    pub payload: Payload,
    #[serde(default)]
    pub metadata: RecordMetadata,
}

/// Normalized record emitted by the integration pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrationRecord {
    pub source: String,
    pub interface: InterfaceRef,
    pub record_id: String,
    pub ingested_at_unix_ms: i64,
    pub payload: Payload,
    #[serde(default)]
    pub metadata: RecordMetadata,
    pub warnings: Vec<ValidationMessage>,
}

/// Record rejected by the pipeline with the associated errors.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetter {
    pub source: String,
    pub interface: InterfaceRef,
    pub payload: Payload,
    #[serde(default)]
    pub metadata: RecordMetadata,
    #[serde(default)]
    pub reason_codes: Vec<String>,
    #[serde(default)]
    pub lineage: Option<DlqLineage>,
    pub errors: Vec<ValidationMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqLineage {
    pub rejected_at_unix_ms: i64,
    pub pipeline_stage: String,
    pub driver_kind: String,
    pub record_id_policy: String,
    #[serde(default)]
    pub source_type: Option<String>,
    #[serde(default)]
    pub source_locator: Option<String>,
}
