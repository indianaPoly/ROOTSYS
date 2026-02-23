use std::collections::BTreeMap;
use std::fs;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::{
    DeadLetter, ExternalRecord, IntegrationRecord, InterfaceRef, Payload, PayloadFormat,
    ValidationMessage,
};
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
    #[error("failed to read contract registry file: {0}")]
    ContractRegistryIo(#[source] std::io::Error),
    #[error("failed to parse contract registry json: {0}")]
    ContractRegistryJson(#[source] serde_json::Error),
    #[error("invalid contract registry definition:\n{0}")]
    ContractRegistryValidation(#[from] ContractRegistryValidationErrors),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError {
    pub code: String,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractRegistryValidationError {
    pub code: String,
    pub path: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractRegistryValidationErrors(pub Vec<ContractRegistryValidationError>);

impl std::fmt::Display for ContractRegistryValidationErrors {
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

impl std::error::Error for ContractRegistryValidationErrors {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContractRegistry {
    pub allowlist: Vec<AllowedInterface>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllowedInterface {
    pub name: String,
    pub version: String,
}

impl ContractRegistry {
    pub fn load(path: &std::path::Path) -> Result<Self, InterfaceError> {
        let content = fs::read_to_string(path).map_err(InterfaceError::ContractRegistryIo)?;
        let registry: ContractRegistry =
            serde_json::from_str(&content).map_err(InterfaceError::ContractRegistryJson)?;
        registry.validate()?;
        Ok(registry)
    }

    pub fn validate(&self) -> Result<(), ContractRegistryValidationErrors> {
        let mut errors = Vec::new();

        if self.allowlist.is_empty() {
            errors.push(ContractRegistryValidationError {
                code: "CONTRACT_REGISTRY_EMPTY_ALLOWLIST".to_string(),
                path: "/allowlist".to_string(),
                message: "must contain at least one (name, version) pair".to_string(),
            });
        }

        let mut seen = std::collections::HashSet::new();
        for (idx, entry) in self.allowlist.iter().enumerate() {
            if entry.name.trim().is_empty() {
                errors.push(ContractRegistryValidationError {
                    code: "CONTRACT_REGISTRY_EMPTY_NAME".to_string(),
                    path: format!("/allowlist/{idx}/name"),
                    message: "must be a non-empty string".to_string(),
                });
            }

            if entry.version.trim().is_empty() {
                errors.push(ContractRegistryValidationError {
                    code: "CONTRACT_REGISTRY_EMPTY_VERSION".to_string(),
                    path: format!("/allowlist/{idx}/version"),
                    message: "must be a non-empty string".to_string(),
                });
            }

            let key = (entry.name.clone(), entry.version.clone());
            if !seen.insert(key) {
                errors.push(ContractRegistryValidationError {
                    code: "CONTRACT_REGISTRY_DUPLICATE_ENTRY".to_string(),
                    path: format!("/allowlist/{idx}"),
                    message: "duplicate (name, version) entry".to_string(),
                });
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(ContractRegistryValidationErrors(errors))
        }
    }

    pub fn is_allowed(&self, name: &str, version: &str) -> bool {
        self.allowlist
            .iter()
            .any(|entry| entry.name == name && entry.version == version)
    }
}

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
    #[serde(default)]
    pub record_id_policy: RecordIdPolicy,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecordIdPolicy {
    Strict,
    #[default]
    HashFallback,
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
                code: "INTERFACE_NAME_EMPTY".to_string(),
                path: "/name".to_string(),
                message: "must be a non-empty string".to_string(),
            });
        }

        if self.version.trim().is_empty() {
            errors.push(ValidationError {
                code: "INTERFACE_VERSION_EMPTY".to_string(),
                path: "/version".to_string(),
                message: "must be a non-empty string".to_string(),
            });
        }

        validate_pointer_list(&mut errors, "/required_paths", &self.required_paths);
        validate_pointer_list(&mut errors, "/record_id_paths", &self.record_id_paths);

        validate_unique_list(&mut errors, "/required_paths", &self.required_paths);
        validate_unique_list(&mut errors, "/record_id_paths", &self.record_id_paths);

        if self.record_id_policy == RecordIdPolicy::Strict && self.record_id_paths.is_empty() {
            errors.push(ValidationError {
                code: "RECORD_ID_POLICY_STRICT_REQUIRES_PATHS".to_string(),
                path: "/record_id_paths".to_string(),
                message: "must contain at least one pointer when record_id_policy is 'strict'"
                    .to_string(),
            });
        }

        match self.driver.kind {
            DriverKind::Rest => {
                if self.driver.rest.is_none() {
                    errors.push(ValidationError {
                        code: "REST_CONFIG_REQUIRED".to_string(),
                        path: "/driver/rest".to_string(),
                        message: "rest config is required when driver.kind is 'rest'".to_string(),
                    });
                }

                if self.driver.db.is_some() {
                    errors.push(ValidationError {
                        code: "REST_DB_CONFLICT".to_string(),
                        path: "/driver/db".to_string(),
                        message: "db config must be omitted when driver.kind is 'rest'".to_string(),
                    });
                }

                if self.driver.input.is_some() {
                    errors.push(ValidationError {
                        code: "REST_INPUT_CONFLICT".to_string(),
                        path: "/driver/input".to_string(),
                        message: "input must be omitted when driver.kind is 'rest'".to_string(),
                    });
                }

                if let Some(rest) = &self.driver.rest {
                    if rest.url.trim().is_empty() {
                        errors.push(ValidationError {
                            code: "REST_URL_EMPTY".to_string(),
                            path: "/driver/rest/url".to_string(),
                            message: "url is required for rest driver".to_string(),
                        });
                    }

                    if let Some(method) = &rest.method {
                        if method.trim().is_empty() {
                            errors.push(ValidationError {
                                code: "REST_METHOD_EMPTY".to_string(),
                                path: "/driver/rest/method".to_string(),
                                message: "method must be a non-empty string when provided"
                                    .to_string(),
                            });
                        }
                    }

                    if let Some(timeout_ms) = rest.timeout_ms {
                        if timeout_ms == 0 {
                            errors.push(ValidationError {
                                code: "REST_TIMEOUT_INVALID".to_string(),
                                path: "/driver/rest/timeout_ms".to_string(),
                                message: "timeout_ms must be > 0 when provided".to_string(),
                            });
                        }
                    }

                    if let Some(items_pointer) = &rest.items_pointer {
                        if let Err(message) = validate_json_pointer(items_pointer) {
                            errors.push(ValidationError {
                                code: "REST_ITEMS_POINTER_INVALID".to_string(),
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
                        code: "DB_CONFIG_REQUIRED".to_string(),
                        path: "/driver/db".to_string(),
                        message: "db config is required when driver.kind is 'db'".to_string(),
                    });
                }

                if self.driver.rest.is_some() {
                    errors.push(ValidationError {
                        code: "DB_REST_CONFLICT".to_string(),
                        path: "/driver/rest".to_string(),
                        message: "rest config must be omitted when driver.kind is 'db'".to_string(),
                    });
                }

                if self.driver.input.is_some() {
                    errors.push(ValidationError {
                        code: "DB_INPUT_CONFLICT".to_string(),
                        path: "/driver/input".to_string(),
                        message: "input must be omitted when driver.kind is 'db'".to_string(),
                    });
                }

                if let Some(db) = &self.driver.db {
                    if db.connection.trim().is_empty() {
                        errors.push(ValidationError {
                            code: "DB_CONNECTION_EMPTY".to_string(),
                            path: "/driver/db/connection".to_string(),
                            message: "connection is required for db driver".to_string(),
                        });
                    }

                    if db.query.trim().is_empty() {
                        errors.push(ValidationError {
                            code: "DB_QUERY_EMPTY".to_string(),
                            path: "/driver/db/query".to_string(),
                            message: "query is required for db driver".to_string(),
                        });
                    }
                }
            }
            _ => {
                if self.driver.rest.is_some() {
                    errors.push(ValidationError {
                        code: "DRIVER_REST_UNEXPECTED".to_string(),
                        path: "/driver/rest".to_string(),
                        message: "rest config must be omitted when driver.kind is not 'rest'"
                            .to_string(),
                    });
                }

                if self.driver.db.is_some() {
                    errors.push(ValidationError {
                        code: "DRIVER_DB_UNEXPECTED".to_string(),
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

    pub fn validate_against_registry(
        &self,
        registry: &ContractRegistry,
    ) -> Result<(), ValidationErrors> {
        if registry.is_allowed(&self.name, &self.version) {
            Ok(())
        } else {
            Err(ValidationErrors(vec![ValidationError {
                code: "CONTRACT_NOT_ALLOWLISTED".to_string(),
                path: "/name".to_string(),
                message: format!(
                    "interface '{}:{}' is not allowlisted in contract registry",
                    self.name, self.version
                ),
            }]))
        }
    }
}

fn validate_pointer_list(errors: &mut Vec<ValidationError>, base_path: &str, pointers: &[String]) {
    for (idx, pointer) in pointers.iter().enumerate() {
        if let Err(message) = validate_json_pointer(pointer) {
            errors.push(ValidationError {
                code: "JSON_POINTER_INVALID".to_string(),
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
                code: "DUPLICATE_ENTRY".to_string(),
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

            let record_id = match self.build_record_id(&record.payload) {
                Ok(value) => value,
                Err(error) => {
                    outcome.dead_letters.push(DeadLetter {
                        source: source.to_string(),
                        interface: self.interface.reference(),
                        payload: record.payload,
                        metadata: record.metadata,
                        errors: vec![error],
                    });
                    continue;
                }
            };
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
    fn validate_and_warn(
        &self,
        payload: &Payload,
    ) -> (Vec<ValidationMessage>, Vec<ValidationMessage>) {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        let payload_kind = payload_kind(payload);
        if !self.matches_payload_format(payload_kind) {
            errors.push(ValidationMessage::new(
                "PAYLOAD_FORMAT_MISMATCH",
                Some("/payload".to_string()),
                format!(
                    "payload format mismatch: expected {:?}, got {:?}",
                    self.interface.payload_format, payload_kind
                ),
            ));
            return (errors, warnings);
        }

        if let Payload::Json(value) = payload {
            for pointer in &self.interface.required_paths {
                if !pointer_exists(value, pointer) {
                    errors.push(ValidationMessage::new(
                        "MISSING_REQUIRED_PATH",
                        Some(pointer.clone()),
                        format!("missing required path {pointer}"),
                    ));
                }
            }

            for pointer in &self.interface.record_id_paths {
                if !pointer_exists(value, pointer) {
                    warnings.push(ValidationMessage::new(
                        "MISSING_RECORD_ID_PATH",
                        Some(pointer.clone()),
                        format!("missing record id path {pointer}"),
                    ));
                }
            }
        } else if !self.interface.required_paths.is_empty()
            || !self.interface.record_id_paths.is_empty()
        {
            warnings.push(ValidationMessage::new(
                "PATHS_IGNORED_FOR_NON_JSON",
                None,
                "interface paths ignored for non-json payload".to_string(),
            ));
        }

        (errors, warnings)
    }

    /// Build an idempotent record id using the interface key rules.
    fn build_record_id(&self, payload: &Payload) -> Result<String, ValidationMessage> {
        if let Payload::Json(value) = payload {
            let mut parts = Vec::new();
            let mut missing = Vec::new();
            for pointer in &self.interface.record_id_paths {
                match value.pointer(pointer) {
                    Some(inner) if !inner.is_null() => parts.push(value_to_string(inner)),
                    _ => missing.push(pointer.clone()),
                }
            }

            if !parts.is_empty() {
                return Ok(parts.join("|"));
            }

            if self.interface.record_id_policy == RecordIdPolicy::Strict {
                return Err(ValidationMessage::new(
                    "RECORD_ID_STRICT_PATHS_UNRESOLVED",
                    Some("/record_id_paths".to_string()),
                    format!(
                        "record_id strict policy violation: failed to resolve record id from paths: {}",
                        missing.join(", ")
                    ),
                ));
            }
        } else if self.interface.record_id_policy == RecordIdPolicy::Strict {
            return Err(ValidationMessage::new(
                "RECORD_ID_STRICT_NON_JSON",
                Some("/record_id_paths".to_string()),
                "record_id strict policy violation: strict mode requires JSON payload with resolvable record_id_paths"
                    .to_string(),
            ));
        }

        Ok(hash_payload(payload))
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

    fn has_code(errors: &ValidationErrors, code: &str) -> bool {
        errors.0.iter().any(|error| error.code == code)
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

    #[test]
    fn strict_record_id_policy_requires_paths() {
        let json = r#"{
            "name": "mes",
            "version": "v1",
            "record_id_policy": "strict",
            "driver": { "kind": "jsonl", "input": "-" }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        let errors = interface.validate().unwrap_err();
        assert!(has_path(&errors, "/record_id_paths"));
        assert!(has_code(&errors, "RECORD_ID_POLICY_STRICT_REQUIRES_PATHS"));
    }

    #[test]
    fn strict_record_id_policy_dead_letters_when_paths_missing() {
        let interface_json = r#"{
            "name": "mes",
            "version": "v1",
            "record_id_policy": "strict",
            "record_id_paths": ["/defect_id"],
            "driver": { "kind": "jsonl", "input": "-" },
            "payload_format": "json"
        }"#;
        let interface: ExternalInterface = serde_json::from_str(interface_json).unwrap();
        interface.validate().unwrap();

        let pipeline = IntegrationPipeline::new(interface);
        let input = ExternalRecord {
            payload: Payload::from_json(serde_json::json!({ "other": 1 })),
            metadata: Default::default(),
        };

        let output = pipeline.integrate("mes", vec![input]);
        assert_eq!(output.records.len(), 0);
        assert_eq!(output.dead_letters.len(), 1);
        assert_eq!(
            output.dead_letters[0].errors[0].code,
            "RECORD_ID_STRICT_PATHS_UNRESOLVED"
        );
        assert!(output.dead_letters[0].errors[0]
            .message
            .contains("strict policy violation"));
    }

    #[test]
    fn hash_fallback_policy_uses_hash_when_paths_missing() {
        let interface_json = r#"{
            "name": "mes",
            "version": "v1",
            "record_id_paths": ["/defect_id"],
            "driver": { "kind": "jsonl", "input": "-" },
            "payload_format": "json"
        }"#;
        let interface: ExternalInterface = serde_json::from_str(interface_json).unwrap();
        interface.validate().unwrap();

        let pipeline = IntegrationPipeline::new(interface);
        let input = ExternalRecord {
            payload: Payload::from_json(serde_json::json!({ "other": 1 })),
            metadata: Default::default(),
        };

        let output = pipeline.integrate("mes", vec![input]);
        assert_eq!(output.records.len(), 1);
        assert_eq!(output.dead_letters.len(), 0);
        assert!(!output.records[0].record_id.is_empty());
    }

    #[test]
    fn contract_registry_rejects_non_allowlisted_interface() {
        let interface = ExternalInterface {
            name: "unknown".to_string(),
            version: "v1".to_string(),
            driver: DriverSpec::default(),
            payload_format: PayloadFormat::Unknown,
            record_id_paths: vec![],
            required_paths: vec![],
            record_id_policy: RecordIdPolicy::HashFallback,
        };

        let registry = ContractRegistry {
            allowlist: vec![AllowedInterface {
                name: "mes".to_string(),
                version: "v1".to_string(),
            }],
        };

        let err = interface.validate_against_registry(&registry).unwrap_err();
        assert!(has_path(&err, "/name"));
        assert!(has_code(&err, "CONTRACT_NOT_ALLOWLISTED"));
    }

    #[test]
    fn contract_registry_accepts_allowlisted_interface() {
        let interface = ExternalInterface {
            name: "mes".to_string(),
            version: "v1".to_string(),
            driver: DriverSpec::default(),
            payload_format: PayloadFormat::Unknown,
            record_id_paths: vec![],
            required_paths: vec![],
            record_id_policy: RecordIdPolicy::HashFallback,
        };

        let registry = ContractRegistry {
            allowlist: vec![AllowedInterface {
                name: "mes".to_string(),
                version: "v1".to_string(),
            }],
        };

        interface.validate_against_registry(&registry).unwrap();
    }
}
