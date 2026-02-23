use std::collections::BTreeMap;
use std::fs;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::{
    DeadLetter, DlqLineage, ExternalRecord, IntegrationRecord, InterfaceRef, Payload,
    PayloadFormat, ValidationMessage,
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
    #[serde(default)]
    pub auth: Option<RestAuthConfig>,
    #[serde(default)]
    pub pagination: Option<RestPaginationConfig>,
    #[serde(default)]
    pub retry: Option<RestRetryConfig>,
    #[serde(default)]
    pub circuit_breaker: Option<CircuitBreakerConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestRetryConfig {
    #[serde(default)]
    pub max_attempts: Option<u32>,
    #[serde(default)]
    pub base_delay_ms: Option<u64>,
    #[serde(default)]
    pub max_delay_ms: Option<u64>,
    #[serde(default)]
    pub jitter_percent: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestPaginationConfig {
    #[serde(default)]
    pub kind: RestPaginationKind,
    #[serde(default)]
    pub cursor: Option<CursorPaginationConfig>,
    #[serde(default)]
    pub page: Option<PagePaginationConfig>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RestPaginationKind {
    Cursor,
    #[default]
    Page,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CursorPaginationConfig {
    pub cursor_param: String,
    pub cursor_path: String,
    #[serde(default)]
    pub initial_cursor: Option<String>,
    #[serde(default)]
    pub max_pages: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PagePaginationConfig {
    pub page_param: String,
    pub page_size_param: String,
    pub page_size: u32,
    #[serde(default)]
    pub initial_page: Option<u32>,
    #[serde(default)]
    pub max_pages: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestAuthConfig {
    #[serde(default)]
    pub kind: RestAuthKind,
    #[serde(default)]
    pub api_key: Option<ApiKeyAuthConfig>,
    #[serde(default)]
    pub oauth2_client_credentials: Option<OAuth2ClientCredentialsConfig>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RestAuthKind {
    #[default]
    ApiKey,
    #[serde(rename = "oauth2_client_credentials")]
    OAuth2ClientCredentials,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiKeyAuthConfig {
    #[serde(rename = "in")]
    pub location: ApiKeyLocation,
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OAuth2ClientCredentialsConfig {
    pub token_url: String,
    pub client_id: String,
    pub client_secret: String,
    #[serde(default)]
    pub scope: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ApiKeyLocation {
    Header,
    Query,
}

/// DB driver configuration payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DbDriverConfig {
    #[serde(default)]
    pub kind: DbKind,
    pub connection: String,
    pub query: String,
    #[serde(default)]
    pub postgres_tls_mode: Option<PostgresTlsMode>,
    #[serde(default)]
    pub pool: Option<DbPoolConfig>,
    #[serde(default)]
    pub retry: Option<DbRetryConfig>,
    #[serde(default)]
    pub circuit_breaker: Option<CircuitBreakerConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircuitBreakerConfig {
    #[serde(default)]
    pub failure_threshold: Option<u32>,
    #[serde(default)]
    pub open_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DbPoolConfig {
    #[serde(default)]
    pub min_connections: Option<u32>,
    #[serde(default)]
    pub max_connections: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DbRetryConfig {
    #[serde(default)]
    pub max_attempts: Option<u32>,
    #[serde(default)]
    pub base_delay_ms: Option<u64>,
    #[serde(default)]
    pub max_delay_ms: Option<u64>,
    #[serde(default)]
    pub jitter_percent: Option<u32>,
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PostgresTlsMode {
    Disable,
    Require,
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

                    if let Some(retry) = &rest.retry {
                        if let Some(max_attempts) = retry.max_attempts {
                            if max_attempts == 0 {
                                errors.push(ValidationError {
                                    code: "REST_RETRY_MAX_ATTEMPTS_INVALID".to_string(),
                                    path: "/driver/rest/retry/max_attempts".to_string(),
                                    message: "max_attempts must be > 0 when provided".to_string(),
                                });
                            }
                        }

                        if let Some(base_delay_ms) = retry.base_delay_ms {
                            if base_delay_ms == 0 {
                                errors.push(ValidationError {
                                    code: "REST_RETRY_BASE_DELAY_INVALID".to_string(),
                                    path: "/driver/rest/retry/base_delay_ms".to_string(),
                                    message: "base_delay_ms must be > 0 when provided".to_string(),
                                });
                            }
                        }

                        if let Some(max_delay_ms) = retry.max_delay_ms {
                            if max_delay_ms == 0 {
                                errors.push(ValidationError {
                                    code: "REST_RETRY_MAX_DELAY_INVALID".to_string(),
                                    path: "/driver/rest/retry/max_delay_ms".to_string(),
                                    message: "max_delay_ms must be > 0 when provided".to_string(),
                                });
                            }
                        }

                        if let (Some(base_delay_ms), Some(max_delay_ms)) =
                            (retry.base_delay_ms, retry.max_delay_ms)
                        {
                            if max_delay_ms < base_delay_ms {
                                errors.push(ValidationError {
                                    code: "REST_RETRY_DELAY_RANGE_INVALID".to_string(),
                                    path: "/driver/rest/retry/max_delay_ms".to_string(),
                                    message: "max_delay_ms must be >= base_delay_ms".to_string(),
                                });
                            }
                        }

                        if let Some(jitter_percent) = retry.jitter_percent {
                            if jitter_percent > 100 {
                                errors.push(ValidationError {
                                    code: "REST_RETRY_JITTER_PERCENT_INVALID".to_string(),
                                    path: "/driver/rest/retry/jitter_percent".to_string(),
                                    message: "jitter_percent must be <= 100 when provided"
                                        .to_string(),
                                });
                            }
                        }
                    }

                    if let Some(circuit_breaker) = &rest.circuit_breaker {
                        if let Some(failure_threshold) = circuit_breaker.failure_threshold {
                            if failure_threshold == 0 {
                                errors.push(ValidationError {
                                    code: "REST_CIRCUIT_BREAKER_FAILURE_THRESHOLD_INVALID"
                                        .to_string(),
                                    path: "/driver/rest/circuit_breaker/failure_threshold"
                                        .to_string(),
                                    message: "failure_threshold must be > 0 when provided"
                                        .to_string(),
                                });
                            }
                        }

                        if let Some(open_timeout_ms) = circuit_breaker.open_timeout_ms {
                            if open_timeout_ms == 0 {
                                errors.push(ValidationError {
                                    code: "REST_CIRCUIT_BREAKER_OPEN_TIMEOUT_INVALID".to_string(),
                                    path: "/driver/rest/circuit_breaker/open_timeout_ms"
                                        .to_string(),
                                    message: "open_timeout_ms must be > 0 when provided"
                                        .to_string(),
                                });
                            }
                        }
                    }

                    if let Some(auth) = &rest.auth {
                        match auth.kind {
                            RestAuthKind::ApiKey => {
                                if auth.api_key.is_none() {
                                    errors.push(ValidationError {
                                        code: "REST_AUTH_API_KEY_REQUIRED".to_string(),
                                        path: "/driver/rest/auth/api_key".to_string(),
                                        message:
                                            "api_key config is required when auth.kind is 'api_key'"
                                                .to_string(),
                                    });
                                }

                                if let Some(api_key) = &auth.api_key {
                                    if api_key.name.trim().is_empty() {
                                        errors.push(ValidationError {
                                            code: "REST_AUTH_API_KEY_NAME_EMPTY".to_string(),
                                            path: "/driver/rest/auth/api_key/name".to_string(),
                                            message: "name must be a non-empty string".to_string(),
                                        });
                                    }

                                    if api_key.value.trim().is_empty() {
                                        errors.push(ValidationError {
                                            code: "REST_AUTH_API_KEY_VALUE_EMPTY".to_string(),
                                            path: "/driver/rest/auth/api_key/value".to_string(),
                                            message: "value must be a non-empty string".to_string(),
                                        });
                                    }

                                    if api_key.location == ApiKeyLocation::Header
                                        && rest.headers.contains_key(&api_key.name)
                                    {
                                        errors.push(ValidationError {
                                            code: "REST_AUTH_API_KEY_HEADER_CONFLICT".to_string(),
                                            path: "/driver/rest/headers".to_string(),
                                            message: format!(
                                                "header '{}' conflicts with api_key auth header injection",
                                                api_key.name
                                            ),
                                        });
                                    }
                                }
                            }
                            RestAuthKind::OAuth2ClientCredentials => {
                                if auth.oauth2_client_credentials.is_none() {
                                    errors.push(ValidationError {
                                        code: "REST_AUTH_OAUTH2_REQUIRED".to_string(),
                                        path: "/driver/rest/auth/oauth2_client_credentials".to_string(),
                                        message: "oauth2_client_credentials config is required when auth.kind is 'oauth2_client_credentials'"
                                            .to_string(),
                                    });
                                }

                                if let Some(oauth2) = &auth.oauth2_client_credentials {
                                    if oauth2.token_url.trim().is_empty() {
                                        errors.push(ValidationError {
                                            code: "REST_AUTH_OAUTH2_TOKEN_URL_EMPTY".to_string(),
                                            path: "/driver/rest/auth/oauth2_client_credentials/token_url"
                                                .to_string(),
                                            message: "token_url must be a non-empty string".to_string(),
                                        });
                                    }

                                    if oauth2.client_id.trim().is_empty() {
                                        errors.push(ValidationError {
                                            code: "REST_AUTH_OAUTH2_CLIENT_ID_EMPTY".to_string(),
                                            path: "/driver/rest/auth/oauth2_client_credentials/client_id"
                                                .to_string(),
                                            message: "client_id must be a non-empty string".to_string(),
                                        });
                                    }

                                    if oauth2.client_secret.trim().is_empty() {
                                        errors.push(ValidationError {
                                            code: "REST_AUTH_OAUTH2_CLIENT_SECRET_EMPTY".to_string(),
                                            path: "/driver/rest/auth/oauth2_client_credentials/client_secret"
                                                .to_string(),
                                            message: "client_secret must be a non-empty string".to_string(),
                                        });
                                    }

                                    if rest.headers.contains_key("Authorization") {
                                        errors.push(ValidationError {
                                            code: "REST_AUTH_OAUTH2_HEADER_CONFLICT".to_string(),
                                            path: "/driver/rest/headers".to_string(),
                                            message: "header 'Authorization' conflicts with oauth2 bearer token injection"
                                                .to_string(),
                                        });
                                    }
                                }
                            }
                        }
                    }

                    if let Some(pagination) = &rest.pagination {
                        match pagination.kind {
                            RestPaginationKind::Cursor => {
                                if pagination.cursor.is_none() {
                                    errors.push(ValidationError {
                                        code: "REST_PAGINATION_CURSOR_REQUIRED".to_string(),
                                        path: "/driver/rest/pagination/cursor".to_string(),
                                        message: "cursor config is required when pagination.kind is 'cursor'"
                                            .to_string(),
                                    });
                                }

                                if let Some(cursor) = &pagination.cursor {
                                    if cursor.cursor_param.trim().is_empty() {
                                        errors.push(ValidationError {
                                            code: "REST_PAGINATION_CURSOR_PARAM_EMPTY".to_string(),
                                            path: "/driver/rest/pagination/cursor/cursor_param"
                                                .to_string(),
                                            message: "cursor_param must be a non-empty string"
                                                .to_string(),
                                        });
                                    }

                                    if let Err(message) = validate_json_pointer(&cursor.cursor_path)
                                    {
                                        errors.push(ValidationError {
                                            code: "REST_PAGINATION_CURSOR_PATH_INVALID".to_string(),
                                            path: "/driver/rest/pagination/cursor/cursor_path"
                                                .to_string(),
                                            message: message.to_string(),
                                        });
                                    }

                                    if let Some(max_pages) = cursor.max_pages {
                                        if max_pages == 0 {
                                            errors.push(ValidationError {
                                                code: "REST_PAGINATION_MAX_PAGES_INVALID"
                                                    .to_string(),
                                                path: "/driver/rest/pagination/cursor/max_pages"
                                                    .to_string(),
                                                message: "max_pages must be > 0 when provided"
                                                    .to_string(),
                                            });
                                        }
                                    }
                                }

                                if let Some(response_format) = rest.response_format {
                                    if response_format != PayloadFormat::Json
                                        && response_format != PayloadFormat::Unknown
                                    {
                                        errors.push(ValidationError {
                                            code: "REST_PAGINATION_REQUIRES_JSON_RESPONSE".to_string(),
                                            path: "/driver/rest/response_format".to_string(),
                                            message: "cursor pagination requires response_format 'json' or 'unknown'"
                                                .to_string(),
                                        });
                                    }
                                }
                            }
                            RestPaginationKind::Page => {
                                if pagination.page.is_none() {
                                    errors.push(ValidationError {
                                        code: "REST_PAGINATION_PAGE_REQUIRED".to_string(),
                                        path: "/driver/rest/pagination/page".to_string(),
                                        message:
                                            "page config is required when pagination.kind is 'page'"
                                                .to_string(),
                                    });
                                }

                                if let Some(page) = &pagination.page {
                                    if page.page_param.trim().is_empty() {
                                        errors.push(ValidationError {
                                            code: "REST_PAGINATION_PAGE_PARAM_EMPTY".to_string(),
                                            path: "/driver/rest/pagination/page/page_param"
                                                .to_string(),
                                            message: "page_param must be a non-empty string"
                                                .to_string(),
                                        });
                                    }

                                    if page.page_size_param.trim().is_empty() {
                                        errors.push(ValidationError {
                                            code: "REST_PAGINATION_PAGE_SIZE_PARAM_EMPTY"
                                                .to_string(),
                                            path: "/driver/rest/pagination/page/page_size_param"
                                                .to_string(),
                                            message: "page_size_param must be a non-empty string"
                                                .to_string(),
                                        });
                                    }

                                    if page.page_size == 0 {
                                        errors.push(ValidationError {
                                            code: "REST_PAGINATION_PAGE_SIZE_INVALID".to_string(),
                                            path: "/driver/rest/pagination/page/page_size"
                                                .to_string(),
                                            message: "page_size must be > 0".to_string(),
                                        });
                                    }

                                    if let Some(initial_page) = page.initial_page {
                                        if initial_page == 0 {
                                            errors.push(ValidationError {
                                                code: "REST_PAGINATION_INITIAL_PAGE_INVALID"
                                                    .to_string(),
                                                path: "/driver/rest/pagination/page/initial_page"
                                                    .to_string(),
                                                message: "initial_page must be > 0 when provided"
                                                    .to_string(),
                                            });
                                        }
                                    }

                                    if let Some(max_pages) = page.max_pages {
                                        if max_pages == 0 {
                                            errors.push(ValidationError {
                                                code: "REST_PAGINATION_PAGE_MAX_PAGES_INVALID"
                                                    .to_string(),
                                                path: "/driver/rest/pagination/page/max_pages"
                                                    .to_string(),
                                                message: "max_pages must be > 0 when provided"
                                                    .to_string(),
                                            });
                                        }
                                    }
                                }

                                if let Some(response_format) = rest.response_format {
                                    if response_format != PayloadFormat::Json
                                        && response_format != PayloadFormat::Unknown
                                    {
                                        errors.push(ValidationError {
                                            code: "REST_PAGINATION_REQUIRES_JSON_RESPONSE".to_string(),
                                            path: "/driver/rest/response_format".to_string(),
                                            message: "pagination requires response_format 'json' or 'unknown'"
                                                .to_string(),
                                        });
                                    }
                                }
                            }
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

                    if db.postgres_tls_mode.is_some() && db.kind != DbKind::Postgres {
                        errors.push(ValidationError {
                            code: "DB_TLS_MODE_ONLY_FOR_POSTGRES".to_string(),
                            path: "/driver/db/postgres_tls_mode".to_string(),
                            message: "postgres_tls_mode is only valid when db.kind is 'postgres'"
                                .to_string(),
                        });
                    }

                    if let Some(pool) = &db.pool {
                        if db.kind == DbKind::Sqlite {
                            errors.push(ValidationError {
                                code: "DB_POOL_UNSUPPORTED_FOR_SQLITE".to_string(),
                                path: "/driver/db/pool".to_string(),
                                message: "connection pool config is not supported for sqlite"
                                    .to_string(),
                            });
                        }

                        if let Some(min) = pool.min_connections {
                            if min == 0 {
                                errors.push(ValidationError {
                                    code: "DB_POOL_MIN_CONNECTIONS_INVALID".to_string(),
                                    path: "/driver/db/pool/min_connections".to_string(),
                                    message: "min_connections must be > 0 when provided"
                                        .to_string(),
                                });
                            }
                        }

                        if let Some(max) = pool.max_connections {
                            if max == 0 {
                                errors.push(ValidationError {
                                    code: "DB_POOL_MAX_CONNECTIONS_INVALID".to_string(),
                                    path: "/driver/db/pool/max_connections".to_string(),
                                    message: "max_connections must be > 0 when provided"
                                        .to_string(),
                                });
                            }
                        }

                        if let (Some(min), Some(max)) = (pool.min_connections, pool.max_connections)
                        {
                            if min > max {
                                errors.push(ValidationError {
                                    code: "DB_POOL_MIN_GT_MAX".to_string(),
                                    path: "/driver/db/pool".to_string(),
                                    message: "min_connections must be <= max_connections"
                                        .to_string(),
                                });
                            }
                        }
                    }

                    if let Some(retry) = &db.retry {
                        if let Some(max_attempts) = retry.max_attempts {
                            if max_attempts == 0 {
                                errors.push(ValidationError {
                                    code: "DB_RETRY_MAX_ATTEMPTS_INVALID".to_string(),
                                    path: "/driver/db/retry/max_attempts".to_string(),
                                    message: "max_attempts must be > 0 when provided".to_string(),
                                });
                            }
                        }

                        if let Some(base_delay_ms) = retry.base_delay_ms {
                            if base_delay_ms == 0 {
                                errors.push(ValidationError {
                                    code: "DB_RETRY_BASE_DELAY_INVALID".to_string(),
                                    path: "/driver/db/retry/base_delay_ms".to_string(),
                                    message: "base_delay_ms must be > 0 when provided".to_string(),
                                });
                            }
                        }

                        if let Some(max_delay_ms) = retry.max_delay_ms {
                            if max_delay_ms == 0 {
                                errors.push(ValidationError {
                                    code: "DB_RETRY_MAX_DELAY_INVALID".to_string(),
                                    path: "/driver/db/retry/max_delay_ms".to_string(),
                                    message: "max_delay_ms must be > 0 when provided".to_string(),
                                });
                            }
                        }

                        if let (Some(base_delay_ms), Some(max_delay_ms)) =
                            (retry.base_delay_ms, retry.max_delay_ms)
                        {
                            if max_delay_ms < base_delay_ms {
                                errors.push(ValidationError {
                                    code: "DB_RETRY_DELAY_RANGE_INVALID".to_string(),
                                    path: "/driver/db/retry/max_delay_ms".to_string(),
                                    message: "max_delay_ms must be >= base_delay_ms".to_string(),
                                });
                            }
                        }

                        if let Some(jitter_percent) = retry.jitter_percent {
                            if jitter_percent > 100 {
                                errors.push(ValidationError {
                                    code: "DB_RETRY_JITTER_PERCENT_INVALID".to_string(),
                                    path: "/driver/db/retry/jitter_percent".to_string(),
                                    message: "jitter_percent must be <= 100 when provided"
                                        .to_string(),
                                });
                            }
                        }
                    }

                    if let Some(circuit_breaker) = &db.circuit_breaker {
                        if let Some(failure_threshold) = circuit_breaker.failure_threshold {
                            if failure_threshold == 0 {
                                errors.push(ValidationError {
                                    code: "DB_CIRCUIT_BREAKER_FAILURE_THRESHOLD_INVALID"
                                        .to_string(),
                                    path: "/driver/db/circuit_breaker/failure_threshold"
                                        .to_string(),
                                    message: "failure_threshold must be > 0 when provided"
                                        .to_string(),
                                });
                            }
                        }

                        if let Some(open_timeout_ms) = circuit_breaker.open_timeout_ms {
                            if open_timeout_ms == 0 {
                                errors.push(ValidationError {
                                    code: "DB_CIRCUIT_BREAKER_OPEN_TIMEOUT_INVALID".to_string(),
                                    path: "/driver/db/circuit_breaker/open_timeout_ms".to_string(),
                                    message: "open_timeout_ms must be > 0 when provided"
                                        .to_string(),
                                });
                            }
                        }
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
                outcome
                    .dead_letters
                    .push(self.to_dead_letter(source, record, errors));
                continue;
            }

            let record_id = match self.build_record_id(&record.payload) {
                Ok(value) => value,
                Err(error) => {
                    outcome
                        .dead_letters
                        .push(self.to_dead_letter(source, record, vec![error]));
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

    fn to_dead_letter(
        &self,
        source: &str,
        record: ExternalRecord,
        errors: Vec<ValidationMessage>,
    ) -> DeadLetter {
        let reason_codes = dedupe_reason_codes(&errors);
        let source_type = record
            .metadata
            .source_details
            .as_ref()
            .map(|details| details.source_type.clone());
        let source_locator = record
            .metadata
            .source_details
            .as_ref()
            .and_then(|details| details.locator.clone());

        DeadLetter {
            source: source.to_string(),
            interface: self.interface.reference(),
            payload: record.payload,
            metadata: record.metadata,
            reason_codes,
            lineage: Some(DlqLineage {
                rejected_at_unix_ms: unix_ms_now(),
                pipeline_stage: "integration".to_string(),
                driver_kind: driver_kind_label(self.interface.driver.kind).to_string(),
                record_id_policy: record_id_policy_label(self.interface.record_id_policy)
                    .to_string(),
                source_type,
                source_locator,
            }),
            errors,
        }
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

fn dedupe_reason_codes(errors: &[ValidationMessage]) -> Vec<String> {
    let mut codes = Vec::new();
    for error in errors {
        if !codes.iter().any(|code| code == &error.code) {
            codes.push(error.code.clone());
        }
    }
    codes
}

fn driver_kind_label(kind: DriverKind) -> &'static str {
    match kind {
        DriverKind::Jsonl => "jsonl",
        DriverKind::Text => "text",
        DriverKind::Binary => "binary",
        DriverKind::Rest => "rest",
        DriverKind::Db => "db",
    }
}

fn record_id_policy_label(policy: RecordIdPolicy) -> &'static str {
    match policy {
        RecordIdPolicy::Strict => "strict",
        RecordIdPolicy::HashFallback => "hash_fallback",
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
    fn errors_when_api_key_auth_missing_payload() {
        let json = r#"{
            "name": "rest-auth",
            "version": "v1",
            "driver": {
                "kind": "rest",
                "rest": {
                    "url": "https://api.example.com/events",
                    "auth": {
                        "kind": "api_key"
                    }
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        let errors = interface.validate().unwrap_err();
        assert!(has_code(&errors, "REST_AUTH_API_KEY_REQUIRED"));
    }

    #[test]
    fn errors_when_api_key_auth_fields_are_empty() {
        let json = r#"{
            "name": "rest-auth",
            "version": "v1",
            "driver": {
                "kind": "rest",
                "rest": {
                    "url": "https://api.example.com/events",
                    "auth": {
                        "kind": "api_key",
                        "api_key": {
                            "in": "header",
                            "name": "",
                            "value": ""
                        }
                    }
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        let errors = interface.validate().unwrap_err();
        assert!(has_code(&errors, "REST_AUTH_API_KEY_NAME_EMPTY"));
        assert!(has_code(&errors, "REST_AUTH_API_KEY_VALUE_EMPTY"));
    }

    #[test]
    fn errors_when_api_key_header_conflicts_with_static_headers() {
        let json = r#"{
            "name": "rest-auth",
            "version": "v1",
            "driver": {
                "kind": "rest",
                "rest": {
                    "url": "https://api.example.com/events",
                    "headers": { "X-API-KEY": "legacy" },
                    "auth": {
                        "kind": "api_key",
                        "api_key": {
                            "in": "header",
                            "name": "X-API-KEY",
                            "value": "secret"
                        }
                    }
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        let errors = interface.validate().unwrap_err();
        assert!(has_code(&errors, "REST_AUTH_API_KEY_HEADER_CONFLICT"));
    }

    #[test]
    fn errors_when_oauth2_auth_missing_payload() {
        let json = r#"{
            "name": "rest-auth",
            "version": "v1",
            "driver": {
                "kind": "rest",
                "rest": {
                    "url": "https://api.example.com/events",
                    "auth": {
                        "kind": "oauth2_client_credentials"
                    }
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        let errors = interface.validate().unwrap_err();
        assert!(has_code(&errors, "REST_AUTH_OAUTH2_REQUIRED"));
    }

    #[test]
    fn errors_when_oauth2_auth_fields_are_empty() {
        let json = r#"{
            "name": "rest-auth",
            "version": "v1",
            "driver": {
                "kind": "rest",
                "rest": {
                    "url": "https://api.example.com/events",
                    "auth": {
                        "kind": "oauth2_client_credentials",
                        "oauth2_client_credentials": {
                            "token_url": "",
                            "client_id": "",
                            "client_secret": ""
                        }
                    }
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        let errors = interface.validate().unwrap_err();
        assert!(has_code(&errors, "REST_AUTH_OAUTH2_TOKEN_URL_EMPTY"));
        assert!(has_code(&errors, "REST_AUTH_OAUTH2_CLIENT_ID_EMPTY"));
        assert!(has_code(&errors, "REST_AUTH_OAUTH2_CLIENT_SECRET_EMPTY"));
    }

    #[test]
    fn errors_when_cursor_pagination_missing_cursor_config() {
        let json = r#"{
            "name": "rest-paging",
            "version": "v1",
            "driver": {
                "kind": "rest",
                "rest": {
                    "url": "https://api.example.com/events",
                    "pagination": {
                        "kind": "cursor"
                    }
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        let errors = interface.validate().unwrap_err();
        assert!(has_code(&errors, "REST_PAGINATION_CURSOR_REQUIRED"));
    }

    #[test]
    fn errors_when_cursor_pagination_has_invalid_fields() {
        let json = r#"{
            "name": "rest-paging",
            "version": "v1",
            "driver": {
                "kind": "rest",
                "rest": {
                    "url": "https://api.example.com/events",
                    "response_format": "text",
                    "pagination": {
                        "kind": "cursor",
                        "cursor": {
                            "cursor_param": "",
                            "cursor_path": "next_cursor",
                            "max_pages": 0
                        }
                    }
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        let errors = interface.validate().unwrap_err();
        assert!(has_code(&errors, "REST_PAGINATION_CURSOR_PARAM_EMPTY"));
        assert!(has_code(&errors, "REST_PAGINATION_CURSOR_PATH_INVALID"));
        assert!(has_code(&errors, "REST_PAGINATION_MAX_PAGES_INVALID"));
        assert!(has_code(&errors, "REST_PAGINATION_REQUIRES_JSON_RESPONSE"));
    }

    #[test]
    fn cursor_pagination_valid_config_passes_validation() {
        let json = r#"{
            "name": "rest-paging",
            "version": "v1",
            "driver": {
                "kind": "rest",
                "rest": {
                    "url": "https://api.example.com/events",
                    "response_format": "json",
                    "pagination": {
                        "kind": "cursor",
                        "cursor": {
                            "cursor_param": "cursor",
                            "cursor_path": "/next_cursor",
                            "max_pages": 100
                        }
                    }
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        interface.validate().unwrap();
    }

    #[test]
    fn errors_when_page_pagination_missing_page_config() {
        let json = r#"{
            "name": "rest-paging",
            "version": "v1",
            "driver": {
                "kind": "rest",
                "rest": {
                    "url": "https://api.example.com/events",
                    "pagination": {
                        "kind": "page"
                    }
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        let errors = interface.validate().unwrap_err();
        assert!(has_code(&errors, "REST_PAGINATION_PAGE_REQUIRED"));
    }

    #[test]
    fn errors_when_page_pagination_has_invalid_fields() {
        let json = r#"{
            "name": "rest-paging",
            "version": "v1",
            "driver": {
                "kind": "rest",
                "rest": {
                    "url": "https://api.example.com/events",
                    "response_format": "text",
                    "pagination": {
                        "kind": "page",
                        "page": {
                            "page_param": "",
                            "page_size_param": "",
                            "page_size": 0,
                            "initial_page": 0,
                            "max_pages": 0
                        }
                    }
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        let errors = interface.validate().unwrap_err();
        assert!(has_code(&errors, "REST_PAGINATION_PAGE_PARAM_EMPTY"));
        assert!(has_code(&errors, "REST_PAGINATION_PAGE_SIZE_PARAM_EMPTY"));
        assert!(has_code(&errors, "REST_PAGINATION_PAGE_SIZE_INVALID"));
        assert!(has_code(&errors, "REST_PAGINATION_INITIAL_PAGE_INVALID"));
        assert!(has_code(&errors, "REST_PAGINATION_PAGE_MAX_PAGES_INVALID"));
        assert!(has_code(&errors, "REST_PAGINATION_REQUIRES_JSON_RESPONSE"));
    }

    #[test]
    fn page_pagination_valid_config_passes_validation() {
        let json = r#"{
            "name": "rest-paging",
            "version": "v1",
            "driver": {
                "kind": "rest",
                "rest": {
                    "url": "https://api.example.com/events",
                    "response_format": "json",
                    "pagination": {
                        "kind": "page",
                        "page": {
                            "page_param": "page",
                            "page_size_param": "page_size",
                            "page_size": 100,
                            "initial_page": 1,
                            "max_pages": 10
                        }
                    }
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        interface.validate().unwrap();
    }

    #[test]
    fn errors_when_retry_policy_fields_are_invalid() {
        let json = r#"{
            "name": "rest-retry",
            "version": "v1",
            "driver": {
                "kind": "rest",
                "rest": {
                    "url": "https://api.example.com/events",
                    "retry": {
                        "max_attempts": 0,
                        "base_delay_ms": 0,
                        "max_delay_ms": 0,
                        "jitter_percent": 101
                    }
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        let errors = interface.validate().unwrap_err();
        assert!(has_code(&errors, "REST_RETRY_MAX_ATTEMPTS_INVALID"));
        assert!(has_code(&errors, "REST_RETRY_BASE_DELAY_INVALID"));
        assert!(has_code(&errors, "REST_RETRY_MAX_DELAY_INVALID"));
        assert!(has_code(&errors, "REST_RETRY_JITTER_PERCENT_INVALID"));
    }

    #[test]
    fn errors_when_retry_delay_range_is_invalid() {
        let json = r#"{
            "name": "rest-retry",
            "version": "v1",
            "driver": {
                "kind": "rest",
                "rest": {
                    "url": "https://api.example.com/events",
                    "retry": {
                        "max_attempts": 3,
                        "base_delay_ms": 500,
                        "max_delay_ms": 200,
                        "jitter_percent": 20
                    }
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        let errors = interface.validate().unwrap_err();
        assert!(has_code(&errors, "REST_RETRY_DELAY_RANGE_INVALID"));
    }

    #[test]
    fn retry_policy_valid_config_passes_validation() {
        let json = r#"{
            "name": "rest-retry",
            "version": "v1",
            "driver": {
                "kind": "rest",
                "rest": {
                    "url": "https://api.example.com/events",
                    "retry": {
                        "max_attempts": 3,
                        "base_delay_ms": 100,
                        "max_delay_ms": 2000,
                        "jitter_percent": 20
                    }
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        interface.validate().unwrap();
    }

    #[test]
    fn errors_when_rest_circuit_breaker_fields_are_invalid() {
        let json = r#"{
            "name": "rest-circuit",
            "version": "v1",
            "driver": {
                "kind": "rest",
                "rest": {
                    "url": "https://api.example.com/events",
                    "circuit_breaker": {
                        "failure_threshold": 0,
                        "open_timeout_ms": 0
                    }
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        let errors = interface.validate().unwrap_err();
        assert!(has_code(
            &errors,
            "REST_CIRCUIT_BREAKER_FAILURE_THRESHOLD_INVALID"
        ));
        assert!(has_code(
            &errors,
            "REST_CIRCUIT_BREAKER_OPEN_TIMEOUT_INVALID"
        ));
    }

    #[test]
    fn rest_circuit_breaker_valid_config_passes_validation() {
        let json = r#"{
            "name": "rest-circuit",
            "version": "v1",
            "driver": {
                "kind": "rest",
                "rest": {
                    "url": "https://api.example.com/events",
                    "circuit_breaker": {
                        "failure_threshold": 5,
                        "open_timeout_ms": 30000
                    }
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        interface.validate().unwrap();
    }

    #[test]
    fn errors_when_postgres_tls_mode_used_for_non_postgres_db() {
        let json = r#"{
            "name": "db-sample",
            "version": "v1",
            "driver": {
                "kind": "db",
                "db": {
                    "kind": "sqlite",
                    "connection": "./sample.db",
                    "query": "select 1",
                    "postgres_tls_mode": "require"
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        let errors = interface.validate().unwrap_err();
        assert!(has_code(&errors, "DB_TLS_MODE_ONLY_FOR_POSTGRES"));
    }

    #[test]
    fn postgres_tls_mode_require_validates_for_postgres_db() {
        let json = r#"{
            "name": "db-sample",
            "version": "v1",
            "driver": {
                "kind": "db",
                "db": {
                    "kind": "postgres",
                    "connection": "host=localhost user=app password=secret dbname=ops",
                    "query": "select 1",
                    "postgres_tls_mode": "require"
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        interface.validate().unwrap();
    }

    #[test]
    fn errors_when_db_pool_config_invalid() {
        let json = r#"{
            "name": "db-sample",
            "version": "v1",
            "driver": {
                "kind": "db",
                "db": {
                    "kind": "postgres",
                    "connection": "host=localhost user=app password=secret dbname=ops",
                    "query": "select 1",
                    "pool": {
                        "min_connections": 0,
                        "max_connections": 0
                    }
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        let errors = interface.validate().unwrap_err();
        assert!(has_code(&errors, "DB_POOL_MIN_CONNECTIONS_INVALID"));
        assert!(has_code(&errors, "DB_POOL_MAX_CONNECTIONS_INVALID"));
    }

    #[test]
    fn errors_when_db_pool_min_exceeds_max() {
        let json = r#"{
            "name": "db-sample",
            "version": "v1",
            "driver": {
                "kind": "db",
                "db": {
                    "kind": "postgres",
                    "connection": "host=localhost user=app password=secret dbname=ops",
                    "query": "select 1",
                    "pool": {
                        "min_connections": 10,
                        "max_connections": 5
                    }
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        let errors = interface.validate().unwrap_err();
        assert!(has_code(&errors, "DB_POOL_MIN_GT_MAX"));
    }

    #[test]
    fn errors_when_db_pool_used_for_sqlite() {
        let json = r#"{
            "name": "db-sample",
            "version": "v1",
            "driver": {
                "kind": "db",
                "db": {
                    "kind": "sqlite",
                    "connection": "./sample.db",
                    "query": "select 1",
                    "pool": {
                        "min_connections": 1,
                        "max_connections": 5
                    }
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        let errors = interface.validate().unwrap_err();
        assert!(has_code(&errors, "DB_POOL_UNSUPPORTED_FOR_SQLITE"));
    }

    #[test]
    fn errors_when_db_retry_policy_fields_are_invalid() {
        let json = r#"{
            "name": "db-retry",
            "version": "v1",
            "driver": {
                "kind": "db",
                "db": {
                    "kind": "postgres",
                    "connection": "host=localhost user=app password=secret dbname=ops",
                    "query": "select 1",
                    "retry": {
                        "max_attempts": 0,
                        "base_delay_ms": 0,
                        "max_delay_ms": 0,
                        "jitter_percent": 101
                    }
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        let errors = interface.validate().unwrap_err();
        assert!(has_code(&errors, "DB_RETRY_MAX_ATTEMPTS_INVALID"));
        assert!(has_code(&errors, "DB_RETRY_BASE_DELAY_INVALID"));
        assert!(has_code(&errors, "DB_RETRY_MAX_DELAY_INVALID"));
        assert!(has_code(&errors, "DB_RETRY_JITTER_PERCENT_INVALID"));
    }

    #[test]
    fn errors_when_db_retry_delay_range_is_invalid() {
        let json = r#"{
            "name": "db-retry",
            "version": "v1",
            "driver": {
                "kind": "db",
                "db": {
                    "kind": "mysql",
                    "connection": "mysql://app:secret@localhost:3306/ops",
                    "query": "select 1",
                    "retry": {
                        "max_attempts": 3,
                        "base_delay_ms": 500,
                        "max_delay_ms": 200,
                        "jitter_percent": 20
                    }
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        let errors = interface.validate().unwrap_err();
        assert!(has_code(&errors, "DB_RETRY_DELAY_RANGE_INVALID"));
    }

    #[test]
    fn db_retry_policy_valid_config_passes_validation() {
        let json = r#"{
            "name": "db-retry",
            "version": "v1",
            "driver": {
                "kind": "db",
                "db": {
                    "kind": "postgres",
                    "connection": "host=localhost user=app password=secret dbname=ops",
                    "query": "select 1",
                    "retry": {
                        "max_attempts": 3,
                        "base_delay_ms": 100,
                        "max_delay_ms": 2000,
                        "jitter_percent": 20
                    }
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        interface.validate().unwrap();
    }

    #[test]
    fn errors_when_db_circuit_breaker_fields_are_invalid() {
        let json = r#"{
            "name": "db-circuit",
            "version": "v1",
            "driver": {
                "kind": "db",
                "db": {
                    "kind": "postgres",
                    "connection": "host=localhost user=app password=secret dbname=ops",
                    "query": "select 1",
                    "circuit_breaker": {
                        "failure_threshold": 0,
                        "open_timeout_ms": 0
                    }
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        let errors = interface.validate().unwrap_err();
        assert!(has_code(
            &errors,
            "DB_CIRCUIT_BREAKER_FAILURE_THRESHOLD_INVALID"
        ));
        assert!(has_code(&errors, "DB_CIRCUIT_BREAKER_OPEN_TIMEOUT_INVALID"));
    }

    #[test]
    fn db_circuit_breaker_valid_config_passes_validation() {
        let json = r#"{
            "name": "db-circuit",
            "version": "v1",
            "driver": {
                "kind": "db",
                "db": {
                    "kind": "mysql",
                    "connection": "mysql://app:secret@localhost:3306/ops",
                    "query": "select 1",
                    "circuit_breaker": {
                        "failure_threshold": 5,
                        "open_timeout_ms": 30000
                    }
                }
            }
        }"#;

        let interface: ExternalInterface = serde_json::from_str(json).unwrap();
        interface.validate().unwrap();
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
        assert_eq!(
            output.dead_letters[0].reason_codes,
            vec!["RECORD_ID_STRICT_PATHS_UNRESOLVED".to_string()]
        );
        let lineage = output.dead_letters[0]
            .lineage
            .as_ref()
            .expect("lineage should exist on dead letters");
        assert_eq!(lineage.pipeline_stage, "integration");
        assert_eq!(lineage.driver_kind, "jsonl");
        assert_eq!(lineage.record_id_policy, "strict");
        assert!(output.dead_letters[0].errors[0]
            .message
            .contains("strict policy violation"));
    }

    #[test]
    fn dead_letter_reason_codes_are_unique() {
        let interface_json = r#"{
            "name": "mes",
            "version": "v1",
            "required_paths": ["/defect_id", "/lot_id"],
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
        assert_eq!(output.dead_letters.len(), 1);
        assert_eq!(output.dead_letters[0].reason_codes.len(), 1);
        assert_eq!(
            output.dead_letters[0].reason_codes[0],
            "MISSING_REQUIRED_PATH"
        );
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
