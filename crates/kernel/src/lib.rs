use serde::{Deserialize, Serialize};
use std::path::Path;

use rusqlite::{params, Connection, OptionalExtension};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionKind {
    ConfirmLink,
    RejectLink,
    AddEvidenceToLink,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionActor {
    pub actor_id: String,
    pub role: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfirmLinkCommand {
    pub link_id: String,
    pub justification: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RejectLinkCommand {
    pub link_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddEvidenceToLinkCommand {
    pub link_id: String,
    pub evidence_id: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActionCommand {
    ConfirmLink(ConfirmLinkCommand),
    RejectLink(RejectLinkCommand),
    AddEvidenceToLink(AddEvidenceToLinkCommand),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionRequest {
    pub actor: ActionActor,
    pub command: ActionCommand,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionResult {
    pub action_kind: ActionKind,
    pub link_id: String,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KernelError {
    Validation {
        field: &'static str,
        message: String,
    },
    Forbidden {
        role: String,
        action: ActionKind,
    },
}

pub trait ActionHandler {
    fn handle(&self, request: ActionRequest) -> Result<ActionResult, KernelError>;
}

#[derive(Debug, Clone)]
pub struct BasicActionHandler {
    policy: RolePolicy,
}

impl Default for BasicActionHandler {
    fn default() -> Self {
        Self {
            policy: RolePolicy::default(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RolePolicy;

impl RolePolicy {
    pub fn authorize(&self, role: &str, action: ActionKind) -> Result<(), KernelError> {
        if is_action_allowed(role, action) {
            return Ok(());
        }

        Err(KernelError::Forbidden {
            role: role.to_string(),
            action,
        })
    }
}

fn is_action_allowed(role: &str, action: ActionKind) -> bool {
    match role {
        "admin" => true,
        "reviewer" => matches!(action, ActionKind::ConfirmLink | ActionKind::RejectLink),
        "operator" => matches!(action, ActionKind::AddEvidenceToLink),
        _ => false,
    }
}

impl ActionHandler for BasicActionHandler {
    fn handle(&self, request: ActionRequest) -> Result<ActionResult, KernelError> {
        validate_non_empty("actor.actor_id", &request.actor.actor_id)?;

        match request.command {
            ActionCommand::ConfirmLink(command) => {
                self.policy
                    .authorize(&request.actor.role, ActionKind::ConfirmLink)?;
                validate_non_empty("command.confirm_link.link_id", &command.link_id)?;
                validate_non_empty("command.confirm_link.justification", &command.justification)?;

                Ok(ActionResult {
                    action_kind: ActionKind::ConfirmLink,
                    link_id: command.link_id.clone(),
                    summary: format!(
                        "link confirmed with justification: {}",
                        command.justification
                    ),
                })
            }
            ActionCommand::RejectLink(command) => {
                self.policy
                    .authorize(&request.actor.role, ActionKind::RejectLink)?;
                validate_non_empty("command.reject_link.link_id", &command.link_id)?;
                validate_non_empty("command.reject_link.reason", &command.reason)?;

                Ok(ActionResult {
                    action_kind: ActionKind::RejectLink,
                    link_id: command.link_id.clone(),
                    summary: format!("link rejected with reason: {}", command.reason),
                })
            }
            ActionCommand::AddEvidenceToLink(command) => {
                self.policy
                    .authorize(&request.actor.role, ActionKind::AddEvidenceToLink)?;
                validate_non_empty("command.add_evidence_to_link.link_id", &command.link_id)?;
                validate_non_empty(
                    "command.add_evidence_to_link.evidence_id",
                    &command.evidence_id,
                )?;
                validate_non_empty(
                    "command.add_evidence_to_link.description",
                    &command.description,
                )?;

                Ok(ActionResult {
                    action_kind: ActionKind::AddEvidenceToLink,
                    link_id: command.link_id.clone(),
                    summary: format!("evidence {} attached to link", command.evidence_id),
                })
            }
        }
    }
}

fn validate_non_empty(field: &'static str, value: &str) -> Result<(), KernelError> {
    if value.trim().is_empty() {
        return Err(KernelError::Validation {
            field,
            message: "must be a non-empty string".to_string(),
        });
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEvent {
    pub event_id: i64,
    pub actor_id: String,
    pub actor_role: String,
    pub action_kind: ActionKind,
    pub link_id: String,
    pub summary: String,
    pub created_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditQuery {
    pub link_id: Option<String>,
    pub limit: usize,
}

impl Default for AuditQuery {
    fn default() -> Self {
        Self {
            link_id: None,
            limit: 50,
        }
    }
}

#[derive(Debug)]
pub enum AuditError {
    Sqlite(rusqlite::Error),
    Validation(String),
}

impl std::fmt::Display for AuditError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuditError::Sqlite(error) => write!(f, "sqlite error: {}", error),
            AuditError::Validation(message) => write!(f, "validation error: {}", message),
        }
    }
}

impl std::error::Error for AuditError {}

impl From<rusqlite::Error> for AuditError {
    fn from(value: rusqlite::Error) -> Self {
        AuditError::Sqlite(value)
    }
}

pub trait AuditLogStore {
    fn append(&self, event: &AuditEvent) -> Result<(), AuditError>;
    fn query(&self, query: AuditQuery) -> Result<Vec<AuditEvent>, AuditError>;
}

#[derive(Debug, Clone)]
pub struct SqliteAuditLogStore {
    db_path: String,
}

impl SqliteAuditLogStore {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, AuditError> {
        let db_path = path.as_ref().to_string_lossy().to_string();
        let connection = Connection::open(&db_path)?;
        Self::initialize(&connection)?;
        Ok(Self { db_path })
    }

    fn connect(&self) -> Result<Connection, AuditError> {
        let connection = Connection::open(&self.db_path)?;
        Self::initialize(&connection)?;
        Ok(connection)
    }

    fn initialize(connection: &Connection) -> Result<(), AuditError> {
        connection.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS audit_events (
                event_id INTEGER PRIMARY KEY,
                actor_id TEXT NOT NULL,
                actor_role TEXT NOT NULL,
                action_kind TEXT NOT NULL,
                link_id TEXT NOT NULL,
                summary TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_audit_events_link_id
                ON audit_events (link_id, event_id DESC);
            ",
        )?;
        Ok(())
    }
}

impl AuditLogStore for SqliteAuditLogStore {
    fn append(&self, event: &AuditEvent) -> Result<(), AuditError> {
        if event.event_id <= 0 {
            return Err(AuditError::Validation(
                "event_id must be a positive integer".to_string(),
            ));
        }

        let connection = self.connect()?;
        let latest_event_id: Option<i64> = connection
            .query_row(
                "SELECT event_id FROM audit_events ORDER BY event_id DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()?;

        if let Some(latest_event_id) = latest_event_id {
            if event.event_id <= latest_event_id {
                return Err(AuditError::Validation(format!(
                    "event_id must be strictly increasing (latest={}, new={})",
                    latest_event_id, event.event_id
                )));
            }
        }

        connection.execute(
            "
            INSERT INTO audit_events (
                event_id, actor_id, actor_role, action_kind, link_id, summary, created_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ",
            params![
                event.event_id,
                event.actor_id,
                event.actor_role,
                format_action_kind(event.action_kind),
                event.link_id,
                event.summary,
                event.created_at_unix_ms,
            ],
        )?;

        Ok(())
    }

    fn query(&self, query: AuditQuery) -> Result<Vec<AuditEvent>, AuditError> {
        if query.limit == 0 {
            return Err(AuditError::Validation("limit must be > 0".to_string()));
        }

        let connection = self.connect()?;
        let limit = i64::try_from(query.limit)
            .map_err(|_| AuditError::Validation("limit is too large".to_string()))?;

        let events = if let Some(link_id) = query.link_id {
            let mut statement = connection.prepare(
                "
                SELECT event_id, actor_id, actor_role, action_kind, link_id, summary, created_at_unix_ms
                FROM audit_events
                WHERE link_id = ?1
                ORDER BY event_id DESC
                LIMIT ?2
                ",
            )?;
            let mapped = statement.query_map(params![link_id, limit], map_audit_event_row)?;
            mapped.collect::<Result<Vec<_>, _>>()?
        } else {
            let mut statement = connection.prepare(
                "
                SELECT event_id, actor_id, actor_role, action_kind, link_id, summary, created_at_unix_ms
                FROM audit_events
                ORDER BY event_id DESC
                LIMIT ?1
                ",
            )?;
            let mapped = statement.query_map(params![limit], map_audit_event_row)?;
            mapped.collect::<Result<Vec<_>, _>>()?
        };

        Ok(events)
    }
}

fn map_audit_event_row(row: &rusqlite::Row<'_>) -> Result<AuditEvent, rusqlite::Error> {
    Ok(AuditEvent {
        event_id: row.get(0)?,
        actor_id: row.get(1)?,
        actor_role: row.get(2)?,
        action_kind: parse_action_kind(&row.get::<_, String>(3)?).map_err(|message| {
            rusqlite::Error::FromSqlConversionFailure(
                3,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    message,
                )),
            )
        })?,
        link_id: row.get(4)?,
        summary: row.get(5)?,
        created_at_unix_ms: row.get(6)?,
    })
}

fn format_action_kind(action_kind: ActionKind) -> &'static str {
    match action_kind {
        ActionKind::ConfirmLink => "confirm_link",
        ActionKind::RejectLink => "reject_link",
        ActionKind::AddEvidenceToLink => "add_evidence_to_link",
    }
}

fn parse_action_kind(value: &str) -> Result<ActionKind, String> {
    match value {
        "confirm_link" => Ok(ActionKind::ConfirmLink),
        "reject_link" => Ok(ActionKind::RejectLink),
        "add_evidence_to_link" => Ok(ActionKind::AddEvidenceToLink),
        unknown => Err(format!("unknown action_kind: {}", unknown)),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{
        ActionActor, ActionCommand, ActionHandler, ActionKind, ActionRequest,
        AddEvidenceToLinkCommand, AuditEvent, AuditLogStore, AuditQuery, BasicActionHandler,
        ConfirmLinkCommand, KernelError, RejectLinkCommand, SqliteAuditLogStore,
    };

    fn actor_with_role(role: &str) -> ActionActor {
        ActionActor {
            actor_id: "reviewer-1".to_string(),
            role: role.to_string(),
        }
    }

    #[test]
    fn handles_confirm_link_command() {
        let handler = BasicActionHandler::default();
        let result = handler
            .handle(ActionRequest {
                actor: actor_with_role("reviewer"),
                command: ActionCommand::ConfirmLink(ConfirmLinkCommand {
                    link_id: "link-1".to_string(),
                    justification: "same defect id".to_string(),
                }),
            })
            .expect("confirm link should succeed");

        assert_eq!(result.action_kind, ActionKind::ConfirmLink);
        assert_eq!(result.link_id, "link-1");
        assert!(result.summary.contains("same defect id"));
    }

    #[test]
    fn handles_reject_link_command() {
        let handler = BasicActionHandler::default();
        let result = handler
            .handle(ActionRequest {
                actor: actor_with_role("reviewer"),
                command: ActionCommand::RejectLink(RejectLinkCommand {
                    link_id: "link-2".to_string(),
                    reason: "insufficient evidence".to_string(),
                }),
            })
            .expect("reject link should succeed");

        assert_eq!(result.action_kind, ActionKind::RejectLink);
        assert_eq!(result.link_id, "link-2");
        assert!(result.summary.contains("insufficient evidence"));
    }

    #[test]
    fn handles_add_evidence_command() {
        let handler = BasicActionHandler::default();
        let result = handler
            .handle(ActionRequest {
                actor: actor_with_role("operator"),
                command: ActionCommand::AddEvidenceToLink(AddEvidenceToLinkCommand {
                    link_id: "link-3".to_string(),
                    evidence_id: "evidence-9".to_string(),
                    description: "attached photo".to_string(),
                }),
            })
            .expect("add evidence should succeed");

        assert_eq!(result.action_kind, ActionKind::AddEvidenceToLink);
        assert_eq!(result.link_id, "link-3");
        assert!(result.summary.contains("evidence-9"));
    }

    #[test]
    fn rejects_empty_actor_id() {
        let handler = BasicActionHandler::default();
        let error = handler
            .handle(ActionRequest {
                actor: ActionActor {
                    actor_id: String::new(),
                    role: "reviewer".to_string(),
                },
                command: ActionCommand::ConfirmLink(ConfirmLinkCommand {
                    link_id: "link-1".to_string(),
                    justification: "valid".to_string(),
                }),
            })
            .expect_err("empty actor_id should fail");

        assert_eq!(
            error,
            KernelError::Validation {
                field: "actor.actor_id",
                message: "must be a non-empty string".to_string(),
            }
        );
    }

    #[test]
    fn rejects_empty_confirm_justification() {
        let handler = BasicActionHandler::default();
        let error = handler
            .handle(ActionRequest {
                actor: actor_with_role("reviewer"),
                command: ActionCommand::ConfirmLink(ConfirmLinkCommand {
                    link_id: "link-1".to_string(),
                    justification: "  ".to_string(),
                }),
            })
            .expect_err("empty confirmation justification should fail");

        assert_eq!(
            error,
            KernelError::Validation {
                field: "command.confirm_link.justification",
                message: "must be a non-empty string".to_string(),
            }
        );
    }

    #[test]
    fn rejects_empty_reject_reason() {
        let handler = BasicActionHandler::default();
        let error = handler
            .handle(ActionRequest {
                actor: actor_with_role("reviewer"),
                command: ActionCommand::RejectLink(RejectLinkCommand {
                    link_id: "link-2".to_string(),
                    reason: String::new(),
                }),
            })
            .expect_err("empty reject reason should fail");

        assert_eq!(
            error,
            KernelError::Validation {
                field: "command.reject_link.reason",
                message: "must be a non-empty string".to_string(),
            }
        );
    }

    #[test]
    fn rejects_empty_evidence_description() {
        let handler = BasicActionHandler::default();
        let error = handler
            .handle(ActionRequest {
                actor: actor_with_role("operator"),
                command: ActionCommand::AddEvidenceToLink(AddEvidenceToLinkCommand {
                    link_id: "link-3".to_string(),
                    evidence_id: "evidence-9".to_string(),
                    description: " ".to_string(),
                }),
            })
            .expect_err("empty evidence description should fail");

        assert_eq!(
            error,
            KernelError::Validation {
                field: "command.add_evidence_to_link.description",
                message: "must be a non-empty string".to_string(),
            }
        );
    }

    #[test]
    fn rejects_forbidden_action_by_role_policy() {
        let handler = BasicActionHandler::default();
        let error = handler
            .handle(ActionRequest {
                actor: actor_with_role("reviewer"),
                command: ActionCommand::AddEvidenceToLink(AddEvidenceToLinkCommand {
                    link_id: "link-3".to_string(),
                    evidence_id: "evidence-9".to_string(),
                    description: "attached photo".to_string(),
                }),
            })
            .expect_err("reviewer should not be allowed to add evidence");

        assert_eq!(
            error,
            KernelError::Forbidden {
                role: "reviewer".to_string(),
                action: ActionKind::AddEvidenceToLink,
            }
        );
    }

    #[test]
    fn admin_is_allowed_for_all_actions() {
        let handler = BasicActionHandler::default();

        let confirm = handler.handle(ActionRequest {
            actor: actor_with_role("admin"),
            command: ActionCommand::ConfirmLink(ConfirmLinkCommand {
                link_id: "link-1".to_string(),
                justification: "approved".to_string(),
            }),
        });
        assert!(confirm.is_ok());

        let reject = handler.handle(ActionRequest {
            actor: actor_with_role("admin"),
            command: ActionCommand::RejectLink(RejectLinkCommand {
                link_id: "link-2".to_string(),
                reason: "invalid".to_string(),
            }),
        });
        assert!(reject.is_ok());

        let add = handler.handle(ActionRequest {
            actor: actor_with_role("admin"),
            command: ActionCommand::AddEvidenceToLink(AddEvidenceToLinkCommand {
                link_id: "link-3".to_string(),
                evidence_id: "e-1".to_string(),
                description: "evidence".to_string(),
            }),
        });
        assert!(add.is_ok());
    }

    #[test]
    fn sqlite_audit_store_appends_and_queries_by_link() {
        let db_path = temp_audit_db_path("query_by_link");
        let store = SqliteAuditLogStore::new(&db_path).expect("store should initialize");

        store
            .append(&AuditEvent {
                event_id: 1,
                actor_id: "reviewer-1".to_string(),
                actor_role: "reviewer".to_string(),
                action_kind: ActionKind::ConfirmLink,
                link_id: "link-a".to_string(),
                summary: "confirmed".to_string(),
                created_at_unix_ms: 1_706_000_000_001,
            })
            .expect("append event 1");
        store
            .append(&AuditEvent {
                event_id: 2,
                actor_id: "operator-1".to_string(),
                actor_role: "operator".to_string(),
                action_kind: ActionKind::AddEvidenceToLink,
                link_id: "link-b".to_string(),
                summary: "evidence attached".to_string(),
                created_at_unix_ms: 1_706_000_000_002,
            })
            .expect("append event 2");
        store
            .append(&AuditEvent {
                event_id: 3,
                actor_id: "reviewer-2".to_string(),
                actor_role: "reviewer".to_string(),
                action_kind: ActionKind::RejectLink,
                link_id: "link-a".to_string(),
                summary: "rejected".to_string(),
                created_at_unix_ms: 1_706_000_000_003,
            })
            .expect("append event 3");

        let events = store
            .query(AuditQuery {
                link_id: Some("link-a".to_string()),
                limit: 10,
            })
            .expect("query should succeed");

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_id, 3);
        assert_eq!(events[1].event_id, 1);
    }

    #[test]
    fn sqlite_audit_store_rejects_non_increasing_event_id() {
        let db_path = temp_audit_db_path("non_increasing");
        let store = SqliteAuditLogStore::new(&db_path).expect("store should initialize");

        store
            .append(&AuditEvent {
                event_id: 10,
                actor_id: "reviewer-1".to_string(),
                actor_role: "reviewer".to_string(),
                action_kind: ActionKind::ConfirmLink,
                link_id: "link-a".to_string(),
                summary: "confirmed".to_string(),
                created_at_unix_ms: 1_706_000_000_001,
            })
            .expect("append event 10");

        let duplicate = store.append(&AuditEvent {
            event_id: 10,
            actor_id: "reviewer-2".to_string(),
            actor_role: "reviewer".to_string(),
            action_kind: ActionKind::RejectLink,
            link_id: "link-a".to_string(),
            summary: "rejected".to_string(),
            created_at_unix_ms: 1_706_000_000_010,
        });

        assert!(duplicate.is_err());
    }

    #[test]
    fn sqlite_audit_store_applies_limit_descending() {
        let db_path = temp_audit_db_path("limit_descending");
        let store = SqliteAuditLogStore::new(&db_path).expect("store should initialize");

        for event_id in 1..=5 {
            store
                .append(&AuditEvent {
                    event_id,
                    actor_id: "reviewer-1".to_string(),
                    actor_role: "reviewer".to_string(),
                    action_kind: ActionKind::ConfirmLink,
                    link_id: "link-a".to_string(),
                    summary: "confirmed".to_string(),
                    created_at_unix_ms: 1_706_000_000_000 + event_id,
                })
                .expect("append event");
        }

        let events = store
            .query(AuditQuery {
                link_id: None,
                limit: 2,
            })
            .expect("query should succeed");

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_id, 5);
        assert_eq!(events[1].event_id, 4);
    }

    fn temp_audit_db_path(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should move forward")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "rootsys-kernel-audit-{}-{}-{}.db",
            test_name,
            std::process::id(),
            nanos
        ))
    }
}
