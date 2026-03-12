use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{Arc, Mutex};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateLinkState {
    Candidate,
    InReview,
    Confirmed,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateStateTransition {
    pub link_id: String,
    pub action_kind: ActionKind,
    pub from_state: CandidateLinkState,
    pub to_state: CandidateLinkState,
    pub actor_id: String,
    pub actor_role: String,
    pub transitioned_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CandidateLifecycleError {
    InvalidTransition {
        action: ActionKind,
        from: CandidateLinkState,
    },
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CandidateLifecycleMachine;

impl CandidateLifecycleMachine {
    pub fn next_state(
        &self,
        current: CandidateLinkState,
        action: ActionKind,
    ) -> Result<CandidateLinkState, CandidateLifecycleError> {
        match action {
            ActionKind::ConfirmLink => match current {
                CandidateLinkState::Candidate | CandidateLinkState::InReview => {
                    Ok(CandidateLinkState::Confirmed)
                }
                CandidateLinkState::Confirmed | CandidateLinkState::Rejected => {
                    Err(CandidateLifecycleError::InvalidTransition {
                        action,
                        from: current,
                    })
                }
            },
            ActionKind::RejectLink => match current {
                CandidateLinkState::Candidate | CandidateLinkState::InReview => {
                    Ok(CandidateLinkState::Rejected)
                }
                CandidateLinkState::Confirmed | CandidateLinkState::Rejected => {
                    Err(CandidateLifecycleError::InvalidTransition {
                        action,
                        from: current,
                    })
                }
            },
            ActionKind::AddEvidenceToLink => match current {
                CandidateLinkState::Candidate | CandidateLinkState::InReview => {
                    Ok(CandidateLinkState::InReview)
                }
                CandidateLinkState::Confirmed | CandidateLinkState::Rejected => {
                    Err(CandidateLifecycleError::InvalidTransition {
                        action,
                        from: current,
                    })
                }
            },
        }
    }
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

#[derive(Debug)]
pub enum CandidateStateStoreError {
    Sqlite(rusqlite::Error),
    Validation(String),
    Poisoned(String),
}

impl std::fmt::Display for CandidateStateStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CandidateStateStoreError::Sqlite(error) => write!(f, "sqlite error: {}", error),
            CandidateStateStoreError::Validation(message) => {
                write!(f, "validation error: {}", message)
            }
            CandidateStateStoreError::Poisoned(message) => {
                write!(f, "mutex poisoned: {}", message)
            }
        }
    }
}

impl std::error::Error for CandidateStateStoreError {}

impl From<rusqlite::Error> for CandidateStateStoreError {
    fn from(value: rusqlite::Error) -> Self {
        CandidateStateStoreError::Sqlite(value)
    }
}

pub trait CandidateStateStore {
    fn get_state(&self, link_id: &str) -> Result<CandidateLinkState, CandidateStateStoreError>;
    fn set_state(
        &self,
        link_id: &str,
        state: CandidateLinkState,
        updated_at_unix_ms: i64,
    ) -> Result<(), CandidateStateStoreError>;
    fn append_transition(
        &self,
        transition: &CandidateStateTransition,
    ) -> Result<(), CandidateStateStoreError>;
    fn query_transitions(
        &self,
        link_id: &str,
        limit: usize,
    ) -> Result<Vec<CandidateStateTransition>, CandidateStateStoreError>;
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryCandidateStateStore {
    states: Arc<Mutex<std::collections::BTreeMap<String, CandidateLinkState>>>,
    transitions: Arc<Mutex<Vec<CandidateStateTransition>>>,
}

impl CandidateStateStore for InMemoryCandidateStateStore {
    fn get_state(&self, link_id: &str) -> Result<CandidateLinkState, CandidateStateStoreError> {
        let states = self
            .states
            .lock()
            .map_err(|error| CandidateStateStoreError::Poisoned(error.to_string()))?;
        Ok(*states
            .get(link_id)
            .unwrap_or(&CandidateLinkState::Candidate))
    }

    fn set_state(
        &self,
        link_id: &str,
        state: CandidateLinkState,
        _updated_at_unix_ms: i64,
    ) -> Result<(), CandidateStateStoreError> {
        if link_id.trim().is_empty() {
            return Err(CandidateStateStoreError::Validation(
                "link_id must be a non-empty string".to_string(),
            ));
        }

        let mut states = self
            .states
            .lock()
            .map_err(|error| CandidateStateStoreError::Poisoned(error.to_string()))?;
        states.insert(link_id.to_string(), state);
        Ok(())
    }

    fn append_transition(
        &self,
        transition: &CandidateStateTransition,
    ) -> Result<(), CandidateStateStoreError> {
        let mut transitions = self
            .transitions
            .lock()
            .map_err(|error| CandidateStateStoreError::Poisoned(error.to_string()))?;
        transitions.push(transition.clone());
        Ok(())
    }

    fn query_transitions(
        &self,
        link_id: &str,
        limit: usize,
    ) -> Result<Vec<CandidateStateTransition>, CandidateStateStoreError> {
        if limit == 0 {
            return Err(CandidateStateStoreError::Validation(
                "limit must be > 0".to_string(),
            ));
        }

        let transitions = self
            .transitions
            .lock()
            .map_err(|error| CandidateStateStoreError::Poisoned(error.to_string()))?;

        let mut result = transitions
            .iter()
            .rev()
            .filter(|transition| transition.link_id == link_id)
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();
        result.shrink_to_fit();
        Ok(result)
    }
}

#[derive(Debug, Clone)]
pub struct SqliteCandidateStateStore {
    db_path: String,
}

impl SqliteCandidateStateStore {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, CandidateStateStoreError> {
        let db_path = path.as_ref().to_string_lossy().to_string();
        let connection = Connection::open(&db_path)?;
        Self::initialize(&connection)?;
        Ok(Self { db_path })
    }

    fn connect(&self) -> Result<Connection, CandidateStateStoreError> {
        let connection = Connection::open(&self.db_path)?;
        Self::initialize(&connection)?;
        Ok(connection)
    }

    fn initialize(connection: &Connection) -> Result<(), CandidateStateStoreError> {
        connection.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS candidate_link_state (
                link_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS candidate_state_transitions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                link_id TEXT NOT NULL,
                action_kind TEXT NOT NULL,
                from_state TEXT NOT NULL,
                to_state TEXT NOT NULL,
                actor_id TEXT NOT NULL,
                actor_role TEXT NOT NULL,
                transitioned_at_unix_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_candidate_state_transitions_link_id
                ON candidate_state_transitions (link_id, id DESC);
            ",
        )?;
        Ok(())
    }
}

impl CandidateStateStore for SqliteCandidateStateStore {
    fn get_state(&self, link_id: &str) -> Result<CandidateLinkState, CandidateStateStoreError> {
        if link_id.trim().is_empty() {
            return Err(CandidateStateStoreError::Validation(
                "link_id must be a non-empty string".to_string(),
            ));
        }

        let connection = self.connect()?;
        let value: Option<String> = connection
            .query_row(
                "SELECT state FROM candidate_link_state WHERE link_id = ?1",
                params![link_id],
                |row| row.get(0),
            )
            .optional()?;

        if let Some(value) = value {
            parse_candidate_state(&value)
                .map_err(|message| CandidateStateStoreError::Validation(message.to_string()))
        } else {
            Ok(CandidateLinkState::Candidate)
        }
    }

    fn set_state(
        &self,
        link_id: &str,
        state: CandidateLinkState,
        updated_at_unix_ms: i64,
    ) -> Result<(), CandidateStateStoreError> {
        if link_id.trim().is_empty() {
            return Err(CandidateStateStoreError::Validation(
                "link_id must be a non-empty string".to_string(),
            ));
        }

        let connection = self.connect()?;
        connection.execute(
            "
            INSERT INTO candidate_link_state (link_id, state, updated_at_unix_ms)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(link_id)
            DO UPDATE SET state = excluded.state, updated_at_unix_ms = excluded.updated_at_unix_ms
            ",
            params![link_id, format_candidate_state(state), updated_at_unix_ms],
        )?;

        Ok(())
    }

    fn append_transition(
        &self,
        transition: &CandidateStateTransition,
    ) -> Result<(), CandidateStateStoreError> {
        let connection = self.connect()?;
        connection.execute(
            "
            INSERT INTO candidate_state_transitions (
                link_id,
                action_kind,
                from_state,
                to_state,
                actor_id,
                actor_role,
                transitioned_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ",
            params![
                transition.link_id,
                format_action_kind(transition.action_kind),
                format_candidate_state(transition.from_state),
                format_candidate_state(transition.to_state),
                transition.actor_id,
                transition.actor_role,
                transition.transitioned_at_unix_ms,
            ],
        )?;
        Ok(())
    }

    fn query_transitions(
        &self,
        link_id: &str,
        limit: usize,
    ) -> Result<Vec<CandidateStateTransition>, CandidateStateStoreError> {
        if link_id.trim().is_empty() {
            return Err(CandidateStateStoreError::Validation(
                "link_id must be a non-empty string".to_string(),
            ));
        }
        if limit == 0 {
            return Err(CandidateStateStoreError::Validation(
                "limit must be > 0".to_string(),
            ));
        }

        let connection = self.connect()?;
        let limit = i64::try_from(limit)
            .map_err(|_| CandidateStateStoreError::Validation("limit is too large".to_string()))?;

        let mut statement = connection.prepare(
            "
            SELECT link_id, action_kind, from_state, to_state, actor_id, actor_role, transitioned_at_unix_ms
            FROM candidate_state_transitions
            WHERE link_id = ?1
            ORDER BY id DESC
            LIMIT ?2
            ",
        )?;

        let mapped = statement.query_map(params![link_id, limit], |row| {
            let action_kind_value: String = row.get(1)?;
            let from_state_value: String = row.get(2)?;
            let to_state_value: String = row.get(3)?;

            let action_kind = parse_action_kind(&action_kind_value).map_err(|message| {
                rusqlite::Error::FromSqlConversionFailure(
                    1,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        message,
                    )),
                )
            })?;

            let from_state = parse_candidate_state(&from_state_value).map_err(|message| {
                rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        message,
                    )),
                )
            })?;

            let to_state = parse_candidate_state(&to_state_value).map_err(|message| {
                rusqlite::Error::FromSqlConversionFailure(
                    3,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        message,
                    )),
                )
            })?;

            Ok(CandidateStateTransition {
                link_id: row.get(0)?,
                action_kind,
                from_state,
                to_state,
                actor_id: row.get(4)?,
                actor_role: row.get(5)?,
                transitioned_at_unix_ms: row.get(6)?,
            })
        })?;

        Ok(mapped.collect::<Result<Vec<_>, _>>()?)
    }
}

pub trait AuditLogStore {
    fn append(&self, event: &AuditEvent) -> Result<(), AuditError>;
    fn query(&self, query: AuditQuery) -> Result<Vec<AuditEvent>, AuditError>;
}

#[derive(Debug)]
pub enum ActionApiError {
    Kernel(KernelError),
    Audit(AuditError),
    CandidateState(CandidateStateStoreError),
    Lifecycle(CandidateLifecycleError),
    Validation(String),
}

impl std::fmt::Display for ActionApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ActionApiError::Kernel(error) => write!(f, "kernel error: {:?}", error),
            ActionApiError::Audit(error) => write!(f, "audit error: {}", error),
            ActionApiError::CandidateState(error) => {
                write!(f, "candidate-state error: {}", error)
            }
            ActionApiError::Lifecycle(error) => write!(f, "lifecycle error: {:?}", error),
            ActionApiError::Validation(message) => write!(f, "validation error: {}", message),
        }
    }
}

impl std::error::Error for ActionApiError {}

impl From<KernelError> for ActionApiError {
    fn from(value: KernelError) -> Self {
        ActionApiError::Kernel(value)
    }
}

impl From<AuditError> for ActionApiError {
    fn from(value: AuditError) -> Self {
        ActionApiError::Audit(value)
    }
}

impl From<CandidateStateStoreError> for ActionApiError {
    fn from(value: CandidateStateStoreError) -> Self {
        ActionApiError::CandidateState(value)
    }
}

impl From<CandidateLifecycleError> for ActionApiError {
    fn from(value: CandidateLifecycleError) -> Self {
        ActionApiError::Lifecycle(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionExecutionResponse {
    pub result: ActionResult,
    pub audit_event_id: i64,
    pub previous_state: CandidateLinkState,
    pub current_state: CandidateLinkState,
}

#[derive(Debug, Clone)]
pub struct ActionAuditApiService<H, A, S>
where
    H: ActionHandler,
    A: AuditLogStore,
    S: CandidateStateStore,
{
    handler: H,
    audit_store: A,
    candidate_state_store: S,
    lifecycle: CandidateLifecycleMachine,
}

impl<H, A, S> ActionAuditApiService<H, A, S>
where
    H: ActionHandler,
    A: AuditLogStore,
    S: CandidateStateStore,
{
    pub fn new(handler: H, audit_store: A, candidate_state_store: S) -> Self {
        Self {
            handler,
            audit_store,
            candidate_state_store,
            lifecycle: CandidateLifecycleMachine,
        }
    }

    pub fn execute(
        &self,
        request: ActionRequest,
        created_at_unix_ms: i64,
    ) -> Result<ActionExecutionResponse, ActionApiError> {
        let link_id = command_link_id(&request.command)
            .ok_or_else(|| ActionApiError::Validation("link_id must be provided".to_string()))?
            .to_string();

        let previous_state = self.candidate_state_store.get_state(&link_id)?;
        let result = self.handler.handle(request.clone())?;
        let current_state = self
            .lifecycle
            .next_state(previous_state, result.action_kind)?;

        self.candidate_state_store
            .set_state(&link_id, current_state, created_at_unix_ms)?;
        self.candidate_state_store
            .append_transition(&CandidateStateTransition {
                link_id: link_id.clone(),
                action_kind: result.action_kind,
                from_state: previous_state,
                to_state: current_state,
                actor_id: request.actor.actor_id.clone(),
                actor_role: request.actor.role.clone(),
                transitioned_at_unix_ms: created_at_unix_ms,
            })?;

        let next_audit_event_id = self.next_audit_event_id()?;
        self.audit_store.append(&AuditEvent {
            event_id: next_audit_event_id,
            actor_id: request.actor.actor_id,
            actor_role: request.actor.role,
            action_kind: result.action_kind,
            link_id: result.link_id.clone(),
            summary: result.summary.clone(),
            created_at_unix_ms,
        })?;

        Ok(ActionExecutionResponse {
            result,
            audit_event_id: next_audit_event_id,
            previous_state,
            current_state,
        })
    }

    pub fn query_audit(&self, query: AuditQuery) -> Result<Vec<AuditEvent>, ActionApiError> {
        Ok(self.audit_store.query(query)?)
    }

    pub fn query_candidate_history(
        &self,
        link_id: &str,
        limit: usize,
    ) -> Result<Vec<CandidateStateTransition>, ActionApiError> {
        Ok(self
            .candidate_state_store
            .query_transitions(link_id, limit)?)
    }

    pub fn query_candidate_state(
        &self,
        link_id: &str,
    ) -> Result<CandidateLinkState, ActionApiError> {
        Ok(self.candidate_state_store.get_state(link_id)?)
    }

    fn next_audit_event_id(&self) -> Result<i64, ActionApiError> {
        let latest = self.audit_store.query(AuditQuery {
            link_id: None,
            limit: 1,
        })?;
        Ok(latest.first().map(|event| event.event_id + 1).unwrap_or(1))
    }
}

fn command_link_id(command: &ActionCommand) -> Option<&str> {
    match command {
        ActionCommand::ConfirmLink(command) => Some(command.link_id.as_str()),
        ActionCommand::RejectLink(command) => Some(command.link_id.as_str()),
        ActionCommand::AddEvidenceToLink(command) => Some(command.link_id.as_str()),
    }
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

fn format_candidate_state(state: CandidateLinkState) -> &'static str {
    match state {
        CandidateLinkState::Candidate => "candidate",
        CandidateLinkState::InReview => "in_review",
        CandidateLinkState::Confirmed => "confirmed",
        CandidateLinkState::Rejected => "rejected",
    }
}

fn parse_candidate_state(value: &str) -> Result<CandidateLinkState, String> {
    match value {
        "candidate" => Ok(CandidateLinkState::Candidate),
        "in_review" => Ok(CandidateLinkState::InReview),
        "confirmed" => Ok(CandidateLinkState::Confirmed),
        "rejected" => Ok(CandidateLinkState::Rejected),
        unknown => Err(format!("unknown candidate_state: {}", unknown)),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{
        ActionActor, ActionAuditApiService, ActionCommand, ActionHandler, ActionKind,
        ActionRequest, AddEvidenceToLinkCommand, AuditEvent, AuditLogStore, AuditQuery,
        BasicActionHandler, CandidateLifecycleError, CandidateLifecycleMachine, CandidateLinkState,
        CandidateStateStore, ConfirmLinkCommand, InMemoryCandidateStateStore, KernelError,
        RejectLinkCommand, SqliteAuditLogStore, SqliteCandidateStateStore,
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
    fn lifecycle_machine_transitions_candidate_to_confirmed() {
        let machine = CandidateLifecycleMachine;
        let next = machine
            .next_state(CandidateLinkState::Candidate, ActionKind::ConfirmLink)
            .expect("candidate should transition to confirmed on confirm action");
        assert_eq!(next, CandidateLinkState::Confirmed);
    }

    #[test]
    fn lifecycle_machine_rejects_add_evidence_after_confirmation() {
        let machine = CandidateLifecycleMachine;
        let error = machine
            .next_state(CandidateLinkState::Confirmed, ActionKind::AddEvidenceToLink)
            .expect_err("confirmed candidate should reject add-evidence transition");

        assert_eq!(
            error,
            CandidateLifecycleError::InvalidTransition {
                action: ActionKind::AddEvidenceToLink,
                from: CandidateLinkState::Confirmed,
            }
        );
    }

    #[test]
    fn action_api_service_executes_action_and_records_state_and_audit() {
        let handler = BasicActionHandler::default();
        let audit_db_path = temp_audit_db_path("action_api");
        let audit_store = SqliteAuditLogStore::new(&audit_db_path).expect("audit store init");
        let candidate_store = InMemoryCandidateStateStore::default();
        let service = ActionAuditApiService::new(handler, audit_store.clone(), candidate_store);

        let response = service
            .execute(
                ActionRequest {
                    actor: actor_with_role("reviewer"),
                    command: ActionCommand::ConfirmLink(ConfirmLinkCommand {
                        link_id: "link-api-1".to_string(),
                        justification: "validated by expert".to_string(),
                    }),
                },
                1_706_000_111_000,
            )
            .expect("action execution should succeed");

        assert_eq!(response.previous_state, CandidateLinkState::Candidate);
        assert_eq!(response.current_state, CandidateLinkState::Confirmed);
        assert_eq!(response.result.action_kind, ActionKind::ConfirmLink);
        assert_eq!(response.audit_event_id, 1);

        let audit_events = service
            .query_audit(AuditQuery {
                link_id: Some("link-api-1".to_string()),
                limit: 5,
            })
            .expect("audit query should succeed");
        assert_eq!(audit_events.len(), 1);
        assert_eq!(audit_events[0].event_id, 1);

        let history = service
            .query_candidate_history("link-api-1", 5)
            .expect("candidate history query should succeed");
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].from_state, CandidateLinkState::Candidate);
        assert_eq!(history[0].to_state, CandidateLinkState::Confirmed);

        let state = service
            .query_candidate_state("link-api-1")
            .expect("candidate state query should succeed");
        assert_eq!(state, CandidateLinkState::Confirmed);
    }

    #[test]
    fn sqlite_candidate_state_store_persists_state_and_history() {
        let db_path = temp_audit_db_path("candidate_store");
        let store = SqliteCandidateStateStore::new(&db_path).expect("candidate store init");

        store
            .set_state(
                "link-store-1",
                CandidateLinkState::InReview,
                1_706_000_222_000,
            )
            .expect("set state should succeed");
        store
            .append_transition(&super::CandidateStateTransition {
                link_id: "link-store-1".to_string(),
                action_kind: ActionKind::AddEvidenceToLink,
                from_state: CandidateLinkState::Candidate,
                to_state: CandidateLinkState::InReview,
                actor_id: "operator-1".to_string(),
                actor_role: "operator".to_string(),
                transitioned_at_unix_ms: 1_706_000_222_000,
            })
            .expect("append transition should succeed");

        let state = store
            .get_state("link-store-1")
            .expect("get state should succeed");
        assert_eq!(state, CandidateLinkState::InReview);

        let history = store
            .query_transitions("link-store-1", 10)
            .expect("query transitions should succeed");
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].to_state, CandidateLinkState::InReview);
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
