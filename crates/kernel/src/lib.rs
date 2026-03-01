use serde::{Deserialize, Serialize};

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
}

pub trait ActionHandler {
    fn handle(&self, request: ActionRequest) -> Result<ActionResult, KernelError>;
}

#[derive(Debug, Clone, Default)]
pub struct BasicActionHandler;

impl ActionHandler for BasicActionHandler {
    fn handle(&self, request: ActionRequest) -> Result<ActionResult, KernelError> {
        if request.actor.actor_id.trim().is_empty() {
            return Err(KernelError::Validation {
                field: "actor.actor_id",
                message: "must be a non-empty string".to_string(),
            });
        }

        match request.command {
            ActionCommand::ConfirmLink(command) => {
                if command.link_id.trim().is_empty() {
                    return Err(KernelError::Validation {
                        field: "command.confirm_link.link_id",
                        message: "must be a non-empty string".to_string(),
                    });
                }
                if command.justification.trim().is_empty() {
                    return Err(KernelError::Validation {
                        field: "command.confirm_link.justification",
                        message: "must be a non-empty string".to_string(),
                    });
                }

                Ok(ActionResult {
                    action_kind: ActionKind::ConfirmLink,
                    link_id: command.link_id,
                    summary: "link confirmed".to_string(),
                })
            }
            ActionCommand::RejectLink(command) => {
                if command.link_id.trim().is_empty() {
                    return Err(KernelError::Validation {
                        field: "command.reject_link.link_id",
                        message: "must be a non-empty string".to_string(),
                    });
                }
                if command.reason.trim().is_empty() {
                    return Err(KernelError::Validation {
                        field: "command.reject_link.reason",
                        message: "must be a non-empty string".to_string(),
                    });
                }

                Ok(ActionResult {
                    action_kind: ActionKind::RejectLink,
                    link_id: command.link_id,
                    summary: "link rejected".to_string(),
                })
            }
            ActionCommand::AddEvidenceToLink(command) => {
                if command.link_id.trim().is_empty() {
                    return Err(KernelError::Validation {
                        field: "command.add_evidence_to_link.link_id",
                        message: "must be a non-empty string".to_string(),
                    });
                }
                if command.evidence_id.trim().is_empty() {
                    return Err(KernelError::Validation {
                        field: "command.add_evidence_to_link.evidence_id",
                        message: "must be a non-empty string".to_string(),
                    });
                }

                Ok(ActionResult {
                    action_kind: ActionKind::AddEvidenceToLink,
                    link_id: command.link_id,
                    summary: "evidence attached to link".to_string(),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ActionActor, ActionCommand, ActionHandler, ActionKind, ActionRequest,
        AddEvidenceToLinkCommand, BasicActionHandler, ConfirmLinkCommand, KernelError,
        RejectLinkCommand,
    };

    fn actor() -> ActionActor {
        ActionActor {
            actor_id: "reviewer-1".to_string(),
            role: "reviewer".to_string(),
        }
    }

    #[test]
    fn handles_confirm_link_command() {
        let handler = BasicActionHandler;
        let result = handler
            .handle(ActionRequest {
                actor: actor(),
                command: ActionCommand::ConfirmLink(ConfirmLinkCommand {
                    link_id: "link-1".to_string(),
                    justification: "same defect id".to_string(),
                }),
            })
            .expect("confirm link should succeed");

        assert_eq!(result.action_kind, ActionKind::ConfirmLink);
        assert_eq!(result.link_id, "link-1");
    }

    #[test]
    fn handles_reject_link_command() {
        let handler = BasicActionHandler;
        let result = handler
            .handle(ActionRequest {
                actor: actor(),
                command: ActionCommand::RejectLink(RejectLinkCommand {
                    link_id: "link-2".to_string(),
                    reason: "insufficient evidence".to_string(),
                }),
            })
            .expect("reject link should succeed");

        assert_eq!(result.action_kind, ActionKind::RejectLink);
        assert_eq!(result.link_id, "link-2");
    }

    #[test]
    fn handles_add_evidence_command() {
        let handler = BasicActionHandler;
        let result = handler
            .handle(ActionRequest {
                actor: actor(),
                command: ActionCommand::AddEvidenceToLink(AddEvidenceToLinkCommand {
                    link_id: "link-3".to_string(),
                    evidence_id: "evidence-9".to_string(),
                    description: "attached photo".to_string(),
                }),
            })
            .expect("add evidence should succeed");

        assert_eq!(result.action_kind, ActionKind::AddEvidenceToLink);
        assert_eq!(result.link_id, "link-3");
    }

    #[test]
    fn rejects_empty_actor_id() {
        let handler = BasicActionHandler;
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
}
