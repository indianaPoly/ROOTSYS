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

#[cfg(test)]
mod tests {
    use super::{
        ActionActor, ActionCommand, ActionHandler, ActionKind, ActionRequest,
        AddEvidenceToLinkCommand, BasicActionHandler, ConfirmLinkCommand, KernelError,
        RejectLinkCommand,
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
}
