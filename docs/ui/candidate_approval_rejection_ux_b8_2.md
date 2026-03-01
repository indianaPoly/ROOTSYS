# Candidate Approval/Rejection UX with Justification Capture (B8-2)

This document specifies the implemented UX behavior for candidate approval/rejection flows.

## Scope
- Candidate decision interactions on the single-screen analysis view
- Justification capture for `confirmLink` and `rejectLink`
- Payload mapping to B7 action schemas

## Entry Points
- Candidate list row action buttons:
  - `Confirm`
  - `Reject`
  - `Add Evidence`
- Candidate detail panel action section

## UI Components

## 1) Candidate Decision Card
- Displays:
  - candidate id
  - confidence band + score
  - explain tokens
  - current status (`candidate|in_review|confirmed|rejected`)
- Primary buttons:
  - `Confirm Link`
  - `Reject Link`
  - `Add Evidence`

## 2) Confirm Dialog
- Required inputs:
  - `justification` (textarea, min 10 chars)
- Optional inputs:
  - `confidence_override` (number 0.0..1.0)
- Action:
  - submit -> `confirmLink`

## 3) Reject Dialog
- Required inputs:
  - `reason` (textarea, min 10 chars)
- Optional inputs:
  - `reason_code` (enum selector)
    - `TIME_WINDOW_MISMATCH`
    - `EQUIPMENT_CONTEXT_MISMATCH`
    - `INSUFFICIENT_EVIDENCE`
    - `DUPLICATE_CANDIDATE`
    - `OTHER`
- Action:
  - submit -> `rejectLink`

## 4) Add Evidence Drawer
- Required inputs:
  - `evidence_id` (search/select)
- Optional inputs:
  - `description` (textarea)
- Action:
  - submit -> `addEvidenceToLink`

## Interaction States

Per candidate row:
- `idle`
- `submitting`
- `success`
- `error`

Behavior:
- while `submitting`, disable action buttons and show spinner
- on success, refresh candidate status immediately
- on error, show inline error panel with `policy_code`/`error_code`

## Validation Rules

- `confirmLink.justification`: required, length >= 10
- `confirmLink.confidence_override`: optional, numeric, range `0.0..1.0`
- `rejectLink.reason`: required, length >= 10
- `addEvidenceToLink.evidence_id`: required
- unknown fields in payload are blocked client-side

Validation error messages (examples):
- `Please provide at least 10 characters for justification.`
- `Confidence override must be between 0.0 and 1.0.`
- `Please select an evidence item.`

## Payload Mapping

## confirmLink
```json
{
  "action_id": "<generated>",
  "action_type": "confirmLink",
  "requested_by": "<current_user>",
  "requested_at_unix_ms": 0,
  "request_context": { "source_app": "capa-ui", "trace_id": "<trace>" },
  "payload": {
    "link_id": "<candidate_link_id>",
    "justification": "<text>",
    "confidence_override": 0.91
  }
}
```

## rejectLink
```json
{
  "action_id": "<generated>",
  "action_type": "rejectLink",
  "requested_by": "<current_user>",
  "requested_at_unix_ms": 0,
  "request_context": { "source_app": "capa-ui", "trace_id": "<trace>" },
  "payload": {
    "link_id": "<candidate_link_id>",
    "reason": "<text>",
    "reason_code": "TIME_WINDOW_MISMATCH"
  }
}
```

## addEvidenceToLink
```json
{
  "action_id": "<generated>",
  "action_type": "addEvidenceToLink",
  "requested_by": "<current_user>",
  "requested_at_unix_ms": 0,
  "request_context": { "source_app": "capa-ui", "trace_id": "<trace>" },
  "payload": {
    "link_id": "<candidate_link_id>",
    "evidence_id": "<evidence_id>",
    "description": "<optional text>"
  }
}
```

## Permission/Error UX Mapping

- `POLICY_PERMISSION_DENIED`
  - toast: "You do not have permission to perform this action."
  - preserve unsent form inputs
- `POLICY_SCOPE_DENIED`
  - toast: "This candidate is outside your scope."
- `ACTION_INVALID_LINK_STATE`
  - refresh candidate row and show latest status

## Audit and Traceability Requirements

- Show action result with:
  - `action_id`
  - decision outcome (`allow/deny`)
  - timestamp
- Action history drawer (candidate-level):
  - recent transitions and reasons

## Accessibility Requirements

- Dialog open/close must trap and restore keyboard focus.
- All action controls must be keyboard operable.
- Errors must be announced in aria-live region.

## Acceptance Checklist
- Confirm/reject flows include required justification/reason capture.
- Payloads conform to B7-1 schema.
- Permission/policy denials are surfaced with machine-readable code context.
- Candidate status updates are reflected without leaving the current screen context.

## Dependencies
- `docs/ui/ia_ux_requirements_b8_1.md`
- `docs/actions/action_schema_specs_b7_1.md`
- `docs/actions/permission_policy_model_b7_2.md`
- `docs/actions/audit_log_model_b7_3.md`
