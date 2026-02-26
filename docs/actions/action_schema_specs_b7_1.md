# Action Schema Specs (B7-1)

This document defines baseline action schemas for ontology link decisions.

## Scope
- `confirmLink`
- `rejectLink`
- `addEvidenceToLink`

These actions operate on ontology links and support R3 human-in-the-loop workflows.

## Common Action Envelope

All actions use the same envelope fields:
- `action_id` (string, unique)
- `action_type` (enum)
- `requested_by` (string, user id)
- `requested_at_unix_ms` (int64)
- `request_context` (object, optional)
  - `source_app` (string)
  - `trace_id` (string)
  - `comment` (string)

Validation rules:
- `action_id`, `action_type`, `requested_by`, `requested_at_unix_ms` are required.
- Unknown fields are rejected at schema boundary.

## 1) confirmLink

Purpose: promote a candidate link to confirmed.

Payload schema:
- `link_id` (string, required)
- `justification` (string, required)
- `confidence_override` (number, optional, range `0.0..1.0`)

Preconditions:
- target link exists
- target link status is `candidate`
- actor has permission `link.confirm`

Postconditions:
- link status transitions to `confirmed`
- action emits audit event with previous/new status

Failure codes:
- `ACTION_LINK_NOT_FOUND`
- `ACTION_INVALID_LINK_STATE`
- `ACTION_PERMISSION_DENIED`
- `ACTION_VALIDATION_FAILED`

## 2) rejectLink

Purpose: reject a candidate link.

Payload schema:
- `link_id` (string, required)
- `reason` (string, required)
- `reason_code` (string, optional, machine-readable)

Preconditions:
- target link exists
- target link status is `candidate`
- actor has permission `link.reject`

Postconditions:
- link status transitions to `rejected`
- action emits audit event with reject reason

Failure codes:
- `ACTION_LINK_NOT_FOUND`
- `ACTION_INVALID_LINK_STATE`
- `ACTION_PERMISSION_DENIED`
- `ACTION_VALIDATION_FAILED`

## 3) addEvidenceToLink

Purpose: attach an evidence object to a link decision context.

Payload schema:
- `link_id` (string, required)
- `evidence_id` (string, required)
- `description` (string, optional)

Preconditions:
- target link exists
- target evidence exists
- actor has permission `link.attach_evidence`

Postconditions:
- creates/updates `supported_by` relation between cause and evidence context
- action emits audit event with attached evidence id

Failure codes:
- `ACTION_LINK_NOT_FOUND`
- `ACTION_EVIDENCE_NOT_FOUND`
- `ACTION_PERMISSION_DENIED`
- `ACTION_VALIDATION_FAILED`

## State Transition Summary

- `confirmLink`: `candidate -> confirmed`
- `rejectLink`: `candidate -> rejected`
- `addEvidenceToLink`: no direct link status transition required

## Idempotency Guidance

- `action_id` is the idempotency key.
- Repeated submission with same `action_id` must be handled as idempotent replay.
- Repeated submission with different `action_id` but identical payload is treated as a separate action.

## Minimal JSON Examples

```json
{
  "action_id": "act_20260226_001",
  "action_type": "confirmLink",
  "requested_by": "operator_a",
  "requested_at_unix_ms": 1762200000000,
  "request_context": { "source_app": "capa", "trace_id": "tr_123" },
  "payload": {
    "link_id": "link_abc",
    "justification": "same lot and matching defect pattern",
    "confidence_override": 0.92
  }
}
```

```json
{
  "action_id": "act_20260226_002",
  "action_type": "rejectLink",
  "requested_by": "operator_b",
  "requested_at_unix_ms": 1762200005000,
  "payload": {
    "link_id": "link_xyz",
    "reason": "time window mismatch",
    "reason_code": "TIME_WINDOW_MISMATCH"
  }
}
```

```json
{
  "action_id": "act_20260226_003",
  "action_type": "addEvidenceToLink",
  "requested_by": "operator_a",
  "requested_at_unix_ms": 1762200010000,
  "payload": {
    "link_id": "link_abc",
    "evidence_id": "evidence_77",
    "description": "maintenance report page 3"
  }
}
```

## Follow-up Dependencies
- B7-2 permission model binds required permission keys.
- B7-3 audit log model defines event storage/query schema.
- B8-2 approval/rejection UX should submit payloads conforming to this spec.
