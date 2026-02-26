# Permission and Policy Model (B7-2)

This document defines who can perform which actions and how policy checks are evaluated.

## Scope
- Authorization model for action execution (`confirmLink`, `rejectLink`, `addEvidenceToLink`)
- Policy evaluation order and decision outcomes
- Minimal role model for operator workflows

## Design Goals
- Explicit least-privilege permissions
- Deterministic policy decisions with machine-readable denial reasons
- Separation of role assignment and runtime policy checks

## Core Concepts

## Subject
- Human user or service actor performing an action.
- Subject fields:
  - `subject_id`
  - `subject_type` (`user|service`)
  - `roles` (set)

## Resource
- Target entity of action.
- Resource fields:
  - `resource_type` (`link|evidence|defect|cause`)
  - `resource_id`
  - `tenant` (optional)
  - `owner_team` (optional)

## Action Permission Keys
- `link.confirm`
- `link.reject`
- `link.attach_evidence`
- `link.read`
- `evidence.read`

## Role Model (MVP)

- `operator`
  - allow: `link.read`, `evidence.read`, `link.attach_evidence`
  - conditional allow: `link.confirm`, `link.reject` for owned scope

- `reviewer`
  - allow: all operator permissions
  - allow: `link.confirm`, `link.reject` across shared scope

- `admin`
  - allow: all permissions across tenant
  - reserved for governance and incident handling

## Policy Evaluation Order

Policy checks run in this order (fail-fast):

1. `AUTHN_REQUIRED`
   - subject must be authenticated

2. `ACTION_SCHEMA_VALID`
   - payload must pass B7-1 schema validation

3. `RESOURCE_EXISTS`
   - target resource must exist and be accessible

4. `PERMISSION_MATCH`
   - subject roles must grant required permission key

5. `SCOPE_POLICY`
   - scope checks (tenant/team/ownership) must pass

6. `STATE_POLICY`
   - target state transition must be valid (`candidate -> confirmed/rejected`)

## Decision Output Contract

Every decision returns:
- `decision` (`allow|deny`)
- `policy_code` (machine-readable)
- `reason` (human-readable)

Standard deny codes:
- `POLICY_AUTHN_REQUIRED`
- `POLICY_RESOURCE_NOT_FOUND`
- `POLICY_PERMISSION_DENIED`
- `POLICY_SCOPE_DENIED`
- `POLICY_INVALID_STATE`

## Action-to-Permission Mapping

- `confirmLink` -> requires `link.confirm`
- `rejectLink` -> requires `link.reject`
- `addEvidenceToLink` -> requires `link.attach_evidence`

## Scope Rules (MVP)

- Operator scope:
  - can confirm/reject links only if `owner_team == subject.team`
- Reviewer scope:
  - can confirm/reject across teams within same tenant
- Admin scope:
  - unrestricted within tenant

## Conflict Resolution

- Explicit deny policy overrides allow.
- Missing role/permission defaults to deny.
- For multi-role users, union of allows is evaluated before explicit deny.

## Minimal Policy Matrix

| Role     | confirmLink | rejectLink | addEvidenceToLink |
|----------|-------------|------------|-------------------|
| operator | conditional | conditional| allow             |
| reviewer | allow       | allow      | allow             |
| admin    | allow       | allow      | allow             |

## Example Decision Records

```json
{
  "decision": "deny",
  "policy_code": "POLICY_SCOPE_DENIED",
  "reason": "operator cannot modify link outside owner_team"
}
```

```json
{
  "decision": "allow",
  "policy_code": "POLICY_ALLOW",
  "reason": "reviewer has link.confirm in tenant scope"
}
```

## Integration Notes
- B7-1 action schemas define payload shapes.
- B7-3 audit log must persist permission decision context (`decision`, `policy_code`, `subject_id`).
- B8-2 UI should surface deny code and remediation hint.
