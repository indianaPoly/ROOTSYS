# Audit Log Requirements and Storage/Query Model (B7-3)

This document defines audit requirements and a storage/query model for action and policy events.

## Scope
- Audit coverage for B7 action execution (`confirmLink`, `rejectLink`, `addEvidenceToLink`)
- Required event fields and retention expectations
- Logical storage model and query patterns for operations/compliance

## Audit Goals
- Full traceability of who did what, when, and why.
- Reliable forensic replay for disputed decisions.
- Machine-queryable events for dashboards and policy tuning.

## Event Capture Requirements

An audit event MUST be recorded for:
- action request accepted
- action validation failure
- policy evaluation result (allow/deny)
- state transition success/failure
- evidence attach success/failure

## Required Event Fields

Every event row includes:
- `event_id` (string, unique)
- `event_at_unix_ms` (int64)
- `event_type` (enum)
- `action_id` (string, nullable for system events)
- `action_type` (string)
- `subject_id` (string)
- `subject_type` (`user|service`)
- `roles` (array<string>)
- `resource_type` (string)
- `resource_id` (string)
- `tenant` (string, optional)
- `decision` (`allow|deny|na`)
- `policy_code` (string, optional)
- `status_before` (string, optional)
- `status_after` (string, optional)
- `reason` (string, optional)
- `reason_code` (string, optional)
- `trace_id` (string, optional)
- `request_payload_json` (json)
- `result_payload_json` (json)

## Event Type Catalog (MVP)

- `action.received`
- `action.validation_failed`
- `action.policy_decision`
- `action.state_transition_succeeded`
- `action.state_transition_failed`
- `action.evidence_attached`
- `action.execution_failed`

## Storage Model

Recommended logical table: `action_audit_events`

Partition keys:
- `event_date` (derived from `event_at_unix_ms`)
- optional `tenant`

Indexes:
- `idx_audit_action_id` (`action_id`)
- `idx_audit_subject_time` (`subject_id`, `event_at_unix_ms desc`)
- `idx_audit_resource_time` (`resource_type`, `resource_id`, `event_at_unix_ms desc`)
- `idx_audit_policy_code` (`policy_code`, `event_at_unix_ms desc`)
- `idx_audit_event_type_time` (`event_type`, `event_at_unix_ms desc`)

## Retention and Immutability

- Retention baseline: 365 days online + archive tier beyond baseline.
- Audit records are append-only.
- Correction is represented as a new event, never in-place overwrite.

## Query Model (Required Use Cases)

## 1) Action Timeline
- Input: `action_id`
- Output: ordered event sequence for single decision lifecycle

## 2) User Activity
- Input: `subject_id`, time range
- Output: actions executed, deny rates, impacted resources

## 3) Resource History
- Input: `resource_type`, `resource_id`
- Output: all modifications and decision rationale over time

## 4) Policy Denial Analysis
- Input: `policy_code`, time range
- Output: frequency, top affected roles/resources

## 5) Compliance Export
- Input: tenant + date range
- Output: immutable event extract with integrity metadata

## Minimal SQL Examples

```sql
-- Action timeline
SELECT *
FROM action_audit_events
WHERE action_id = :action_id
ORDER BY event_at_unix_ms ASC;
```

```sql
-- Denial trend by policy code
SELECT policy_code, COUNT(*) AS denied_count
FROM action_audit_events
WHERE decision = 'deny'
  AND event_at_unix_ms BETWEEN :from_ms AND :to_ms
GROUP BY policy_code
ORDER BY denied_count DESC;
```

## Integration Contracts
- B7-1 provides `action_id`, `action_type`, payload structure.
- B7-2 provides `decision` and `policy_code` fields.
- B8-3 dashboards consume aggregation queries from this model.

## Non-Goals (B7-3)
- Not implementing physical DB migration in this step.
- Not defining tenant-specific legal hold policies beyond baseline retention.
