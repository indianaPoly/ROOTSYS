# Operational Dashboards Spec (B8-3)

This document defines operational dashboard requirements for DLQ volume, approval rate, and lead time.

## Scope
- Operator/reviewer monitoring dashboards
- Metric definitions, dimensions, and refresh cadence
- Query model aligned with action audit events and integration outputs

## Dashboard Goals
- Detect quality regressions early (DLQ spikes, low approval quality).
- Measure decision throughput and latency.
- Support weekly operational review and incident triage.

## Dashboard Set

## 1) DLQ Operations Dashboard

Primary metrics:
- `dlq_volume_total` (count)
- `dlq_volume_by_reason_code` (top N)
- `dlq_rate` = `dead_letters / (records + dead_letters)`
- `replay_success_rate`

Breakdowns:
- by `source`
- by `interface_name/interface_version`
- by time window (`hour`, `day`, `week`)

Alert suggestions:
- DLQ rate > threshold for 2 consecutive windows
- sudden +X% increase in top reason code

## 2) Candidate Decision Dashboard

Primary metrics:
- `approval_rate` = `confirmed / (confirmed + rejected)`
- `rejection_rate`
- `candidate_backlog` (open `candidate|in_review` count)
- `policy_denial_rate`

Breakdowns:
- by reviewer/team
- by confidence band (`high|medium|low`)
- by reason code

## 3) Lead Time Dashboard

Primary metrics:
- `time_to_first_decision` (candidate created -> first confirm/reject)
- `time_to_confirmation` (candidate created -> confirmed)
- `review_cycle_time_p50/p90`

Breakdowns:
- by source/interface
- by confidence band
- by shift/day-of-week

## Metric Definitions

## DLQ Volume
- Numerator: rows written to DLQ sink (file/sqlite)
- Time anchor: `dead_letter.lineage.rejected_at_unix_ms` (fallback ingest timestamp)

## Approval Rate
- Confirmed count: audit events where `event_type=action.state_transition_succeeded` and `to_state=confirmed`
- Rejected count: same with `to_state=rejected`
- Windowed formula:
  - `approval_rate = confirmed / (confirmed + rejected)`

## Lead Time
- `candidate_created_at`: candidate link `created_at_unix_ms`
- `decision_at`: transition event timestamp
- `lead_time_ms = decision_at - candidate_created_at`

## Data Sources
- Action audit model:
  - `docs/actions/audit_log_model_b7_3.md`
- Candidate link states (ontology link store)
- Integration outcomes and DLQ rows (runtime outputs)

## Query Model (Reference)

## DLQ by reason code
```sql
SELECT reason_code, COUNT(*) AS cnt
FROM dlq_events
WHERE rejected_at_unix_ms BETWEEN :from_ms AND :to_ms
GROUP BY reason_code
ORDER BY cnt DESC;
```

## Approval rate
```sql
WITH decisions AS (
  SELECT to_state
  FROM action_audit_events
  WHERE event_type = 'action.state_transition_succeeded'
    AND event_at_unix_ms BETWEEN :from_ms AND :to_ms
)
SELECT
  SUM(CASE WHEN to_state='confirmed' THEN 1 ELSE 0 END) * 1.0 /
  NULLIF(SUM(CASE WHEN to_state IN ('confirmed','rejected') THEN 1 ELSE 0 END), 0) AS approval_rate
FROM decisions;
```

## Review cycle p90
```sql
SELECT
  percentile_cont(0.9) WITHIN GROUP (ORDER BY lead_time_ms) AS p90_lead_time
FROM candidate_decision_facts
WHERE decided_at_unix_ms BETWEEN :from_ms AND :to_ms;
```

## UX Requirements for Dashboards
- Global filters:
  - date range
  - source
  - interface version
  - team/reviewer
- Drilldown behavior:
  - click metric card -> filtered table with raw events
  - from table row -> deep-link to `/analysis/:defect_id`
- Export:
  - CSV export for current filtered view

## Refresh and SLA
- Near-real-time mode: refresh every 60s
- Review mode: daily materialized aggregates
- Dashboard load target: <= 3s for default filter set

## Access Control
- Operator: read-only dashboard access for own scope
- Reviewer: read-only broader scope
- Admin: full scope including denial and audit drilldown

## Acceptance Checklist
- Metric definitions unambiguous and queryable
- Required dimensions and filters specified
- Drilldown path to raw evidence/audit events specified
- Alert conditions defined for DLQ and decision quality

## Dependencies
- `docs/actions/audit_log_model_b7_3.md`
- `docs/ui/candidate_approval_rejection_ux_b8_2.md`
- `docs/ui/ia_ux_requirements_b8_1.md`
