# Deterministic Linking Rules and Pipeline Plan (B6-2)

This document defines R1 deterministic linking rules and execution plan for ontology pipelines.

## Scope
- Deterministic (`R1`) link generation only
- Pipeline plan from normalized records to ontology links
- Rule catalog for `Defect`, `Cause`, `Evidence`, `CompositeCause`

## Goal
- Create high-confidence links with zero ambiguity using stable keys.
- Ensure links are reproducible across reruns and backfills.

## Inputs and Prerequisites
- Object/link baseline spec:
  - `docs/ontology/object_link_type_specs_b6_1.md`
- Integration identity guarantees:
  - `source`, `interface.name`, `interface.version`, `record_id`
- Required normalized fields:
  - `defect_id`
  - `lot_id` (where applicable)
  - `equipment_id` (where applicable)
  - `occurred_at_unix_ms`

## R1 Rule Definition

R1 applies when all required matching keys exist and pass strict equality checks.

R1 characteristics:
- deterministic (`same input -> same link output`)
- confidence fixed to `1.0`
- link status created as `confirmed`
- link_rule set to `r1`

## Deterministic Rule Catalog

## Rule R1-DC-01: `Defect -> Cause (has_cause)` by exact defect key
- Required keys:
  - `source`
  - `defect_id`
  - `cause_key`
- Condition:
  - defect and cause records share exact `(source, defect_id)` context
- Output:
  - create/update `has_cause` with `status=confirmed`, `confidence=1.0`, `link_rule=r1`

## Rule R1-CE-01: `Cause -> Evidence (supported_by)` by exact evidence reference
- Required keys:
  - `source`
  - `cause_key`
  - `evidence_key`
- Condition:
  - cause record references evidence id and evidence record exists
- Output:
  - create/update `supported_by` with `status=linked`, `link_rule=r1`

## Rule R1-CC-01: `Cause -> CompositeCause (combines_to)` by deterministic group key
- Required keys:
  - `source`
  - `composite_group_key`
  - `cause_key`
- Condition:
  - cause row includes valid deterministic group key
- Output:
  - create/update `combines_to` with `status=active`, `link_rule=r1`

## Rule R1-DL-01: `Defect -> Cause` by lot/equipment strict join
- Required keys:
  - `source`
  - `lot_id`
  - `equipment_id`
  - `defect_code`
- Condition:
  - exact equality on all keys and strict time window inclusion
- Output:
  - create/update `has_cause` as deterministic confirmed link

## Deterministic Key Priority

Use the first satisfied rule by priority:
1. `defect_id` exact join
2. `cause/evidence` explicit reference ids
3. deterministic composite group key
4. strict `lot_id + equipment_id + defect_code + time_window`

If none match, do not create R1 link (defer to R2 candidate flow).

## Pipeline Plan

## Stage 1: Prepare Canonical Keys
- Normalize text/case/whitespace for key fields.
- Validate required key presence.
- Route invalid rows to DLQ with deterministic rule failure codes.

## Stage 2: Build Deterministic Join Tables
- Materialize key-indexed maps:
  - defect index
  - cause index
  - evidence index
  - composite group index

## Stage 3: Apply Rule Catalog in Priority Order
- Evaluate R1 rules sequentially.
- Record rule id used (`R1-DC-01`, etc).
- Emit link rows with deterministic metadata.

## Stage 4: Upsert Links Idempotently
- Upsert key:
  - `(from_id, to_id, link_type, link_rule)`
- Existing row behavior:
  - refresh `updated_at_unix_ms`
  - keep `status=confirmed` for R1 links

## Stage 5: Quality and Metrics
- Emit per-rule counts:
  - matched
  - skipped (missing keys)
  - conflicts
- Emit duplicate-prevented count from idempotent upsert.

## Conflict Handling
- If multiple deterministic rules produce the same link:
  - keep single link row
  - retain highest-priority rule id
  - log conflict metric/event
- If deterministic result conflicts with previously rejected R3 decision:
  - keep existing rejected state and flag for review (no silent overwrite)

## Required Error Codes (MVP)
- `R1_REQUIRED_KEY_MISSING`
- `R1_TIME_WINDOW_INVALID`
- `R1_CONFLICTING_RULES`
- `R1_REFERENCE_NOT_FOUND`

## Operational Checklist
- Rule order fixed and versioned.
- Rule set changes require document version bump.
- Backfill run must report per-rule delta before/after.

## Follow-up Dependencies
- B6-3 extends with probabilistic candidate link logic.
- B6-4 defines human confirmation transitions for candidate links.
