# Ontology Object and Link Type Specs (B6-1)

This document defines the baseline object and link type specifications for the product ontology layer.

## Scope
- Object types: `Defect`, `Cause`, `Evidence`, `CompositeCause`
- Link types:
  - `Defect -> Cause` (`has_cause`)
  - `Cause -> Evidence` (`supported_by`)
  - `Cause -> CompositeCause` (`combines_to`)
  - `Defect -> Cause` candidate link (`candidate_of`, probabilistic)

## Modeling Principles
- Every object has a stable business key and lifecycle metadata.
- Links carry minimal but explicit provenance (`link_rule`, `confidence`, timestamps).
- Probabilistic candidate links never overwrite deterministic confirmed links.
- Human confirmation status is modeled as state, not implicit deletion.

## Common Metadata Fields

All object types include:
- `id` (string, primary key)
- `source` (string)
- `interface_name` (string)
- `interface_version` (string)
- `created_at_unix_ms` (int64)
- `updated_at_unix_ms` (int64)
- `status` (enum, object-specific values)

## Object Type Specs

## 1) Defect

Purpose: top-level defect entity under analysis.

Required fields:
- `id` (e.g. `defect:{source}:{defect_id}`)
- `defect_code` (string)
- `occurred_at_unix_ms` (int64)
- `line_id` (string)

Optional fields:
- `lot_id` (string)
- `equipment_id` (string)
- `severity` (enum: `low|medium|high|critical`)
- `summary` (string)

Status enum:
- `open`
- `in_review`
- `resolved`

## 2) Cause

Purpose: normalized cause hypothesis or confirmed cause.

Required fields:
- `id` (e.g. `cause:{source}:{cause_key}`)
- `cause_type` (enum: `machine|material|method|man|measurement|environment|other`)
- `title` (string)

Optional fields:
- `description` (string)
- `confidence` (float64, 0.0..1.0)
- `detected_at_unix_ms` (int64)

Status enum:
- `candidate`
- `confirmed`
- `rejected`

## 3) Evidence

Purpose: factual support artifact (log/document/measurement/image).

Required fields:
- `id` (e.g. `evidence:{source}:{evidence_key}`)
- `evidence_type` (enum: `sensor_log|inspection_report|image|work_order|operator_note|other`)
- `captured_at_unix_ms` (int64)

Optional fields:
- `uri` (string)
- `checksum` (string)
- `content_type` (string)
- `excerpt` (string)

Status enum:
- `active`
- `archived`

## 4) CompositeCause

Purpose: grouped causal pattern composed from multiple cause nodes.

Required fields:
- `id` (e.g. `composite_cause:{source}:{group_key}`)
- `title` (string)
- `aggregation_rule` (enum: `all_of|any_of|weighted`)

Optional fields:
- `description` (string)
- `composite_confidence` (float64, 0.0..1.0)

Status enum:
- `candidate`
- `confirmed`
- `deprecated`

## Link Type Specs

All links include common fields:
- `id` (string)
- `from_id` (string)
- `to_id` (string)
- `created_at_unix_ms` (int64)
- `updated_at_unix_ms` (int64)
- `link_rule` (enum: `r1|r2|r3`)
- `confidence` (float64, 0.0..1.0, optional for deterministic)
- `status` (enum, link-specific)

## 1) `has_cause` (`Defect -> Cause`)
- Meaning: confirmed or pending causal relationship.
- Status:
  - `candidate`
  - `confirmed`
  - `rejected`
- Rules:
  - R1 creates `confirmed` directly.
  - R2 creates `candidate` with confidence.
  - R3 transitions candidate to `confirmed|rejected`.

## 2) `supported_by` (`Cause -> Evidence`)
- Meaning: evidence supports a cause claim.
- Status:
  - `linked`
  - `invalidated`
- Rules:
  - Multiple evidence links per cause allowed.
  - Evidence soft-deletion should not erase historical audit.

## 3) `combines_to` (`Cause -> CompositeCause`)
- Meaning: cause contributes to composite cause.
- Status:
  - `active`
  - `removed`
- Rules:
  - A cause can contribute to multiple composite causes if justified.

## 4) `candidate_of` (`Defect -> Cause` probabilistic candidate)
- Meaning: R2-only candidate relationship before human decision.
- Status:
  - `candidate`
  - `promoted`
  - `discarded`
- Rules:
  - Promotion generates or updates `has_cause` as `confirmed`.
  - Discard keeps audit trail and reason.

## Identity and Keying Guidance
- Recommended object IDs:
  - Defect: `defect:{source}:{defect_id}`
  - Cause: `cause:{source}:{normalized_cause_key}`
  - Evidence: `evidence:{source}:{evidence_key}`
  - CompositeCause: `composite_cause:{source}:{group_key}`
- For missing business keys, hash fallback is allowed but should be tracked as low-confidence lineage.

## Quality Rules (MVP)
- Required field violations must route to DLQ with reason codes.
- Link confidence for R2 must be in `[0.0, 1.0]`.
- Duplicate active links with same `(from_id, to_id, link_type, status)` should be prevented.

## Output and API Readiness
- This spec is intended to unblock:
  - B6-2 deterministic linking rules
  - B6-3 probabilistic candidate schema
  - B6-4 human-in-the-loop confirmation state machine
  - B7 action schemas and policy/audit definitions
