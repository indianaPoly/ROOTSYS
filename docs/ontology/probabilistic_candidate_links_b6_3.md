# Probabilistic Candidate Links and Confidence Schema (B6-3)

This document defines R2 probabilistic candidate linking rules and confidence schema.

## Scope
- R2 candidate link generation (`Defect -> Cause`, `candidate_of`)
- Confidence score model and calibration bands
- Candidate lifecycle metadata for downstream R3 confirmation

## Goal
- Surface likely links when deterministic keys are insufficient.
- Preserve uncertainty explicitly for human review.
- Keep behavior reproducible and auditable.

## Input Preconditions
- R1 rules already attempted and no deterministic match was produced.
- Required minimum fields for R2 evaluation:
  - `source`
  - `occurred_at_unix_ms`
  - at least one of: `line_id`, `equipment_id`, `lot_id`, `defect_code`

## Candidate Link Type
- Link type: `candidate_of` (`Defect -> Cause`)
- Required fields:
  - `id`, `from_id`, `to_id`
  - `link_rule = r2`
  - `status = candidate`
  - `confidence` (`0.0..1.0`)
  - `score_components` (json)
  - `created_at_unix_ms`, `updated_at_unix_ms`

## R2 Rule Family

## Rule R2-TIME-01 (time proximity)
- Signal: defect and cause occur within configured window.
- Default window: ±30 minutes.
- Score component: `time_score`.

## Rule R2-LINE-01 (line/equipment affinity)
- Signal: same line and/or equipment context.
- Score component: `line_equipment_score`.

## Rule R2-CODE-01 (defect/cause pattern similarity)
- Signal: defect code and cause category mapping compatibility.
- Score component: `code_similarity_score`.

## Rule R2-SHIFT-01 (work-shift affinity)
- Signal: same operator shift or adjacent handoff window.
- Score component: `shift_score`.

## Confidence Schema

Final score is weighted sum:

`confidence = w_time * time_score + w_line * line_equipment_score + w_code * code_similarity_score + w_shift * shift_score`

Default weights (MVP baseline):
- `w_time = 0.40`
- `w_line = 0.25`
- `w_code = 0.25`
- `w_shift = 0.10`

Constraints:
- each component in `[0.0, 1.0]`
- confidence clamped to `[0.0, 1.0]`
- missing component contributes `0.0` unless explicitly configured otherwise

## Confidence Bands
- `0.85 - 1.00`: `high` (priority review queue)
- `0.65 - 0.84`: `medium` (standard review queue)
- `0.45 - 0.64`: `low` (optional review)
- `< 0.45`: dropped (no candidate link emitted)

## Candidate Emission Rules
- Emit candidate only when:
  - `confidence >= 0.45`
  - candidate does not duplicate active deterministic link
- Rank candidates per defect by confidence descending.
- Keep top-N candidates per defect (default `N=5`).

## Metadata Requirements

Each candidate row must include:
- `confidence_band` (`high|medium|low`)
- `score_components` json, e.g.:

```json
{
  "time_score": 0.92,
  "line_equipment_score": 1.0,
  "code_similarity_score": 0.70,
  "shift_score": 0.50,
  "weights": {"time": 0.4, "line": 0.25, "code": 0.25, "shift": 0.1}
}
```

- `explain_tokens` (array<string>, optional): machine-readable reasons (`SAME_EQUIPMENT`, `WITHIN_30M`, etc)

## Conflict and Dedup Policy
- If same `(from_id, to_id)` appears from multiple R2 passes:
  - keep highest confidence version
  - merge unique explain tokens
- If R1 later confirms same pair:
  - candidate transitions to `promoted`
  - `has_cause` confirmed link is authoritative

## Error Codes (MVP)
- `R2_REQUIRED_CONTEXT_MISSING`
- `R2_SCORE_COMPONENT_INVALID`
- `R2_WEIGHT_CONFIG_INVALID`
- `R2_CANDIDATE_CAP_EXCEEDED`

## Pipeline Plan

## Stage 1: Candidate Context Build
- Construct candidate pool using relaxed keys (time/line/equipment/shift).

## Stage 2: Component Scoring
- Compute each component score and persist intermediate table.

## Stage 3: Confidence Aggregation
- Apply weighted formula and assign confidence band.

## Stage 4: Candidate Filtering and Ranking
- Drop `<0.45`, keep top-N per defect.

## Stage 5: Candidate Link Upsert
- Upsert key: `(from_id, to_id, link_type='candidate_of', link_rule='r2')`.

## Stage 6: Metrics and Monitoring
- Emit per-run metrics:
  - total candidates generated
  - by confidence band
  - promoted/rejected counts (when R3 outcomes available)

## Validation Checklist
- Confidence values in range and reproducible for same input snapshot.
- Candidate bands align with configured thresholds.
- No candidate emitted where deterministic confirmed link already exists.

## Follow-up Dependencies
- B6-4 defines R3 state transitions (`candidate -> confirmed/rejected`).
- B8-2 UI should render confidence band + explain tokens for reviewer decisions.
