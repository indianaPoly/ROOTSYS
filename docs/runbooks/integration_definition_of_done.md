# Integration Output Definition of Done

This checklist defines when integration outputs are considered release-ready.

## Scope
- `IntegrationRecord` emission quality
- `DeadLetter` (DLQ) quality and triage readiness
- Fabric merge dedupe correctness

## 1) Interface + Contract
- Interface JSON passes `ExternalInterface` validation.
- Interface `(name, version)` is allowlisted in `system/contracts/reference/allowlist.json`.
- `record_id_policy` is explicitly chosen (`hash_fallback` or `strict`) when non-default behavior is required.

## 2) IntegrationRecord Acceptance Criteria
- Every accepted record includes:
  - `source`
  - `interface.name`
  - `interface.version`
  - `record_id`
  - `ingested_at_unix_ms`
  - `payload`
- `record_id` behavior is deterministic for the same payload + interface policy.
- Warnings are populated only for non-blocking issues.

## 3) DLQ Acceptance Criteria
- Rejected payloads are written as `DeadLetter` with:
  - original `payload`
  - inherited `metadata`
  - non-empty `errors`
- Validation failures (schema/path/policy) are represented as explicit error strings.
- Under `record_id_policy = strict`, unresolved `record_id_paths` are DLQ-worthy failures.

## 4) Dedupe Acceptance Criteria (fabric)
- Merge dedupe key is `(source, interface.name, interface.version, record_id)`.
- Duplicate input rows collapse to one output row when dedupe is enabled.
- Non-duplicate rows are preserved.

## 5) Operational Validation
- CLI run creates output JSONL successfully.
- DLQ file is produced when there are rejected records.
- Sample fixtures (`tests/fixtures/interfaces/*.json`) pass runtime tests.

## 6) Verification Gate
- `cargo test` passes for workspace.
- `cargo build` succeeds for workspace.
- Any known limitations are documented in `README.md`.
