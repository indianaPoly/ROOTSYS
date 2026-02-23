# Idempotency and Dedupe Strategy

This runbook defines how we keep integration and fabric outputs stable when the same source data is processed multiple times.

## Scope
- Integration pipeline outputs (`IntegrationRecord`, `DeadLetter`)
- Fabric merge output and optional dedupe behavior
- Replay/re-run scenarios for batch and future streaming paths

## Core Guarantees
- Re-processing the same payload under the same `(source, interface.name, interface.version)` does not create semantic duplicates when dedupe is enabled downstream.
- `record_id` generation is deterministic under both `hash_fallback` and `strict` policies.
- Dedupe identity in fabric is consistently computed as `(source, interface.name, interface.version, record_id)`.

## Identity Model
- Integration identity fields:
  - `source`
  - `interface.name`
  - `interface.version`
  - `record_id`
- Fabric dedupe key:
  - `(source, interface.name, interface.version, record_id)`

## Pipeline Strategy (Integration)

### 1) Deterministic `record_id`
- Prefer explicit `record_id_paths` for business-stable IDs.
- `record_id_policy = strict`:
  - if configured paths do not resolve, payload is rejected to DLQ.
  - use when duplicate tolerance is low and key quality is enforced.
- `record_id_policy = hash_fallback`:
  - use resolved path values when available.
  - otherwise use payload hash for deterministic fallback.

### 2) Repeat Run Behavior
- Re-running the same interface against identical source rows should generate the same `record_id` values.
- `ingested_at_unix_ms` can differ between runs and is not part of dedupe identity.
- Integration stage remains append-oriented; dedupe normalization is finalized at fabric merge.

### 3) DLQ Behavior and Replay Safety
- Strict policy failures (`record_id` unresolved) are intentionally non-idempotent rejections until data quality is corrected.
- Replay should use the same interface version where possible to preserve identity semantics.
- If identity rules must change, bump interface version and treat output as a new contract lineage.

## Fabric Strategy (Dedupe)

### 1) Dedupe Switch
- `--dedupe true` is recommended for production merge jobs and replay jobs.
- `--dedupe false` is only for diagnostics, audits, or raw comparison workflows.

### 2) Collapse Rules
- Records with the same dedupe key collapse to one output row.
- Records with different `source` or interface reference are intentionally not collapsed, even if payload matches.

### 3) Ordering and Determinism
- Dedupe key fields must be produced before merge.
- Upstream changes to keying logic require regression verification using fixture-based runs.

## Operational Guardrails
- Contract registry allowlist must stay aligned with interface versions used by jobs.
- Retry/circuit-breaker settings affect delivery reliability only; they must not alter dedupe identity.
- For backfills/replays:
  - prefer same interface version and same key policy.
  - run merge with dedupe enabled.
  - compare output cardinality against expected unique key count.

## Validation Checklist
- Interface validation passes (`ExternalInterface` + allowlist).
- `record_id_policy` chosen explicitly when strict identity behavior is required.
- `cargo test` and `cargo build` pass before release.
- Sample fixtures and merge runs confirm:
  - duplicate collapse works under `--dedupe true`
  - non-duplicates are preserved
  - strict-mode unresolved keys go to DLQ

## Known Trade-offs
- `hash_fallback` is resilient but can hide missing business IDs if overused.
- `strict` improves identity quality but increases DLQ volume when source quality is poor.
- Dedupe across different interface versions is intentionally isolated to avoid accidental cross-contract merges.
