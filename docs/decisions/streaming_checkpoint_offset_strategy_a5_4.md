# Streaming Checkpoint and Offset Strategy (A5-4)

This decision record defines checkpoint/offset management for ROOTSYS streaming ingestion.

## Scope
- Streaming ingestion for `driver.kind = stream` (Kafka-first).
- Runtime behavior for resume, replay window, and failure recovery.
- Persistence model for offsets/checkpoints and operational rules.

## Core Goals
- Avoid duplicate processing across restarts as much as possible.
- Preserve at-least-once delivery guarantees (no silent loss).
- Keep idempotency aligned with existing `record_id` and fabric dedupe semantics.

## Source of Truth
- Primary checkpoint identity key:
  - `(interface.name, interface.version, source, stream.source, topic, group_id, partition)`
- Stored checkpoint value:
  - `offset` (last successfully committed event offset)
  - `updated_at_unix_ms`
  - `scheduler_run_id` (optional correlation)

## Processing Model
- **Read** batch from stream source.
- **Integrate** batch through `IntegrationPipeline`.
- **Persist output + DLQ**.
- **Commit checkpoint** only after output and DLQ writes succeed.

This ordering ensures failure before checkpoint commit replays the batch, which is acceptable with idempotent keys + dedupe.

## Start Offset Rules
- `start_offset = latest`
  - Use only when no checkpoint exists.
  - If checkpoint exists, resume from checkpoint+1.
- `start_offset = earliest`
  - Use for bootstrap/backfill when no checkpoint exists.
  - If checkpoint exists, still resume from checkpoint+1 unless explicit reset is requested.

## Failure and Recovery
- If integration or output persistence fails:
  - Do not advance checkpoint.
  - Next run reconsumes same range (at-least-once).
- If checkpoint write fails after output persistence:
  - Data may replay on next run.
  - Accept replay; rely on `record_id` and dedupe strategy to avoid duplicate downstream semantics.

## Reset and Replay Operations
- Checkpoint reset must be explicit and auditable:
  - reset by full stream key
  - reset to `earliest` or specified offset
- DLQ replay remains separate from source offset progression.
- Never mutate checkpoint automatically during DLQ replay jobs.

## Storage Strategy (MVP -> Next)
- MVP persistence backend: SQLite table in shell/runtime local storage.
- Recommended schema fields:
  - `interface_name`, `interface_version`, `source`, `stream_source`, `topic`, `group_id`, `partition`
  - `offset`, `updated_at_unix_ms`, optional `scheduler_run_id`
- Next step: external durable backend (shared DB) for multi-runner coordination.

## Concurrency Policy
- For MVP, single active runner per `(interface, topic, group_id)` is required.
- Multi-runner/partition balancing is out of scope for A5 and should be designed with lock/lease semantics.

## Observability Requirements
- Emit checkpoint metrics/logs per run:
  - checkpoint read offset
  - processed record count
  - committed offset range
  - replayed offset count (if any)
- Correlate with scheduler run logs via run ID.

## Alignment with Existing Guarantees
- `record_id_policy` remains the idempotency anchor.
- Fabric dedupe key remains `(source, interface.name, interface.version, record_id)`.
- Checkpoint policy minimizes duplicate work but does not replace idempotency controls.

## Follow-up Implementation Notes
- A5-2 introduced stream MVP ingestion.
- A5-3 introduced scheduler interval execution.
- This A5-4 strategy is the contract for implementing durable checkpoint storage and resume semantics in the next implementation increment.
