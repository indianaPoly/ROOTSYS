# Streaming Source Selection (A5-1)

This decision record selects the first streaming source for ROOTSYS and defines a minimal interface contract for the A5 MVP track.

## Decision
- First streaming source: **Kafka**
- Deferred source: CDC connectors (database log-based capture)

## Why Kafka First
- The current pipeline model already consumes record-like payloads, which maps directly to Kafka message consumption.
- Kafka allows clear separation of concerns for A5 sequencing:
  - A5-2: streaming driver (consume + transform into `ExternalRecord`)
  - A5-3: scheduler/runner integration
  - A5-4: checkpoint/offset strategy
- Kafka is easier to test locally with fixture topics than introducing DB-specific CDC semantics in the first MVP.
- CDC remains important, but it is better treated as a follow-up source profile once offset/checkpoint primitives are stabilized.

## Minimal Streaming Interface (Proposed)

This is a design target for A5-2/A5-3 implementation (not yet wired into runtime schema):

```json
{
  "name": "mes-stream",
  "version": "v1",
  "driver": {
    "kind": "stream",
    "stream": {
      "source": "kafka",
      "kafka": {
        "brokers": ["localhost:9092"],
        "topic": "mes.events",
        "group_id": "rootsys.mes.v1",
        "format": "json",
        "max_batch_records": 500,
        "poll_timeout_ms": 1000,
        "start_offset": "latest"
      }
    }
  },
  "payload_format": "json",
  "record_id_policy": "hash_fallback",
  "record_id_paths": ["/event_id"]
}
```

## MVP Boundaries
- Included in A5-1:
  - source selection rationale (Kafka over CDC)
  - minimal interface field set for implementation handoff
- Not included in A5-1:
  - runtime schema changes for `driver.kind = stream`
  - actual Kafka driver implementation
  - durable offset/checkpoint persistence

## Risks and Mitigations
- Risk: minimal interface may overfit Kafka and complicate future CDC support.
  - Mitigation: keep `stream.source` explicit and isolate source-specific config under `stream.kafka`.
- Risk: replay/idempotency drift between batch and stream ingest paths.
  - Mitigation: keep `record_id_policy` behavior identical to existing integration pipeline semantics.

## Follow-up IDs
- A5-2: implement streaming driver MVP based on this contract.
- A5-3: integrate runner/scheduler semantics.
- A5-4: finalize checkpoint/offset policy and recovery behavior.
