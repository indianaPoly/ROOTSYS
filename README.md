# ROOTSYS Data Integration

## What This Is
This repository implements the first stage of the data integration pipeline described in `docs/WHITE_PAPER.md`.
The goal is to connect external systems with unknown or inconsistent schemas and normalize them into a
stable integration record stream without forcing a fixed payload schema.

The pipeline works in two steps:
1. **Driver** fetches raw data from an external system (file, REST, or DB).
2. **Integration pipeline** validates minimal contract rules and emits:
   - `IntegrationRecord` for accepted payloads
   - `DeadLetter` for rejected payloads

## Current Structure
- `crates/common`: Shared data structures (`Payload`, `IntegrationRecord`, `DeadLetter`, `PayloadFormat`).
- `crates/drivers`: External system drivers (file, REST, DB).
- `crates/runtime`: Interface definition + integration pipeline logic.
- `crates/shell`: CLI runner for the pipeline.

## What Was Implemented
- **Opaque input support** using `Payload::Binary` (base64 encoded) to preserve unstructured data.
- **External interface definition** that drives validation (required paths, record id paths, payload format).
- **Drivers**:
  - File drivers: `jsonl`, `text`, `binary`
  - REST driver: basic GET/POST with headers and optional body
  - DB driver: **sqlite only** (postgres/mysql configs are parsed but not implemented)
- **DLQ (dead letter) handling** for payloads that fail validation.

## How To Run
### File-based input (JSONL)
```bash
cargo run -p shell -- \
  --interface path/to/interface.json \
  --input path/to/input.jsonl \
  --output path/to/output.jsonl
```

### Opaque binary input
```bash
cargo run -p shell -- \
  --interface path/to/interface.json \
  --input path/to/input.bin \
  --output path/to/output.jsonl \
  --format binary
```

### REST input
```bash
cargo run -p shell -- \
  --interface path/to/rest-interface.json \
  --output path/to/output.jsonl
```

### DB input (sqlite)
```bash
cargo run -p shell -- \
  --interface path/to/db-interface.json \
  --output path/to/output.jsonl
```

## Interface Definition (External System)
The interface JSON drives the pipeline. Example:
```json
{
  "name": "mes",
  "version": "v1",
  "driver": {
    "kind": "jsonl",
    "input": "./data/mes.jsonl",
    "content_type": "application/x-ndjson"
  },
  "payload_format": "json",
  "record_id_paths": ["/defect_id", "/lot_id"],
  "required_paths": ["/defect_id"]
}
```

### Driver: REST
```json
{
  "name": "external-api",
  "version": "v1",
  "driver": {
    "kind": "rest",
    "rest": {
      "url": "https://api.example.com/events",
      "method": "GET",
      "headers": { "Authorization": "Bearer ..." },
      "timeout_ms": 5000,
      "response_format": "json",
      "items_pointer": "/items"
    }
  },
  "payload_format": "json"
}
```
- `items_pointer` is optional. If it points to a JSON array, one record is created per element.
- If `response_format` is `unknown`, the driver tries JSON, then UTF-8 text, then falls back to binary.

### Driver: DB (sqlite only)
```json
{
  "name": "local-db",
  "version": "v1",
  "driver": {
    "kind": "db",
    "db": {
      "kind": "sqlite",
      "connection": "./data/sample.db",
      "query": "SELECT * FROM defect_events"
    }
  },
  "payload_format": "json"
}
```
- Each row becomes a JSON object where keys are column names.
- Blob columns are base64 encoded.

## Output Records
- `IntegrationRecord` retains the raw payload plus metadata and pipeline annotations.
- `DeadLetter` retains the raw payload plus validation errors.

## Next Steps (Planned)
1. Add REST auth helpers (OAuth, API keys) and pagination.
2. Implement DB drivers for postgres/mysql.
3. Add retry/backoff and circuit breaker policies for REST/DB drivers.
4. Persist DLQ to external storage (S3, DB, queue).
5. Add schema registry or contract versioning enforcement.
6. Add streaming drivers (Kafka, CDC) and scheduler integration.

## Verification
Build/test is currently **NOT VERIFIED** in this environment due to restricted network access for crates.io.
