# ROOTSYS Data Integration

## What This Is
This repository implements the first stage of the data integration pipeline described in `docs/WHITE_PAPER.md`.
The goal is to connect external systems with unknown or inconsistent schemas and normalize them into a
stable integration record stream without forcing a fixed payload schema.

The pipeline works in three steps:
1. **Driver** fetches raw data from an external system (file, REST, or DB).
2. **Integration pipeline** validates minimal contract rules and emits:
   - `IntegrationRecord` for accepted payloads
   - `DeadLetter` for rejected payloads
3. **Fabric merge** combines multiple integration outputs into one dataset.

## Current Structure
- `crates/common`: Shared data structures (`Payload`, `IntegrationRecord`, `DeadLetter`, `PayloadFormat`).
- `crates/drivers`: External system drivers (file, REST, DB).
- `crates/runtime`: Interface definition + integration pipeline logic.
- `crates/shell`: CLI runner for the pipeline.
- `crates/fabric`: Merge layer that combines multiple integration outputs.

## What Was Implemented
- **Opaque input support** using `Payload::Binary` (base64 encoded) to preserve unstructured data.
- **External interface definition** that drives validation (required paths, record id paths, payload format).
- **Drivers**:
  - File drivers: `jsonl`, `text`, `binary`
  - REST driver: basic GET/POST with headers and optional body
  - DB driver: `sqlite`, `postgres`, `mysql`
- **DLQ (dead letter) handling** for payloads that fail validation.
- **Merge layer** to combine multiple pipeline outputs with optional dedupe.

## How To Run
### Create sample DBs (fixtures)
```bash
python3 scripts/create_sample_dbs.py
```

### File-based input (JSONL)
```bash
cargo run -p shell -- \
  --interface path/to/interface.json \
  --contract-registry system/contracts/reference/allowlist.json \
  --input path/to/input.jsonl \
  --output path/to/output.jsonl
```

### Opaque binary input
```bash
cargo run -p shell -- \
  --interface path/to/interface.json \
  --contract-registry system/contracts/reference/allowlist.json \
  --input path/to/input.bin \
  --output path/to/output.jsonl \
  --format binary
```

### REST input
```bash
cargo run -p shell -- \
  --interface path/to/rest-interface.json \
  --contract-registry system/contracts/reference/allowlist.json \
  --output path/to/output.jsonl
```

### DB input (sqlite)
```bash
cargo run -p shell -- \
  --interface tests/fixtures/interfaces/mes.db.json \
  --contract-registry system/contracts/reference/allowlist.json \
  --output /tmp/mes.output.jsonl
```

```bash
cargo run -p shell -- \
  --interface tests/fixtures/interfaces/qms.db.json \
  --contract-registry system/contracts/reference/allowlist.json \
  --output /tmp/qms.output.jsonl
```

### DB input (postgres/mysql)
```bash
cargo run -p shell -- \
  --interface path/to/postgres.interface.json \
  --contract-registry system/contracts/reference/allowlist.json \
  --output /tmp/postgres.output.jsonl
```

```bash
cargo run -p shell -- \
  --interface path/to/mysql.interface.json \
  --contract-registry system/contracts/reference/allowlist.json \
  --output /tmp/mysql.output.jsonl
```

### Merge integration outputs
```bash
cargo run -p fabric -- \
  --inputs /tmp/mes.output.jsonl /tmp/qms.output.jsonl \
  --output /tmp/merged.output.jsonl \
  --dedupe true
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
  "record_id_policy": "hash_fallback",
  "record_id_paths": ["/defect_id", "/lot_id"],
  "required_paths": ["/defect_id"]
}
```

- `record_id_policy` controls how record IDs are generated:
  - `hash_fallback` (default): use `record_id_paths` when present, otherwise hash the payload.
  - `strict`: require `record_id_paths` to resolve; unresolved IDs are emitted to DLQ.

- Contract governance is enforced through `--contract-registry` (default: `system/contracts/reference/allowlist.json`).
  The interface `(name, version)` pair must exist in the allowlist.

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
      "headers": { "Accept": "application/json" },
      "auth": {
        "kind": "api_key",
        "api_key": {
          "in": "header",
          "name": "X-API-KEY",
          "value": "<token>"
        }
      },
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
- API key auth supports `in: "header"` and `in: "query"` injection modes.
- OAuth2 client-credentials auth is supported via `auth.kind = "oauth2_client_credentials"` with
  `token_url`, `client_id`, `client_secret`, and optional `scope`.
- OAuth2 access tokens are cached in-memory and refreshed before expiry.

OAuth2 example:
```json
{
  "name": "external-api",
  "version": "v1",
  "driver": {
    "kind": "rest",
    "rest": {
      "url": "https://api.example.com/events",
      "method": "GET",
      "auth": {
        "kind": "oauth2_client_credentials",
        "oauth2_client_credentials": {
          "token_url": "https://auth.example.com/oauth/token",
          "client_id": "client-id",
          "client_secret": "client-secret",
          "scope": "events:read"
        }
      },
      "response_format": "json"
    }
  },
  "payload_format": "json"
}
```

### Driver: DB (sqlite)
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

### Driver: DB (postgres/mysql)
```json
{
  "name": "ops-db",
  "version": "v1",
  "driver": {
    "kind": "db",
    "db": {
      "kind": "postgres",
      "connection": "host=localhost user=app password=secret dbname=ops",
      "query": "SELECT * FROM defect_events"
    }
  },
  "payload_format": "json"
}
```
```json
{
  "name": "ops-db",
  "version": "v1",
  "driver": {
    "kind": "db",
    "db": {
      "kind": "mysql",
      "connection": "mysql://app:secret@localhost:3306/ops",
      "query": "SELECT * FROM defect_events"
    }
  },
  "payload_format": "json"
}
```

## Output Records
- `IntegrationRecord` retains the raw payload plus metadata and pipeline annotations.
- `IntegrationRecord.warnings` and `DeadLetter.errors` are structured messages:
  - `code`: machine-readable error/warning code
  - `path`: optional JSON path/pointer context
  - `message`: human-readable detail
- `DeadLetter` retains the raw payload plus structured validation errors.
## Merge Output
- The merge layer outputs the same `IntegrationRecord` JSONL format.
- When dedupe is enabled, it removes duplicates by `(source, interface.name, interface.version, record_id)`.

## Next Steps (Planned)
1. Add REST auth helpers (OAuth, API keys) and pagination.
2. Add retry/backoff and circuit breaker policies for REST/DB drivers.
3. Add connection pooling and TLS options for DB drivers.
4. Persist DLQ to external storage (S3, DB, queue).
5. Add schema registry or contract versioning enforcement.
6. Add streaming drivers (Kafka, CDC) and scheduler integration.

## Verification
Build/test is currently **NOT VERIFIED** in this environment due to restricted network access for crates.io.
