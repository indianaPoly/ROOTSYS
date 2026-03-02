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
  - Stream driver MVP: `stream.kafka` (fixture-backed input via `mvp_input`)
- **DLQ (dead letter) handling** with a pluggable sink interface (file sink implemented by default).
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

### Stream input (Kafka MVP)
```bash
cargo run -p shell -- \
  --interface tests/fixtures/interfaces/stream.kafka.sample.json \
  --contract-registry system/contracts/reference/allowlist.json \
  --output /tmp/stream.output.jsonl
```

- Current MVP behavior: `driver.stream.kafka.mvp_input` is consumed as the stream source.
- This keeps payload normalization identical to other drivers while streaming runtime semantics evolve.

### Scheduler integration (interval mode)
- Default run mode is one-shot (`--schedule-mode once`).
- Interval mode executes the same interface repeatedly:
  - `--schedule-mode interval`
  - `--interval-seconds <n>`
  - `--max-runs <n>`

Example (run stream interface every 5s, 3 times):
```bash
cargo run -p shell -- \
  --interface tests/fixtures/interfaces/stream.kafka.sample.json \
  --contract-registry system/contracts/reference/allowlist.json \
  --output /tmp/stream.scheduled.output.jsonl \
  --schedule-mode interval \
  --interval-seconds 5 \
  --max-runs 3
```

### Merge integration outputs
```bash
cargo run -p fabric -- \
  --inputs /tmp/mes.output.jsonl \
  --inputs /tmp/qms.output.jsonl \
  --output /tmp/merged.output.jsonl \
  --dedupe
```

### Service-backed smoke tests (REST/Postgres/MySQL)
```bash
bash scripts/run_service_smoke_tests.sh
```

### Complex pipeline checks (schedule/product-flow/replay/merge)
```bash
bash scripts/run_complex_pipeline_checks.sh
```

### Runtime dashboard UI (Next.js 16)
```bash
cd ui
npm install
npm run dev
```

Open `http://localhost:3000` to view artifact status from `/tmp/rootsys-smoke` and `/tmp/rootsys-complex` outputs.

### One-shot full execution (all checks + UI verify)
```bash
bash scripts/run_all_checks_and_prepare_ui.sh
```

### Company profile-based execution
```bash
bash scripts/run_all_checks_and_prepare_ui.sh default
```

Create a new customer profile:
```bash
bash scripts/create_company_profile.sh <company-name>
```

Validate profile paths before full run:
```bash
bash scripts/validate_company_profile.sh <company-name>
```

Scale-up test example:
```bash
ROOTSYS_SMOKE_DB_COUNT=500 ROOTSYS_SMOKE_REST_COUNT=500 ROOTSYS_COMPLEX_STREAM_RECORD_COUNT=1000 bash scripts/run_all_checks_and_prepare_ui.sh <company-name>
```

Profile file location:
- `config/companies/<profile>.env`

Custom config file override:
```bash
ROOTSYS_CONFIG_FILE=/absolute/path/to/company.env bash scripts/run_all_checks_and_prepare_ui.sh
```

Optional: automatically start Next.js dev server after all checks:
```bash
ROOTSYS_RUN_UI_DEV=1 bash scripts/run_all_checks_and_prepare_ui.sh
```

### DLQ sink options
- File sink (default):
  - `--dlq-sink file`
  - `--dlq /path/to/output.dlq.jsonl` (optional, default is derived from `--output`)
- SQLite sink:
  - `--dlq-sink sqlite`
  - `--dlq /path/to/dlq.sqlite` (optional, default is derived from `--output`)
  - `--dlq-table dead_letters` (optional)

Example (SQLite DLQ sink):
```bash
cargo run -p shell -- \
  --interface path/to/interface.json \
  --contract-registry system/contracts/reference/allowlist.json \
  --output /tmp/output.jsonl \
  --dlq-sink sqlite \
  --dlq /tmp/dlq.sqlite \
  --dlq-table dead_letters
```

### DLQ replay (re-integrate rejected payloads)
- Replay from file DLQ:
```bash
cargo run -p shell -- \
  --interface path/to/interface.json \
  --contract-registry system/contracts/reference/allowlist.json \
  --output /tmp/replay.output.jsonl \
  --replay-dlq /tmp/output.dlq.jsonl \
  --replay-dlq-source file
```
- Replay from SQLite DLQ:
```bash
cargo run -p shell -- \
  --interface path/to/interface.json \
  --contract-registry system/contracts/reference/allowlist.json \
  --output /tmp/replay.output.jsonl \
  --replay-dlq /tmp/dlq.sqlite \
  --replay-dlq-source sqlite \
  --replay-dlq-table dead_letters
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
      "retry": {
        "max_attempts": 3,
        "base_delay_ms": 100,
        "max_delay_ms": 2000,
        "jitter_percent": 20
      },
      "circuit_breaker": {
        "failure_threshold": 5,
        "open_timeout_ms": 30000
      },
      "response_format": "json",
      "items_pointer": "/items"
    }
  },
  "payload_format": "json"
}
```
- `items_pointer` is optional. If it points to a JSON array, one record is created per element.
- If `response_format` is `unknown`, the driver tries JSON, then UTF-8 text, then falls back to binary.
- Safe default request timeout is `5000ms` when `timeout_ms` is omitted.
- Transient REST failures are retried with exponential backoff and jitter.
- API key auth supports `in: "header"` and `in: "query"` injection modes.
- OAuth2 client-credentials auth is supported via `auth.kind = "oauth2_client_credentials"` with
  `token_url`, `client_id`, `client_secret`, and optional `scope`.
- OAuth2 access tokens are cached in-memory and refreshed before expiry.
- Cursor pagination is supported via `pagination.kind = "cursor"`.
- Page/page_size pagination is supported via `pagination.kind = "page"`.
- Safe default page cap is `100` requests when pagination `max_pages` is omitted.
- Cursor record emission rules:
  - `items_pointer` points to an array -> emit one record per item.
  - `items_pointer` omitted or non-array target -> emit one record per page payload.
- Page/page_size record emission rules:
  - `items_pointer` points to an array -> emit one record per item.
  - emission stops when a page emits zero records, or `max_pages` is reached.
- Rate-limit policy notes:
  - transient HTTP failures (`408`, `425`, `429`, `500`, `502`, `503`, `504`) and transport errors are retried.
  - default retry policy: `max_attempts=3`, `base_delay_ms=100`, `max_delay_ms=2000`, `jitter_percent=20`.
  - retry policy can be overridden with `rest.retry`.
- Circuit breaker policy is optional for REST and supports:
  - `failure_threshold` (default `5`)
  - `open_timeout_ms` (default `30000`)
  - state transitions: `closed -> open -> half_open -> closed`
  - Use conservative `page_size`, explicit `max_pages`, and endpoint-side quotas for safe operation.

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

Cursor pagination example:
```json
{
  "name": "external-api",
  "version": "v1",
  "driver": {
    "kind": "rest",
    "rest": {
      "url": "https://api.example.com/events",
      "method": "GET",
      "response_format": "json",
      "items_pointer": "/items",
      "pagination": {
        "kind": "cursor",
        "cursor": {
          "cursor_param": "cursor",
          "cursor_path": "/next_cursor",
          "initial_cursor": "",
          "max_pages": 100
        }
      }
    }
  },
  "payload_format": "json"
}
```

Page/page_size pagination example:
```json
{
  "name": "external-api",
  "version": "v1",
  "driver": {
    "kind": "rest",
    "rest": {
      "url": "https://api.example.com/events",
      "method": "GET",
      "response_format": "json",
      "items_pointer": "/items",
      "pagination": {
        "kind": "page",
        "page": {
          "page_param": "page",
          "page_size_param": "page_size",
          "page_size": 100,
          "initial_page": 1,
          "max_pages": 50
        }
      }
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
      "query": "SELECT * FROM defect_events",
      "postgres_tls_mode": "require",
      "pool": {
        "min_connections": 1,
        "max_connections": 10
      },
      "retry": {
        "max_attempts": 3,
        "base_delay_ms": 100,
        "max_delay_ms": 2000,
        "jitter_percent": 20
      },
      "circuit_breaker": {
        "failure_threshold": 5,
        "open_timeout_ms": 30000
      }
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
      "query": "SELECT * FROM defect_events",
      "pool": {
        "min_connections": 1,
        "max_connections": 10
      },
      "retry": {
        "max_attempts": 3,
        "base_delay_ms": 100,
        "max_delay_ms": 2000,
        "jitter_percent": 20
      },
      "circuit_breaker": {
        "failure_threshold": 5,
        "open_timeout_ms": 30000
      }
    }
  },
  "payload_format": "json"
}
```
- `postgres_tls_mode` is optional and only valid for `kind: "postgres"`.
- `postgres_tls_mode` supports:
  - `disable` (default)
  - `require`
- `pool` is optional and supported for `postgres` and `mysql`.
- Pool defaults (when omitted): `min_connections=1`, `max_connections=10`.
- DB retry policy is optional for all DB kinds and supports:
  - `max_attempts` (default `3`)
  - `base_delay_ms` (default `100`)
  - `max_delay_ms` (default `2000`)
  - `jitter_percent` (default `20`, range `0..=100`)
- Circuit breaker policy is optional for all DB kinds and supports:
  - `failure_threshold` (default `5`)
  - `open_timeout_ms` (default `30000`)
  - state transitions: `closed -> open -> half_open -> closed`

## Output Records
- `IntegrationRecord` retains the raw payload plus metadata and pipeline annotations.
- `metadata` is standardized across drivers:
  - `content_type`
  - `filename`
  - optional `source_details` (`source_type`, optional `locator`)
- `IntegrationRecord.warnings` and `DeadLetter.errors` are structured messages:
  - `code`: machine-readable error/warning code
  - `path`: optional JSON path/pointer context
  - `message`: human-readable detail
- `DeadLetter` retains the raw payload plus structured validation errors.
- `DeadLetter.reason_codes` stores unique machine-readable reason code list.
- `DeadLetter.lineage` stores rejection lineage metadata (`pipeline_stage`, `driver_kind`, `record_id_policy`, source context).
## Merge Output
- The merge layer outputs the same `IntegrationRecord` JSONL format.
- When dedupe is enabled, it removes duplicates by `(source, interface.name, interface.version, record_id)`.
- Operational strategy reference: `docs/runbooks/idempotency_dedupe_strategy.md`.

## Next Steps (Planned)
1. Expand executable product flow coverage across ontology/linkage/kernel runtime paths.
2. Add shell-level end-to-end integration tests for CLI execution chains.

## Verification
The repository is validated locally with:

```bash
cargo fmt --check
cargo check
cargo test
cargo build
```

CI preflight also enforces Rust quality gates on pull requests via `.github/workflows/preflight.yml`.
