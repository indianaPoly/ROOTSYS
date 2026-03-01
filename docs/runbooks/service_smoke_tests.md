# Service-backed Smoke Tests

This runbook verifies real execution paths for REST, Postgres, and MySQL drivers using local services and deterministic fixture assertions.

## Preconditions
- Docker + Docker Compose available (`docker compose` or `docker-compose`)
- Python 3 available
- Rust toolchain available (`cargo`)

## Execute
```bash
bash scripts/run_service_smoke_tests.sh
```

## What It Validates
- Local REST endpoint ingestion via `tests/fixtures/interfaces/rest.smoke.json`
- Postgres ingestion via `tests/fixtures/interfaces/postgres.smoke.json`
- MySQL ingestion via `tests/fixtures/interfaces/mysql.smoke.json`
- Exact `record_id` expectations for all three outputs
- Fabric merge path with dedupe enabled

## Expected Artifacts
- `/tmp/rootsys-smoke/rest.output.jsonl`
- `/tmp/rootsys-smoke/postgres.output.jsonl`
- `/tmp/rootsys-smoke/mysql.output.jsonl`
- `/tmp/rootsys-smoke/merged.db.output.jsonl`

Override output directory with:
```bash
ROOTSYS_SMOKE_OUT_DIR=/tmp/custom-smoke-out bash scripts/run_service_smoke_tests.sh
```
