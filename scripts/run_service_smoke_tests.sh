#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${ROOTSYS_SMOKE_OUT_DIR:-/tmp/rootsys-smoke}"
COMPOSE_FILE="$ROOT_DIR/scripts/smoke/docker-compose.yml"
REST_PORT="${ROOTSYS_SMOKE_REST_PORT:-18080}"
REST_PID=""

source "$ROOT_DIR/scripts/lib/company_config.sh"
load_company_config "$ROOT_DIR"
validate_company_config

mkdir -p "$OUT_DIR"

compose() {
  if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
    docker compose -f "$COMPOSE_FILE" "$@"
    return
  fi

  if command -v docker-compose >/dev/null 2>&1; then
    docker-compose -f "$COMPOSE_FILE" "$@"
    return
  fi

  echo "docker compose (or docker-compose) is required" >&2
  exit 1
}

cleanup() {
  if [[ -n "$REST_PID" ]] && kill -0 "$REST_PID" >/dev/null 2>&1; then
    kill "$REST_PID" >/dev/null 2>&1 || true
  fi
  compose down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "[1/8] Starting Postgres/MySQL containers"
compose up -d

echo "[2/8] Waiting for database health checks"
for _ in {1..60}; do
  if docker inspect --format='{{.State.Health.Status}}' rootsys-smoke-postgres 2>/dev/null | grep -q '^healthy$' \
    && docker inspect --format='{{.State.Health.Status}}' rootsys-smoke-mysql 2>/dev/null | grep -q '^healthy$'; then
    break
  fi
  sleep 1
done

if ! docker inspect --format='{{.State.Health.Status}}' rootsys-smoke-postgres | grep -q '^healthy$'; then
  echo "postgres container did not become healthy" >&2
  exit 1
fi

if ! docker inspect --format='{{.State.Health.Status}}' rootsys-smoke-mysql | grep -q '^healthy$'; then
  echo "mysql container did not become healthy" >&2
  exit 1
fi

echo "[3/8] Starting local REST mock server"
python3 "$ROOT_DIR/scripts/smoke/rest_mock_server.py" --port "$REST_PORT" >"$OUT_DIR/rest.mock.log" 2>&1 &
REST_PID="$!"
sleep 1

echo "[4/8] Running REST smoke interface"
cargo run -p shell -- \
  --interface "$ROOTSYS_INTERFACE_REST_SMOKE" \
  --contract-registry "$ROOTSYS_CONTRACT_REGISTRY" \
  --output "$OUT_DIR/rest.output.jsonl"

echo "[5/8] Running Postgres smoke interface"
cargo run -p shell -- \
  --interface "$ROOTSYS_INTERFACE_POSTGRES_SMOKE" \
  --contract-registry "$ROOTSYS_CONTRACT_REGISTRY" \
  --output "$OUT_DIR/postgres.output.jsonl"

echo "[6/8] Running MySQL smoke interface"
cargo run -p shell -- \
  --interface "$ROOTSYS_INTERFACE_MYSQL_SMOKE" \
  --contract-registry "$ROOTSYS_CONTRACT_REGISTRY" \
  --output "$OUT_DIR/mysql.output.jsonl"

echo "[7/8] Verifying exact record_id outputs"
python3 - "$OUT_DIR" <<'PY'
import json
import os
import pathlib
import sys

out_dir = pathlib.Path(sys.argv[1])

def parse_ids(raw: str):
    return [x for x in (item.strip() for item in raw.split(",")) if x]

cases = {
    "rest.output.jsonl": parse_ids(os.environ["ROOTSYS_SMOKE_EXPECT_REST_IDS"]),
    "postgres.output.jsonl": parse_ids(os.environ["ROOTSYS_SMOKE_EXPECT_POSTGRES_IDS"]),
    "mysql.output.jsonl": parse_ids(os.environ["ROOTSYS_SMOKE_EXPECT_MYSQL_IDS"]),
}

for name, expected in cases.items():
    path = out_dir / name
    lines = [line for line in path.read_text().splitlines() if line.strip()]
    ids = [json.loads(line)["record_id"] for line in lines]
    if ids != expected:
        raise SystemExit(f"unexpected record IDs for {name}: {ids} != {expected}")

print("record_id assertions passed")
PY

echo "[8/8] Merging DB smoke outputs"
cargo run -p fabric -- \
  --inputs "$OUT_DIR/postgres.output.jsonl" \
  --inputs "$OUT_DIR/mysql.output.jsonl" \
  --output "$OUT_DIR/merged.db.output.jsonl" \
  --dedupe

echo "Done. Artifacts:"
echo "- $OUT_DIR/rest.output.jsonl"
echo "- $OUT_DIR/postgres.output.jsonl"
echo "- $OUT_DIR/mysql.output.jsonl"
echo "- $OUT_DIR/merged.db.output.jsonl"
