#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${ROOTSYS_SMOKE_OUT_DIR:-/tmp/rootsys-smoke}"
COMPOSE_FILE="$ROOT_DIR/scripts/smoke/docker-compose.yml"
REST_PORT="${ROOTSYS_SMOKE_REST_PORT:-18080}"
REST_PID=""
SMOKE_DB_COUNT="${ROOTSYS_SMOKE_DB_COUNT:-200}"
SMOKE_REST_COUNT="${ROOTSYS_SMOKE_REST_COUNT:-200}"
SMOKE_REST_ID_PREFIX="${ROOTSYS_SMOKE_REST_ID_PREFIX:-rest}"

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
python3 "$ROOT_DIR/scripts/smoke/rest_mock_server.py" \
  --port "$REST_PORT" \
  --count "$SMOKE_REST_COUNT" \
  --id-prefix "$SMOKE_REST_ID_PREFIX" \
  >"$OUT_DIR/rest.mock.log" 2>&1 &
REST_PID="$!"
sleep 1

echo "[3.5/8] Seeding Postgres/MySQL smoke datasets"
docker exec rootsys-smoke-postgres psql -U app -d ops -v ON_ERROR_STOP=1 -c "TRUNCATE defect_events; INSERT INTO defect_events(defect_id, lot_id, notes) SELECT 'PG-' || LPAD(g::text, 4, '0'), 'LOT-PG-' || g::text, 'postgres smoke row ' || g::text FROM generate_series(1, ${SMOKE_DB_COUNT}) AS g;" >/dev/null
docker exec rootsys-smoke-mysql mysql -uapp -psecret ops -e "SET SESSION cte_max_recursion_depth = GREATEST(${SMOKE_DB_COUNT} + 10, 1000); TRUNCATE TABLE defect_events; INSERT INTO defect_events(defect_id, lot_id, notes) WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < ${SMOKE_DB_COUNT}) SELECT CONCAT('MY-', LPAD(n, 4, '0')), CONCAT('LOT-MY-', n), CONCAT('mysql smoke row ', n) FROM seq;" >/dev/null

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
db_count = int(os.environ.get("ROOTSYS_SMOKE_DB_COUNT", "200"))
rest_count = int(os.environ.get("ROOTSYS_SMOKE_REST_COUNT", "200"))
rest_prefix = os.environ.get("ROOTSYS_SMOKE_REST_ID_PREFIX", "rest")

def ids_from_jsonl(path: pathlib.Path):
    lines = [line for line in path.read_text().splitlines() if line.strip()]
    return [json.loads(line)["record_id"] for line in lines]

rest_ids = ids_from_jsonl(out_dir / "rest.output.jsonl")
postgres_ids = ids_from_jsonl(out_dir / "postgres.output.jsonl")
mysql_ids = ids_from_jsonl(out_dir / "mysql.output.jsonl")

if len(rest_ids) != rest_count:
    raise SystemExit(f"unexpected REST record count: {len(rest_ids)} != {rest_count}")
if len(postgres_ids) != db_count:
    raise SystemExit(f"unexpected Postgres record count: {len(postgres_ids)} != {db_count}")
if len(mysql_ids) != db_count:
    raise SystemExit(f"unexpected MySQL record count: {len(mysql_ids)} != {db_count}")

if rest_ids[0] != f"{rest_prefix}-0001" or rest_ids[-1] != f"{rest_prefix}-{rest_count:04d}":
    raise SystemExit("unexpected REST record_id boundaries")
if postgres_ids[0] != "PG-0001|LOT-PG-1" or postgres_ids[-1] != f"PG-{db_count:04d}|LOT-PG-{db_count}":
    raise SystemExit("unexpected Postgres record_id boundaries")
if mysql_ids[0] != "MY-0001|LOT-MY-1" or mysql_ids[-1] != f"MY-{db_count:04d}|LOT-MY-{db_count}":
    raise SystemExit("unexpected MySQL record_id boundaries")

if len(set(rest_ids)) != rest_count:
    raise SystemExit("REST record_id contains duplicates")
if len(set(postgres_ids)) != db_count:
    raise SystemExit("Postgres record_id contains duplicates")
if len(set(mysql_ids)) != db_count:
    raise SystemExit("MySQL record_id contains duplicates")

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
