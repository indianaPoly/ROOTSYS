#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${ROOTSYS_COMPLEX_OUT_DIR:-/tmp/rootsys-complex}"

source "$ROOT_DIR/scripts/lib/company_config.sh"
load_company_config "$ROOT_DIR"
validate_company_config

CONTRACT_REGISTRY="$ROOTSYS_CONTRACT_REGISTRY"
STREAM_RECORD_COUNT="${ROOTSYS_COMPLEX_STREAM_RECORD_COUNT:-300}"
INTERVAL_RUNS="${ROOTSYS_COMPLEX_INTERVAL_RUNS:-2}"
REPLAY_INPUT_COUNT="${ROOTSYS_COMPLEX_REPLAY_INPUT_COUNT:-50}"

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

echo "[1/6] Interval schedule run (generated stream fixture)"

python3 - "$OUT_DIR/stream.input.jsonl" "$STREAM_RECORD_COUNT" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
count = int(sys.argv[2])
if count <= 0:
    raise SystemExit("ROOTSYS_COMPLEX_STREAM_RECORD_COUNT must be > 0")

with path.open("w", encoding="utf-8") as handle:
    for idx in range(1, count + 1):
        payload = {
            "event_id": f"stream-{idx:05d}",
            "machine": f"M-{(idx % 50) + 1:03d}",
            "status": "ok" if idx % 2 == 0 else "warn",
            "value": idx,
        }
        handle.write(json.dumps(payload, ensure_ascii=True) + "\n")
PY

cat > "$OUT_DIR/stream.generated.interface.json" <<EOF
{
  "name": "mes-stream",
  "version": "v1",
  "driver": {
    "kind": "stream",
    "stream": {
      "source": "kafka",
      "kafka": {
        "brokers": ["localhost:9092"],
        "topic": "quality_events",
        "group_id": "rootsys-test",
        "format": "json",
        "max_batch_records": 5000,
        "poll_timeout_ms": 1000,
        "start_offset": "earliest",
        "mvp_input": "$OUT_DIR/stream.input.jsonl"
      }
    }
  },
  "payload_format": "json",
  "record_id_policy": "strict",
  "record_id_paths": ["/event_id"]
}
EOF

cargo run -p shell -- \
  --interface "$OUT_DIR/stream.generated.interface.json" \
  --contract-registry "$CONTRACT_REGISTRY" \
  --output "$OUT_DIR/stream.interval.output.jsonl" \
  --schedule-mode interval \
  --interval-seconds 1 \
  --max-runs "$INTERVAL_RUNS"

python3 - "$OUT_DIR/stream.interval.output.jsonl" <<'PY'
import os
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
lines = [line for line in path.read_text().splitlines() if line.strip()]
expected_count = int(os.environ.get("ROOTSYS_COMPLEX_STREAM_RECORD_COUNT", "300")) * int(os.environ.get("ROOTSYS_COMPLEX_INTERVAL_RUNS", "2"))
if len(lines) != expected_count:
    raise SystemExit(f"expected {expected_count} stream records, got {len(lines)}")
print("stream interval assertion passed")
PY

echo "[2/6] Product-flow execution and artifact checks"
PRODUCT_DIR="$OUT_DIR/product-flow"
cargo run -p shell -- \
  --interface "$ROOTSYS_INTERFACE_MES" \
  --contract-registry "$CONTRACT_REGISTRY" \
  --output "$OUT_DIR/mes.product.base.output.jsonl" \
  --enable-product-flow \
  --product-output-dir "$PRODUCT_DIR"

python3 - "$PRODUCT_DIR" <<'PY'
import pathlib
import sqlite3
import sys

product = pathlib.Path(sys.argv[1])
required_files = {
    product / "ontology.objects.jsonl": True,
    product / "links.r1.jsonl": False,
    product / "links.r2.jsonl": False,
    product / "actions.results.jsonl": False,
    product / "actions.audit.sqlite": False,
}

for file, must_be_non_empty in required_files.items():
    if not file.exists():
        raise SystemExit(f"missing product-flow artifact: {file}")
    if must_be_non_empty and file.suffix != ".sqlite" and file.stat().st_size == 0:
        raise SystemExit(f"empty product-flow artifact: {file}")

con = sqlite3.connect(product / "actions.audit.sqlite")
con.execute("SELECT COUNT(*) FROM audit_events").fetchone()[0]
con.close()
print("product-flow artifact assertions passed")
PY

echo "[3/6] Prepare strict/permissive replay scenario fixtures"
REPLAY_DIR="$OUT_DIR/replay"
mkdir -p "$REPLAY_DIR"

python3 - "$REPLAY_DIR/input.jsonl" "$REPLAY_INPUT_COUNT" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
count = int(sys.argv[2])
if count <= 0:
    raise SystemExit("ROOTSYS_COMPLEX_REPLAY_INPUT_COUNT must be > 0")

with path.open("w", encoding="utf-8") as handle:
    for idx in range(1, count + 1):
        payload = {"foo": f"bar-{idx:05d}"}
        handle.write(json.dumps(payload, ensure_ascii=True) + "\n")
PY

cat > "$REPLAY_DIR/strict.interface.json" <<EOF
{
  "name": "${ROOTSYS_COMPLEX_REPLAY_INTERFACE_NAME}",
  "version": "${ROOTSYS_COMPLEX_REPLAY_INTERFACE_VERSION}",
  "driver": {
    "kind": "jsonl",
    "input": "${REPLAY_DIR}/input.jsonl"
  },
  "payload_format": "json",
  "record_id_policy": "strict",
  "record_id_paths": ["/defect_id"],
  "required_paths": ["/defect_id"]
}
EOF

cat > "$REPLAY_DIR/permissive.interface.json" <<EOF
{
  "name": "${ROOTSYS_COMPLEX_REPLAY_INTERFACE_NAME}",
  "version": "${ROOTSYS_COMPLEX_REPLAY_INTERFACE_VERSION}",
  "driver": {
    "kind": "jsonl",
    "input": "${REPLAY_DIR}/input.jsonl"
  },
  "payload_format": "json",
  "record_id_policy": "hash_fallback",
  "record_id_paths": []
}
EOF

echo "[4/6] Run strict ingestion with SQLite DLQ"
cargo run -p shell -- \
  --interface "$REPLAY_DIR/strict.interface.json" \
  --contract-registry "$CONTRACT_REGISTRY" \
  --output "$REPLAY_DIR/strict.output.jsonl" \
  --dlq-sink sqlite \
  --dlq "$REPLAY_DIR/strict.dlq.sqlite"

echo "[5/6] Replay from SQLite DLQ with permissive interface"
cargo run -p shell -- \
  --interface "$REPLAY_DIR/permissive.interface.json" \
  --contract-registry "$CONTRACT_REGISTRY" \
  --output "$REPLAY_DIR/replay.output.jsonl" \
  --replay-dlq "$REPLAY_DIR/strict.dlq.sqlite" \
  --replay-dlq-source sqlite \
  --replay-dlq-table dead_letters

python3 - "$REPLAY_DIR/replay.output.jsonl" <<'PY'
import json
import os
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
lines = [line for line in path.read_text().splitlines() if line.strip()]
expected_count = int(os.environ.get("ROOTSYS_COMPLEX_REPLAY_INPUT_COUNT", "50"))
if len(lines) != expected_count:
    raise SystemExit(f"expected replay to recover {expected_count} records, got {len(lines)}")
record = json.loads(lines[0])
if record.get("source") != os.environ["ROOTSYS_COMPLEX_REPLAY_INTERFACE_NAME"]:
    raise SystemExit(f"unexpected replay source: {record.get('source')}")
print("sqlite replay assertion passed")
PY

echo "[6/6] Merge replay+stream outputs with dedupe"
cargo run -p fabric -- \
  --inputs "$REPLAY_DIR/replay.output.jsonl" \
  --inputs "$OUT_DIR/stream.interval.output.jsonl" \
  --output "$OUT_DIR/complex.merged.output.jsonl" \
  --dedupe

echo "Complex pipeline checks completed. Artifacts:"
echo "- $OUT_DIR/stream.interval.output.jsonl"
echo "- $OUT_DIR/product-flow/"
echo "- $OUT_DIR/replay/replay.output.jsonl"
echo "- $OUT_DIR/complex.merged.output.jsonl"
