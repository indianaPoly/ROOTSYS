#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${ROOTSYS_MVP_OUT_DIR:-/tmp/rootsys-mvp}"

mkdir -p "$OUT_DIR"

echo "[1/6] Creating sample fixture databases"
python3 "$ROOT_DIR/scripts/create_sample_dbs.py"

echo "[2/6] Running MES sqlite interface"
cargo run -p shell -- \
  --interface "$ROOT_DIR/tests/fixtures/interfaces/mes.db.json" \
  --contract-registry "$ROOT_DIR/system/contracts/reference/allowlist.json" \
  --output "$OUT_DIR/mes.output.jsonl"

echo "[3/6] Running QMS sqlite interface"
cargo run -p shell -- \
  --interface "$ROOT_DIR/tests/fixtures/interfaces/qms.db.json" \
  --contract-registry "$ROOT_DIR/system/contracts/reference/allowlist.json" \
  --output "$OUT_DIR/qms.output.jsonl"

echo "[4/6] Running stream fixture interface"
cargo run -p shell -- \
  --interface "$ROOT_DIR/tests/fixtures/interfaces/stream.kafka.sample.json" \
  --contract-registry "$ROOT_DIR/system/contracts/reference/allowlist.json" \
  --output "$OUT_DIR/stream.output.jsonl"

echo "[5/6] Merging MES + QMS outputs"
cargo run -p fabric -- \
  --inputs "$OUT_DIR/mes.output.jsonl" \
  --inputs "$OUT_DIR/qms.output.jsonl" \
  --output "$OUT_DIR/merged.output.jsonl" \
  --dedupe

echo "[6/6] Done"
echo "Artifacts:"
echo "- $OUT_DIR/mes.output.jsonl"
echo "- $OUT_DIR/qms.output.jsonl"
echo "- $OUT_DIR/stream.output.jsonl"
echo "- $OUT_DIR/merged.output.jsonl"
