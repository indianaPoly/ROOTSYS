#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${ROOTSYS_MVP_OUT_DIR:-/tmp/rootsys-mvp}"

source "$ROOT_DIR/scripts/lib/company_config.sh"
load_company_config "$ROOT_DIR"
validate_company_config

mkdir -p "$OUT_DIR"

echo "[1/6] Creating sample fixture databases"
python3 "$ROOT_DIR/scripts/create_sample_dbs.py"

echo "[2/6] Running MES sqlite interface"
cargo run -p shell -- \
  --interface "$ROOTSYS_INTERFACE_MES" \
  --contract-registry "$ROOTSYS_CONTRACT_REGISTRY" \
  --output "$OUT_DIR/mes.output.jsonl"

echo "[3/6] Running QMS sqlite interface"
cargo run -p shell -- \
  --interface "$ROOTSYS_INTERFACE_QMS" \
  --contract-registry "$ROOTSYS_CONTRACT_REGISTRY" \
  --output "$OUT_DIR/qms.output.jsonl"

echo "[4/6] Running stream fixture interface"
cargo run -p shell -- \
  --interface "$ROOTSYS_INTERFACE_STREAM" \
  --contract-registry "$ROOTSYS_CONTRACT_REGISTRY" \
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
