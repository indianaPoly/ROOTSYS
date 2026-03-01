#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_UI_DEV="${ROOTSYS_RUN_UI_DEV:-0}"

ensure_command() {
  local name="$1"
  if ! command -v "$name" >/dev/null 2>&1; then
    echo "required command not found: $name" >&2
    exit 1
  fi
}

ensure_docker_ready() {
  ensure_command docker
  if ! docker info >/dev/null 2>&1; then
    echo "docker daemon is not ready. start Docker Desktop first." >&2
    exit 1
  fi
}

echo "[1/8] prerequisites check"
ensure_command cargo
ensure_command python3
ensure_command npm
ensure_docker_ready

echo "[2/8] rust quality gates (fmt/check/test/build)"
cargo fmt --check
cargo check
cargo test
cargo build

echo "[3/8] service-backed smoke tests"
bash "$ROOT_DIR/scripts/run_service_smoke_tests.sh"

echo "[4/8] complex pipeline checks"
bash "$ROOT_DIR/scripts/run_complex_pipeline_checks.sh"

echo "[5/8] install frontend dependencies"
cd "$ROOT_DIR/ui"
npm install --cache .npm-cache

echo "[6/8] frontend typecheck"
npm run typecheck

echo "[7/8] frontend production build"
npm run build

echo "[8/8] completed"
echo "summary:"
echo "- rust verification: passed"
echo "- smoke tests: passed"
echo "- complex checks: passed"
echo "- ui typecheck/build: passed"
echo "- ui dashboard: http://localhost:3000 (run: cd ui && npm run dev)"

if [[ "$RUN_UI_DEV" == "1" ]]; then
  echo "ROOTSYS_RUN_UI_DEV=1 detected, starting next dev server"
  exec npm run dev
fi
