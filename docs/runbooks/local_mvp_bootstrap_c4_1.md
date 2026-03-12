# C4-1 Local MVP Bootstrap

This runbook provides a repeatable local bootstrap path for the current MVP slice.

## Prerequisites

- Rust toolchain with `cargo`
- Python 3
- Repository checked out locally

## One-command bootstrap

```bash
bash scripts/run_local_mvp_bootstrap.sh
```

By default, outputs are written to `/tmp/rootsys-mvp`.

To override output path:

```bash
ROOTSYS_MVP_OUT_DIR=/tmp/rootsys-mvp-demo bash scripts/run_local_mvp_bootstrap.sh
```

## What the script runs

1. `python3 scripts/create_sample_dbs.py`
2. `shell` run with `tests/fixtures/interfaces/mes.db.json`
3. `shell` run with `tests/fixtures/interfaces/qms.db.json`
4. `shell` run with `tests/fixtures/interfaces/stream.kafka.sample.json`
5. `fabric` merge for MES + QMS outputs

## Expected artifacts

- `mes.output.jsonl`
- `qms.output.jsonl`
- `stream.output.jsonl`
- `merged.output.jsonl`

## Troubleshooting

- If `allowlist` validation fails, confirm interface `(name, version)` exists in
  `system/contracts/reference/allowlist.json`.
- If DB fixture reads fail, re-run `python3 scripts/create_sample_dbs.py`.
- If output files are empty, inspect interface query and fixture DB contents.

## D1-4 Operations KPI Dashboard Thresholds

The operations dashboard route is available at `/analysis/ops` and reads action/audit artifacts
from local paths under `/tmp`.

Current warning thresholds:

- Reject-rate warning: `35%`
- Lead-time warning: `240 minutes`
- Backlog warning: `25 candidates`

Threshold source: `ui/lib/ops-kpis.ts`.

## D1-4 Smoke Checks

After bootstrap, validate dashboard behavior with sample artifacts:

1. Run bootstrap and product flow artifacts:

   ```bash
   bash scripts/run_local_mvp_bootstrap.sh
   ```

2. Run UI checks:

   ```bash
   cd ui && npm run typecheck && npm run build
   ```

3. Open `/analysis/ops` and verify:
   - KPI cards render for throughput/approval/reject/lead-time/backlog
   - Trend table shows day-level counts
   - Alert panel reflects threshold comparisons
