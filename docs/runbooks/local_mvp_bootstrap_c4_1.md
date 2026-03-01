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
