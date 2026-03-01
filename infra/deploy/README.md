# deploy

Entry point for infra/deploy.

## Purpose
- Scope: actionable deployment baseline for running ROOTSYS locally.
- Owner: repository maintainers (`@indianaPoly`).

## Contents
- Local bootstrap script: `scripts/run_local_mvp_bootstrap.sh`
- Local bootstrap runbook: `docs/runbooks/local_mvp_bootstrap_c4_1.md`

## Supported Today
- Local single-node execution with fixture-backed interfaces.
- Artifact outputs under `/tmp/rootsys-mvp` (or `ROOTSYS_MVP_OUT_DIR`).

## Not Supported Yet
- Production orchestration (Kubernetes, ECS, Nomad).
- Managed secret injection and environment promotion workflow.

## Deployment Checklist (Local)
1. `python3 scripts/create_sample_dbs.py`
2. `bash scripts/run_local_mvp_bootstrap.sh`
3. Verify output artifacts exist: `mes.output.jsonl`, `qms.output.jsonl`, `stream.output.jsonl`, `merged.output.jsonl`
