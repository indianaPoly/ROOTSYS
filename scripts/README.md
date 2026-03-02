# scripts

Entry point for scripts.

## Purpose
- Scope: local development and MVP bootstrap automation helpers.
- Owner: repository maintainers (`@indianaPoly`).

## Contents
- `scripts/create_sample_dbs.py`: creates sample fixture databases.
- `scripts/create_company_profile.sh`: scaffolds `config/companies/<profile>.env` from first customer template.
- `scripts/validate_company_profile.sh`: validates resolved config paths for a selected company profile.
- `scripts/run_local_mvp_bootstrap.sh`: runs local MVP bootstrap flow end-to-end.
- `scripts/run_service_smoke_tests.sh`: runs REST/Postgres/MySQL service-backed smoke tests with exact output assertions.
- `scripts/run_complex_pipeline_checks.sh`: runs interval/product-flow/sqlite-replay/merge checks with deterministic assertions.
- `scripts/run_all_checks_and_prepare_ui.sh`: one-shot orchestrator for Rust gates + smoke + complex checks + Next.js UI verification.
- `scripts/lib/company_config.sh`: shared profile loader and config path validator used by run scripts.
