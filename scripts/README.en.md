# scripts (English)

Script entrypoint for local execution, verification, and customer onboarding.

## Important scripts
- `scripts/run_all_checks_and_prepare_ui.sh`: one-shot full flow (Rust gates + smoke + complex + UI build)
- `scripts/run_service_smoke_tests.sh`: service-backed smoke validation (REST/Postgres/MySQL)
- `scripts/run_complex_pipeline_checks.sh`: deeper checks (schedule/product flow/replay/merge)
- `scripts/run_local_mvp_bootstrap.sh`: baseline local MVP chain
- `scripts/create_company_profile.sh`: scaffold new customer profile env
- `scripts/validate_company_profile.sh`: validate selected profile paths/settings
- `scripts/create_sample_dbs.py`: generate sqlite fixtures with configurable row counts

## Shared config loader
- `scripts/lib/company_config.sh` is sourced by run scripts
- Loads profile file + applies defaults + validates required files/numeric knobs

## Typical run sequence
```bash
bash scripts/create_company_profile.sh hanul-motors
bash scripts/validate_company_profile.sh hanul-motors
bash scripts/run_all_checks_and_prepare_ui.sh hanul-motors
```
