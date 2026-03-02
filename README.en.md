# ROOTSYS (English Guide)

This is the English entry guide for the current ROOTSYS implementation state.

## What is implemented
- Rust data integration runtime (`shell`, `runtime`, `drivers`, `fabric`)
- Service-backed smoke tests (REST/Postgres/MySQL)
- Complex pipeline checks (interval stream, product flow, DLQ replay, merge)
- Company profile-based configuration (`config/companies/*.env`)
- Next.js 16 runtime dashboard (`ui/`)

## Main commands
- Full one-shot execution:
```bash
bash scripts/run_all_checks_and_prepare_ui.sh default
```
- Create customer profile:
```bash
bash scripts/create_company_profile.sh <company-name>
```
- Validate customer profile:
```bash
bash scripts/validate_company_profile.sh <company-name>
```

## Scale-up test example
```bash
ROOTSYS_SMOKE_DB_COUNT=500 \
ROOTSYS_SMOKE_REST_COUNT=500 \
ROOTSYS_COMPLEX_STREAM_RECORD_COUNT=1000 \
ROOTSYS_COMPLEX_REPLAY_INPUT_COUNT=200 \
bash scripts/run_all_checks_and_prepare_ui.sh default
```

## Where to read more
- Core usage and architecture: `README.md`
- Script catalog: `scripts/README.md`
- Company profiles: `config/companies/README.md`
- Runbooks index: `docs/runbooks/README.md`
- UI app: `ui/README.md`
