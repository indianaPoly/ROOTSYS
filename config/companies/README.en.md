# Company Profiles (English)

This directory contains customer/company profile files used by ROOTSYS scripts.

## Files
- `default.env`: baseline local profile
- `acme.sample.env`: sample profile
- `first-customer.sample.env`: starter template for initial enterprise onboarding

## Create a new profile
```bash
bash scripts/create_company_profile.sh <company-name>
```

## Validate a profile
```bash
bash scripts/validate_company_profile.sh <company-name>
```

## Common scale knobs
- `ROOTSYS_SMOKE_DB_COUNT`
- `ROOTSYS_SMOKE_REST_COUNT`
- `ROOTSYS_COMPLEX_STREAM_RECORD_COUNT`
- `ROOTSYS_COMPLEX_INTERVAL_RUNS`
- `ROOTSYS_COMPLEX_REPLAY_INPUT_COUNT`
