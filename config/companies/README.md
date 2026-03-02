# company profiles

Profile files used by ROOTSYS run scripts.

- `default.env`: baseline local profile.
- `acme.sample.env`: alternate sample profile.
- `first-customer.sample.env`: starter template for first enterprise onboarding.

Create a new profile:
```bash
bash scripts/create_company_profile.sh <company-name>
```

Validate a profile:
```bash
bash scripts/validate_company_profile.sh <company-name>
```
