# Runbooks (English)

Operational runbook index for ROOTSYS runtime execution and release readiness.

## Main runbooks
- `service_smoke_tests.md`: service-backed smoke execution and assertions
- `complex_pipeline_checks.md`: high-depth runtime checks (interval/product/replay/merge)
- `company_profile_configuration.md`: customer profile setup before execution
- `code_and_script_annotations.md`: implementation map for scripts/code responsibilities
- `integration_definition_of_done.md`: integration DoD criteria
- `idempotency_dedupe_strategy.md`: dedupe/idempotency operation model

## Suggested order
1. Configure profile (`company_profile_configuration.md`)
2. Run smoke (`service_smoke_tests.md`)
3. Run complex checks (`complex_pipeline_checks.md`)
4. Validate release/readiness policies
