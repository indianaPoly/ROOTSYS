# D1-5 Whitepaper Readiness Release Gate

This runbook defines go/no-go criteria for whitepaper-complete operation.

## Readiness Criteria by Layer

### Integration Layer

- External interfaces validate against `system/schemas/external_interface.schema.json`.
- Contract allowlist enforcement is active (`system/contracts/reference/allowlist.json`).
- Stream Kafka mode supports `mvp_file` and `live` with checkpoint file strategy.

### Ontology and Linkage Layer

- Ontology includes `Defect`, `Cause`, `CompositeCause`, and `Evidence` object materialization.
- Canonical relations are emitted (`has_cause`, `supported_by`, `combines_to`).
- Deterministic and probabilistic linking outputs are generated and schema-valid.

### Action and Policy Layer

- Candidate lifecycle states are enforced (`candidate`, `in_review`, `confirmed`, `rejected`).
- Action execution requires authenticated context and scoped link authorization.
- Audit logging captures allow/deny decisions with machine-readable error context.

### UI and Operations Layer

- `/analysis/[defectId]` single-screen review path is operational.
- `/analysis/ops` KPI dashboard reports throughput, approval/reject rate, and lead time.
- Alert thresholds are configured and documented (`ui/lib/ops-kpis.ts`).

## Release Gate Checklist (Go/No-Go)

Run from repository root unless noted.

1. Lint/format/static quality

   ```bash
   cargo fmt --check
   ```

2. Type/compile checks

   ```bash
   cargo check
   ```

3. Tests (unit + integration + e2e)

   ```bash
   cargo test
   ```

4. UI type/build checks

   ```bash
   cd ui && npm run typecheck && npm run build
   ```

5. Local bootstrap smoke

   ```bash
   bash scripts/run_local_mvp_bootstrap.sh
   ```

6. Product-flow + action API smoke

   - Open `/analysis` and `/analysis/[defectId]`.
   - Execute confirm/reject/add-evidence actions.
   - Confirm `/analysis/ops` reflects KPI updates.

7. Security gate

   - If `ROOTSYS_ACTION_API_TOKEN` is set, validate `x-rootsys-auth-token` enforcement.
   - Validate deny-path audit entries are persisted for unauthorized attempts.

## Rollback and Recovery

### Rollback Trigger

- Decision API errors or policy denials spike unexpectedly.
- Kafka live ingestion regresses (missing/duplicated offsets).
- KPI alerts exceed thresholds without mitigation.

### Rollback Procedure

1. Revert deployment to the previous known-good commit.
2. Switch stream interfaces to `mode: mvp_file` if live Kafka recovery is required.
3. Preserve and snapshot the current audit and checkpoint artifacts under `/tmp`.
4. Re-run `cargo test` and UI checks before re-promoting.

### Recovery Validation

- `actions.audit.sqlite` remains queryable and append-only.
- Kafka checkpoint file contains valid partition offsets.
- `/analysis/ops` KPI trend returns to expected baseline.

## Verification Evidence Template

For release notes or PR description, include:

- Commands executed
- Pass/fail status per gate
- Commit SHA deployed
- Rollback SHA (if used)
