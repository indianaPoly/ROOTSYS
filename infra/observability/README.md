# observability

Entry point for infra/observability.

## Purpose
- Scope: baseline telemetry for pipeline health during local MVP runs.
- Owner: repository maintainers (`@indianaPoly`).

## Contents
- `docs/runbooks/observability_baseline_c4_2.md`: event/metric definitions and interpretation.
- `crates/shell/src/main.rs`: structured events emitted by runner (`run_summary`, `pipeline_metrics`, `product_flow_summary`, `product_flow_metrics`).

## Supported Today
- Structured JSON logs to stdout.
- Baseline counters for runs, inputs, integration outputs, DLQ outputs, and product flow artifacts.

## Not Supported Yet
- Prometheus/OpenTelemetry export.
- Alerting rules and SLO dashboards.

## Quick Validation
1. Run: `cargo run -p shell -- --interface ... --output ...`
2. Confirm logs include `pipeline_metrics` JSON event.
3. Confirm `dlq_records_total` reflects rejection behavior when invalid records are present.
