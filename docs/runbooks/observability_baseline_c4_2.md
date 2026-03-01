# C4-2 Observability Baseline

This baseline defines minimum observability signals for local MVP operations.

## Structured log events

The shell runner emits JSON log lines for key execution points:

- `run_summary`
  - `run_index`
  - `source`
  - `input_records`
  - `integration_records`
  - `dlq_records`
- `pipeline_metrics`
  - `runs_total`
  - `input_records_total`
  - `integration_records_total`
  - `dlq_records_total`

Each event includes:

- `event`
- `ts_unix_ms`
- `payload`

## Operational interpretation

- Rising `dlq_records_total` indicates schema/contract drift or source data quality regressions.
- Gap between `input_records_total` and `integration_records_total` tracks rejected volume.
- `runs_total` should match scheduler expectations for interval mode.

## Next observability increments

- Add per-interface tags for metrics aggregation.
- Export counters in Prometheus-compatible format.
- Add action/audit counters once kernel actions are wired to runtime execution.
