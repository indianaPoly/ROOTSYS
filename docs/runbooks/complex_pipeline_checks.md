# Complex Pipeline Checks

This runbook validates deeper execution paths beyond the baseline MVP and service smoke tests.

## Execute
```bash
bash scripts/run_complex_pipeline_checks.sh
```

## Validations Included
- Stream fixture in interval mode (`--schedule-mode interval`) with deterministic output count checks
- Product flow artifact generation checks (`ontology`, `links`, `actions`, `audit sqlite`)
- SQLite DLQ replay path (`strict` ingestion to SQLite DLQ, then permissive replay)
- Fabric merge on multi-source outputs with dedupe enabled

## Expected Artifacts
- `/tmp/rootsys-complex/stream.interval.output.jsonl`
- `/tmp/rootsys-complex/product-flow/`
- `/tmp/rootsys-complex/replay/replay.output.jsonl`
- `/tmp/rootsys-complex/complex.merged.output.jsonl`

Override output directory with:
```bash
ROOTSYS_COMPLEX_OUT_DIR=/tmp/custom-complex-out bash scripts/run_complex_pipeline_checks.sh
```
