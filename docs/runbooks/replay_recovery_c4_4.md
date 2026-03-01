# C4-4 Replay and Recovery Runbook

This runbook defines common failure modes and recovery steps for local MVP operations.

## Failure modes

1. Interface contract validation failure
2. Driver fetch/transient errors with repeated DLQ growth
3. Partial run output (integration output exists but replay pending)
4. Suspected action/audit mismatch

## Recovery procedures

### A. Replay from file DLQ

```bash
cargo run -p shell -- \
  --interface path/to/interface.json \
  --contract-registry system/contracts/reference/allowlist.json \
  --output /tmp/replay.output.jsonl \
  --replay-dlq /tmp/output.dlq.jsonl \
  --replay-dlq-source file
```

### B. Replay from SQLite DLQ

```bash
cargo run -p shell -- \
  --interface path/to/interface.json \
  --contract-registry system/contracts/reference/allowlist.json \
  --output /tmp/replay.output.jsonl \
  --replay-dlq /tmp/dlq.sqlite \
  --replay-dlq-source sqlite \
  --replay-dlq-table dead_letters
```

### C. Rebuild baseline outputs

```bash
bash scripts/run_local_mvp_bootstrap.sh
```

## Post-recovery validation checklist

1. `cargo test` passes.
2. Replay output file exists and is non-empty.
3. DLQ growth trend is reduced or explained.
4. Structured logs include `run_summary` and `pipeline_metrics` events.
5. For audit-enabled flows, latest `event_id` remains strictly increasing.

## Notes

- Always preserve original DLQ artifact before replay for forensic traceability.
- Prefer small batch replay first, then full replay when behavior is confirmed.
