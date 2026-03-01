# C1-2 Object Identity and Lineage Rules

This document defines the deterministic identity and lineage propagation rules for ontology objects
materialized from `IntegrationRecord` inputs.

## Deterministic identity strategy

- Strategy ID: `deterministic_v1`
- Object ID inputs (in order):
  1. `source`
  2. `interface.name`
  3. `interface.version`
  4. `record_id`
  5. `object_type` token (`defect` | `cause` | `evidence`)
- Hash algorithm: SHA-256 over the ordered tokens separated with a zero-byte delimiter.

Implications:

- Re-running materialization with the same input produces the same object IDs.
- Different object types from one record produce distinct IDs.
- Identity is independent of runtime clock and execution order.

## Lineage propagation rules

Each materialized object includes `lineage` populated from source integration metadata.

Required lineage fields:

- `source`
- `interface_name`
- `interface_version`
- `record_id`
- `ingested_at_unix_ms`
- `payload_kind` (`json`, `text`, `binary`)
- `payload_sha256` (SHA-256 over `Payload::to_bytes()`)
- `warning_count` (number of integration warnings)

## Verification expectations

- Idempotency test: repeated materialization over identical input yields exactly equal objects.
- Provenance test: lineage fields are present and stable for fixed input.
- Type differentiation test: `defect`, `cause`, and `evidence` IDs differ for the same source record.
