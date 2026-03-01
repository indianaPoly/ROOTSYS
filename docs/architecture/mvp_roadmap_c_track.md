# MVP Roadmap (C Track)

This document defines the next execution track required to move from integration foundations to a usable MVP.
It starts from the current state where A0-A5 and B6-B8 are completed, but key runtime product layers
(`ontology`, `linkage`, `kernel`, and operational `infra`) are still not implemented.

## Scope and intent

- Build the minimum end-to-end flow from integrated records to reviewable candidate links and audited decisions.
- Keep scope narrow: one executable vertical slice before broadening source coverage.
- Prioritize implementation over additional concept docs.

## Current gap summary

- `crates/ontology` is README-only (no object model materialization code).
- `crates/linkage` is README-only (no R1/R2 candidate generation engine).
- `crates/kernel` is README-only (no action/policy/audit command handling).
- `infra/*` is mostly placeholder docs (no concrete local deploy/observability/security baseline).

## Milestones

### C0 - Core product crates bootstrapping

Goal: make `ontology`, `linkage`, and `kernel` compile and integrate into workspace with minimal APIs.

Definition of done:

- Each crate has `Cargo.toml`, `src/lib.rs`, and smoke tests.
- Clear boundary structs/traits are defined and imported by `shell` or `runtime` entrypoints.
- `cargo check`, `cargo test`, and `cargo build` pass workspace-wide.

### C1 - Ontology materialization MVP

Goal: materialize `Defect`, `Cause`, `Evidence`, and link primitives from `IntegrationRecord` stream.

Definition of done:

- Implement deterministic object IDs for core object types.
- Emit normalized ontology object records to JSONL output.
- Add fixtures + tests for mapping behavior and lineage propagation.

### C2 - Link generation MVP (R1 and lightweight R2)

Goal: generate links usable by review UI and downstream actions.

Definition of done:

- R1 deterministic links from strong keys (`defect_id`, `lot_id`, etc.).
- Lightweight R2 candidates from configurable time-window + shared attributes.
- Candidate record schema includes `confidence`, `reasons`, and source lineage.
- Unit/integration tests for both R1 and R2 behavior.

### C3 - Action/Policy/Audit runtime MVP

Goal: execute review decisions (`confirm`, `reject`, `add_evidence`) with policy checks and immutable audit logs.

Definition of done:

- `kernel` exposes command handlers for review actions.
- Simple policy layer supports role-based allow/deny checks.
- Audit append log persisted to local SQLite backend with queryable fields.
- Tests cover authorization failures and successful writes.

### C4 - Local operations baseline

Goal: provide a runnable local environment with basic observability and security controls for MVP demos.

Definition of done:

- Local stack bootstrap doc/script for running shell + storage dependencies.
- Baseline observability: structured logs and minimum metrics counters.
- Secrets/config handling guidelines and non-committed env template.
- Ops runbook for replay/recovery + known failure modes.

## Recommended execution order

1. C0-1 to C0-3 (crate scaffolding and interfaces)
2. C1-* (object materialization)
3. C2-* (link generation)
4. C3-* (actions + audit)
5. C4-* (operations baseline)

## Out of scope for this MVP track

- Full CAPA web app implementation.
- Advanced ML ranking and model serving infra.
- Multi-tenant hardening and production-grade SRE controls.
