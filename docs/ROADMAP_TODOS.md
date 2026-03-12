# ROOTSYS Roadmap & TODOs

This file is the human-controlled roadmap checklist.
Use it as the source of truth for priorities, and mirror each item into GitHub Issues.

## Conventions
- IDs:
  - `A*` = Integration foundations track (this repo)
  - `B*` = Product design/spec track (post-integration: ontology/actions/ui)
  - `C*` = Product MVP implementation track (ontology/linkage/kernel/ops baseline)
  - `D*` = Whitepaper completion and productionization track
- Issue title format: `Roadmap(<ID>): <short action>`
- Labels (recommended):
  - `track/integration` / `track/product`
  - `area/docs` `area/contracts` `area/drivers` `area/resilience` `area/dlq` `area/streaming`
  - `prio/p0` `prio/p1` `prio/p2`
  - `type/feature` `type/chore` `type/design`

## Milestones (recommended)
- `M0-Foundations` (A0)
- `M1-Contracts` (A1)
- `M2-Drivers` (A2)
- `M3-Resilience` (A3)
- `M4-DLQ-Ops` (A4)
- `M5-Streaming` (A5)
- `P1-Ontology` (B6)
- `P2-Actions` (B7)
- `P3-UI` (B8)
- `C0-Core-Bootstrap` (C0)
- `C1-Ontology-Materialization` (C1)
- `C2-Linking-Engine` (C2)
- `C3-Action-Policy-Audit` (C3)
- `C4-Local-Ops-Baseline` (C4)
- `D0-Whitepaper-Core` (D0)
- `D1-Whitepaper-Hardening` (D1)

---

## A0. Foundations (Scope/Ownership/Docs)
- [x] Roadmap(A0-1): Define repo scope + ownership (docs/system)
- [x] Roadmap(A0-2): Replace placeholder entrypoint docs with real pointers (docs/**/README.md, system/**/README.md)
- [x] Roadmap(A0-3): Document Definition of Done for integration outputs (IntegrationRecord/DLQ/dedupe)
- [x] Roadmap(A0-4): Define labeling/milestone conventions for roadmap tracking

## A1. Contracts & Interface Governance
- [x] Roadmap(A1-1): Define + validate ExternalInterface JSON schema (strict field validation)
- [x] Roadmap(A1-2): Add contract registry and enforce (interface.name, interface.version) allowlist
- [x] Roadmap(A1-3): Add record_id policy mode (strict vs hash fallback) and document behavior
- [x] Roadmap(A1-4): Improve validation reporting (structured error codes vs free-form strings)

## A2. Drivers (REST/DB/File) Productionization
- [x] Roadmap(A2-1): REST auth helper: API key (header/query) injection policy
- [x] Roadmap(A2-2): REST auth helper: OAuth2 client-credentials token fetch + caching
- [x] Roadmap(A2-3): REST pagination: cursor-based (config-driven) + record emission rules
- [x] Roadmap(A2-4): REST pagination: page/page_size mode (optional second strategy)
- [x] Roadmap(A2-5): REST request policy: timeouts/rate-limit notes + safe defaults
- [x] Roadmap(A2-6): DB TLS support for Postgres connections (configurable)
- [x] Roadmap(A2-7): DB connection pooling configuration (MySQL pool options + Postgres equivalent)
- [x] Roadmap(A2-8): Standardize driver metadata capture (content_type/filename + optional source details)

## A3. Resilience (Retry/Backoff/Circuit Breaker)
- [x] Roadmap(A3-1): Implement retry/backoff policy for REST driver (with jitter)
- [x] Roadmap(A3-2): Implement retry/backoff policy for DB driver (transient error classification)
- [x] Roadmap(A3-3): Add circuit breaker for REST/DB drivers (open/half-open/close)
- [x] Roadmap(A3-4): Document idempotency + dedupe strategy across pipeline + fabric

## A4. DLQ Ops (Persistence/Replay/Lineage)
- [x] Roadmap(A4-1): Add DLQ sink interface (file today → pluggable sink)
- [x] Roadmap(A4-2): Implement DLQ persistence to one external backend (choose: S3/DB/Queue)
- [x] Roadmap(A4-3): Implement DLQ replay CLI/job (DLQ → re-integrate with new interface)
- [x] Roadmap(A4-4): Enrich DLQ entries with structured reason codes + lineage metadata

## A5. Streaming + Scheduling
- [x] Roadmap(A5-1): Choose first streaming source (Kafka vs CDC) and define minimal interface
- [x] Roadmap(A5-2): Implement streaming driver MVP (records → IntegrationPipeline)
- [x] Roadmap(A5-3): Implement scheduler integration (when/how interfaces run)
- [x] Roadmap(A5-4): Add checkpoint/offset management strategy for streaming

---

## B6. Ontology Layer (Defect/Cause/Evidence)
- [x] Roadmap(B6-1): Define object/link type specs (Defect/Cause/Evidence/CompositeCause)
- [x] Roadmap(B6-2): Define R1 deterministic linking rules + pipeline plan
- [x] Roadmap(B6-3): Define R2 probabilistic candidate links + confidence schema
- [x] Roadmap(B6-4): Define R3 human-in-the-loop confirmation state machine

## B7. Action / Policy / Audit
- [x] Roadmap(B7-1): Define Action schemas (confirmLink/rejectLink/addEvidenceToLink)
- [x] Roadmap(B7-2): Define permission/policy model (who can do what)
- [x] Roadmap(B7-3): Define audit log requirements + storage/query model

## B8. UI/App (CAPA App)
- [x] Roadmap(B8-1): Define IA/UX requirements for single-screen analysis view
- [x] Roadmap(B8-2): Implement candidate approval/rejection UX with justification capture
- [x] Roadmap(B8-3): Add operational dashboards (DLQ volume, approval rate, lead time)

---

## C0. Product Core Bootstrap (Ontology/Linkage/Kernel)
- [x] Roadmap(C0-1): Bootstrap `crates/ontology` crate with MVP interfaces + tests
- [x] Roadmap(C0-2): Bootstrap `crates/linkage` crate with MVP interfaces + tests
- [x] Roadmap(C0-3): Bootstrap `crates/kernel` crate with MVP interfaces + tests

## C1. Ontology Materialization MVP
- [x] Roadmap(C1-1): Implement object materialization for Defect/Cause/Evidence from IntegrationRecord
- [x] Roadmap(C1-2): Define deterministic object identity and lineage propagation rules
- [x] Roadmap(C1-3): Add ontology fixtures + integration tests for materialization outputs

## C2. Linking Engine MVP (R1 + lightweight R2)
- [x] Roadmap(C2-1): Implement deterministic R1 link generation from strong keys
- [x] Roadmap(C2-2): Implement configurable lightweight R2 candidate generation (time window + shared attributes)
- [x] Roadmap(C2-3): Define candidate link schema (confidence/reasons/lineage) + tests

## C3. Action / Policy / Audit Runtime MVP
- [x] Roadmap(C3-1): Implement kernel actions (confirmLink/rejectLink/addEvidenceToLink)
- [x] Roadmap(C3-2): Implement role-based policy checks for action execution
- [x] Roadmap(C3-3): Implement append-only audit logging backend (SQLite) + query model

## C4. Local Ops Baseline for MVP
- [x] Roadmap(C4-1): Provide local bootstrap path for end-to-end MVP run (docs/scripts)
- [x] Roadmap(C4-2): Add baseline observability (structured logs + key metrics counters)
- [x] Roadmap(C4-3): Add security/config baseline (.env template + secret handling guidance)
- [x] Roadmap(C4-4): Add replay/recovery runbook for common failure modes

---

## D0. Whitepaper Core Completion (Priority: P0 first, then P1)
- [x] Roadmap(D0-1): Expand ontology model to include CompositeCause and canonical Defect-Cause-Evidence relations (#60, `prio/p0`)
- [x] Roadmap(D0-2): Implement R3 human-in-the-loop candidate lifecycle and decision state machine (#61, `prio/p0`)
- [x] Roadmap(D0-3): Expose action and audit query APIs for review workflows (#62, `prio/p0`)
- [x] Roadmap(D0-4): Implement single-screen CAPA analysis UI route and 4-pane layout (#63, `prio/p0`)
- [x] Roadmap(D0-5): Integrate CAPA UI actions with policy-aware backend workflows (#64, `prio/p1`)

## D1. Whitepaper Hardening (Streaming/Security/Ops)
- [x] Roadmap(D1-1): Replace stream.kafka fixture mode with real Kafka consume and checkpoint commit (#65, `prio/p1`)
- [x] Roadmap(D1-2): Harden action security model with authenticated actor context and scoped authorization (#66, `prio/p1`)
- [x] Roadmap(D1-3): Add full whitepaper vertical-slice E2E tests across integration to UI decision flow (#67, `prio/p1`)
- [ ] Roadmap(D1-4): Extend operational dashboards for candidate throughput and analysis lead-time KPIs (#68, `prio/p2`)
- [ ] Roadmap(D1-5): Publish whitepaper-readiness runbook and release gate checklist (#69, `prio/p2`)

## Active Priority Order (Execution Queue)
1. `P2`: #68 -> #69
