# ROOTSYS Roadmap & TODOs

This file is the human-controlled roadmap checklist.
Use it as the source of truth for priorities, and mirror each item into GitHub Issues.

## Conventions
- IDs: `A*` = Integration track (this repo), `B*` = Product track (post-integration: ontology/actions/ui)
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

---

## A0. Foundations (Scope/Ownership/Docs)
- [x] Roadmap(A0-1): Define repo scope + ownership (docs/system)
- [x] Roadmap(A0-2): Replace placeholder entrypoint docs with real pointers (docs/**/README.md, system/**/README.md)
- [x] Roadmap(A0-3): Document Definition of Done for integration outputs (IntegrationRecord/DLQ/dedupe)
- [x] Roadmap(A0-4): Define labeling/milestone conventions for roadmap tracking

## A1. Contracts & Interface Governance
- [ ] Roadmap(A1-1): Define + validate ExternalInterface JSON schema (strict field validation)
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
- [ ] Roadmap(A2-7): DB connection pooling configuration (MySQL pool options + Postgres equivalent)
- [ ] Roadmap(A2-8): Standardize driver metadata capture (content_type/filename + optional source details)

## A3. Resilience (Retry/Backoff/Circuit Breaker)
- [ ] Roadmap(A3-1): Implement retry/backoff policy for REST driver (with jitter)
- [ ] Roadmap(A3-2): Implement retry/backoff policy for DB driver (transient error classification)
- [ ] Roadmap(A3-3): Add circuit breaker for REST/DB drivers (open/half-open/close)
- [ ] Roadmap(A3-4): Document idempotency + dedupe strategy across pipeline + fabric

## A4. DLQ Ops (Persistence/Replay/Lineage)
- [ ] Roadmap(A4-1): Add DLQ sink interface (file today → pluggable sink)
- [ ] Roadmap(A4-2): Implement DLQ persistence to one external backend (choose: S3/DB/Queue)
- [ ] Roadmap(A4-3): Implement DLQ replay CLI/job (DLQ → re-integrate with new interface)
- [ ] Roadmap(A4-4): Enrich DLQ entries with structured reason codes + lineage metadata

## A5. Streaming + Scheduling
- [ ] Roadmap(A5-1): Choose first streaming source (Kafka vs CDC) and define minimal interface
- [ ] Roadmap(A5-2): Implement streaming driver MVP (records → IntegrationPipeline)
- [ ] Roadmap(A5-3): Implement scheduler integration (when/how interfaces run)
- [ ] Roadmap(A5-4): Add checkpoint/offset management strategy for streaming

---

## B6. Ontology Layer (Defect/Cause/Evidence)
- [ ] Roadmap(B6-1): Define object/link type specs (Defect/Cause/Evidence/CompositeCause)
- [ ] Roadmap(B6-2): Define R1 deterministic linking rules + pipeline plan
- [ ] Roadmap(B6-3): Define R2 probabilistic candidate links + confidence schema
- [ ] Roadmap(B6-4): Define R3 human-in-the-loop confirmation state machine

## B7. Action / Policy / Audit
- [ ] Roadmap(B7-1): Define Action schemas (confirmLink/rejectLink/addEvidenceToLink)
- [ ] Roadmap(B7-2): Define permission/policy model (who can do what)
- [ ] Roadmap(B7-3): Define audit log requirements + storage/query model

## B8. UI/App (CAPA App)
- [ ] Roadmap(B8-1): Define IA/UX requirements for single-screen analysis view
- [ ] Roadmap(B8-2): Implement candidate approval/rejection UX with justification capture
- [ ] Roadmap(B8-3): Add operational dashboards (DLQ volume, approval rate, lead time)
