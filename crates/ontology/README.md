# ontology

Entry point for crates/ontology.

## Purpose
- Scope: ontology materialization interfaces from `IntegrationRecord` into typed ontology objects.
- Owner: repository maintainers (`@indianaPoly`).

## Contents
- `crates/ontology/src/lib.rs`: object materialization logic (Defect/Cause/Evidence) and JSONL serialization helper.
- `tests/fixtures/ontology/materialization.input.jsonl`: fixture records for ontology materialization tests.
- `docs/architecture/mvp_roadmap_c_track.md`: C-track ontology implementation roadmap.
