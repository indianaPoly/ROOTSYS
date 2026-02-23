# Roadmap Tracking Conventions

This document defines how roadmap work is tracked across `docs/ROADMAP_TODOS.md` and GitHub Issues.

## Scope
- Applies to all roadmap IDs in `docs/ROADMAP_TODOS.md`.
- Covers issue title format, labels, milestones, and completion flow.

## Canonical Sources
- Planning source of truth: `docs/ROADMAP_TODOS.md`
- Execution tracker: GitHub Issues (`roadmap` label)

## Issue Naming
- Required format: `Roadmap(<ID>): <short action>`
- Example: `Roadmap(A1-3): Add record_id policy mode (strict vs hash fallback) and document behavior`

## Required Labels
- `roadmap`
- Track: `track/integration` or `track/product`
- Area: one of `area/docs`, `area/contracts`, `area/drivers`, `area/resilience`, `area/dlq`, `area/streaming`, `area/ontology`, `area/actions`, `area/ui`
- Priority: `prio/p0`, `prio/p1`, or `prio/p2`
- Type: `type/feature`, `type/chore`, `type/design`, `type/test` (as applicable)

## Milestone Mapping
- A0 -> `M0-Foundations`
- A1 -> `M1-Contracts`
- A2 -> `M2-Drivers`
- A3 -> `M3-Resilience`
- A4 -> `M4-DLQ-Ops`
- A5 -> `M5-Streaming`
- B6 -> `P1-Ontology`
- B7 -> `P2-Actions`
- B8 -> `P3-UI`

## Completion Workflow
1. Implement scoped change.
2. Verify locally (`cargo test`, `cargo build`).
3. Update `docs/ROADMAP_TODOS.md` checklist entry from `[ ]` to `[x]`.
4. Close the corresponding GitHub issue with commit/PR reference.

## Change Control
- Keep one roadmap item per issue.
- Avoid combining unrelated IDs in one issue.
- If scope expands, split follow-up issue(s) and cross-link.
