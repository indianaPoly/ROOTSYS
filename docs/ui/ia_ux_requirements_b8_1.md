# IA/UX Requirements for Single-Screen Analysis View (B8-1)

This document defines the information architecture and UX requirements for a single-screen CAPA analysis experience.

## Goal
- Let operators analyze one defect context end-to-end on a single screen without switching tools.
- Minimize analysis lead time from data discovery to action decision.

## Primary User Roles
- `operator`: reviews candidates, confirms/rejects links, adds evidence.
- `reviewer`: validates operator decisions and escalates uncertain cases.
- `admin` (read-mostly for B8 scope): inspects history and system health.

## Core Screen Model

The single screen is composed of 4 synchronized zones:

1. `Defect Context Pane`
   - defect identity, severity, line, lot, occurrence timeline
   - quick filters and search

2. `Cause Graph Pane`
   - confirmed causes, candidate causes, composite cause cluster
   - visual distinction by status (`candidate|confirmed|rejected`)

3. `Evidence Pane`
   - linked evidence list with type badges (image/log/report/note)
   - preview panel and source metadata

4. `Action Pane`
   - primary actions: `confirmLink`, `rejectLink`, `addEvidenceToLink`
   - reason/justification inputs and submit summary

## IA Requirements

## Navigation
- Global navigation should keep users inside one defect-centric route:
  - `/analysis/:defect_id`
- In-screen tabs are allowed only for dense subviews, not for leaving context.

## Entity Hierarchy
- Top node: `Defect`
- Middle nodes: `Cause` and `CompositeCause`
- Support node: `Evidence`
- All panels must reference the same currently selected defect.

## Search/Filter
- Required quick filters:
  - cause status
  - confidence range
  - evidence type
  - time window
- Search must support:
  - defect id
  - cause title/description
  - evidence keyword/excerpt

## UX Requirements

## Selection and Focus
- Selecting a cause highlights:
  - related links in graph pane
  - supporting evidence in evidence pane
  - relevant action form defaults in action pane

## Action Workflow
- `confirmLink`
  - requires justification input
  - optional confidence override
- `rejectLink`
  - requires reason input
  - optional reason code selection
- `addEvidenceToLink`
  - requires evidence selection
  - optional description

## Feedback and Safety
- Show optimistic pending state with explicit completion result.
- If action fails, show machine-readable error code and retry guidance.
- Require confirmation modal only for destructive transitions (`candidate -> rejected`).

## Explainability
- Candidate links must display confidence and origin rule (`R1|R2|R3`).
- Users can open lineage details (source system, interface version, timestamp).

## Accessibility and Performance Baselines
- Keyboard accessible navigation across all 4 panes.
- Color is never the only status indicator.
- Initial screen load target: <= 2s for standard defect context.
- Interactive action response target: <= 500ms for local state transition feedback.

## Data Contract Dependencies
- Requires B6 object/link specs:
  - `docs/ontology/object_link_type_specs_b6_1.md`
- Requires B7 action schemas:
  - `docs/actions/action_schema_specs_b7_1.md`

## Non-Goals (B8-1)
- Not implementing UI code yet (B8-2/B8-3 scope).
- Not defining dashboard metrics detail (B8-3).

## Acceptance Checklist
- Single-screen zone model documented.
- Primary user flows for confirm/reject/attach evidence documented.
- Required filters/search behavior documented.
- Dependencies on ontology/actions explicitly linked.
