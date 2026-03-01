# kernel

Entry point for crates/kernel.

## Purpose
- Scope: action command handling interfaces for confirm/reject/add-evidence workflows.
- Owner: repository maintainers (`@indianaPoly`).

## Contents
- `crates/kernel/src/lib.rs`: action request/command/result models and MVP command handler.
- Includes validation/error handling for invalid action payloads across confirm/reject/add-evidence commands.
- Includes role policy checks (`reviewer`, `operator`, `admin`) before action execution.
- Includes append-only SQLite audit backend and query model (`AuditLogStore`, `SqliteAuditLogStore`, `AuditQuery`).
- `docs/architecture/mvp_roadmap_c_track.md`: C-track action/policy/audit roadmap.
