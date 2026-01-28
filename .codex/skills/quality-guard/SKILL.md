---
name: quality-guard
description: Prevent common senior-level anti-patterns; enforce code quality, safety, and maintainability with actionable rules.
---

## When to use
- User asks for: "퀄리티 올려줘", "리팩토링", "코드 더 깔끔하게", "안티패턴 제거", "best practice 적용"
- Before writing any non-trivial change (2+ files, new API surface, new dependency)
- After producing a diff: run as a self-review gate

---

## Hard Blocks (must not ship)
### Type safety
- Do not bypass type systems with suppression (`as any`, `@ts-ignore`, `@ts-expect-error`) unless user explicitly requests a temporary workaround and you document why.
- Do not use implicit-any settings; keep strict typing where configured.
- Prefer `unknown` over `any` and narrow via type guards.

### Error handling
- No silent failures: never `catch` and ignore. If you catch, you must either handle, log with context, or rethrow.
- Avoid broad catch-all without intent: catch specific error types when possible.
- Do not match errors by string message; use error types/codes.

### Security / privacy
- Never log secrets/PII (tokens, passwords, cookies, auth headers, full payloads). Redact at source.
- Never return stack traces or internal paths in public API responses.

### Correctness
- No hidden side effects in functions that read like getters or pure utilities.
- No mutation of input parameters unless the name/contract clearly says so.

### Dependencies
- No dependency creep: do not add a new library for a one-off unless explicitly requested or clearly justified.
- Do not loosen dependency pinning/locks without reason.

---

## Strong Guidelines (should not ship without justification)
### API & contracts
- Keep public/shared APIs stable. If you must change them, update callers and add tests.
- Prefer small, composable functions with clear contracts (inputs/outputs/errors).

### Maintainability
- Avoid deep nesting (>4). Use guard clauses and early returns.
- Avoid long functions (>50 lines). Split by responsibility.
- Avoid copy-paste duplication. Extract shared logic at the smallest useful seam.
- Avoid magic numbers/strings. Use constants/enums.

### Naming
- Prefer domain-revealing names over abbreviations.
- Avoid overloaded names that mean different things across contexts.

### Observability (when relevant)
- Prefer structured logging with stable fields (e.g., request_id) when the codebase supports it.
- Use correct log levels (ERROR for actionable failures).

---

## Senior Quality Checklist (apply before you say "done")
1) Correctness
- Are edge cases covered (null/empty/timeout/retry)?
- Are invariants explicit (validation at boundaries)?

2) Interfaces
- Did you accidentally widen scope (extra exports/public methods)?
- Is behavior documented where it’s non-obvious ("why", not "what")?

3) Tests
- Is there at least one test that would have failed before this change and passes after?
- Are tests asserting behavior, not implementation details?

4) Failure modes
- If a dependency fails, what happens (timeouts, retries, fallbacks)?
- Are errors surfaced with enough context to debug?

5) Diff hygiene
- Any unrelated file changes? revert/split.
- Any formatting-only changes not requested? revert.

---

## Output Requirements
When invoking this skill, respond with:
- Violations found (Hard Blocks / Guidelines)
- Minimal remediation plan (1–5 bullets)
- What you intentionally did NOT change (scope guard)
