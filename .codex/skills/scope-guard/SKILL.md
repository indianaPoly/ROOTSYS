---
name: scope-guard
description: Prevent garbage diffs by enforcing scope boundaries and recommending PR splits or reverts.
---

## When to use
- User request is vague or broad (e.g., "cleanup", "refactor", "정리")
- Diff size grows beyond intent
- Unrelated files appear in changes
- Cosmetic-only changes detected

---

## Enforcement Rules
1) Unrelated changes:
- If unrelated files are modified, recommend:
  a) revert them, or
  b) split into a separate PR (preferred if they are valuable and requested)

2) Cosmetic noise:
- Remove formatting-only or style-only changes unless explicitly requested.
- Avoid mass import sorting / rename churn across the repo.

3) Risky changes:
- If shared/public APIs are changed:
  - require tests
  - require explicit migration notes
  - prefer backward-compatible changes

4) Minimal solution:
- Choose the smallest change that satisfies acceptance criteria.

---

## Output Requirements
- List violations (if any):
  - Unrelated files:
  - Cosmetic diffs:
  - API-breaking risk:
- Provide remediation plan:
  - Keep:
  - Revert:
  - Split into separate PR:
- Provide a proposed PR scope statement (1–3 bullets)
