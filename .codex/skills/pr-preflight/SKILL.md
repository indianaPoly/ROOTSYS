---
name: pr-preflight
description: Sync with base branch and run lint/type/test/build; only proceed to PR after a successful build.
---

## When to use
- User is about to open/update a PR
- User asks: "PR 올려도 돼?", "PR 준비해줘", "머지해도 돼?", "빌드 확인해줘", "preflight", "PR"

---

## Decision Rules (command selection)
1) Prefer repo-defined commands:
- package.json scripts (lint/typecheck/test/build)
- Makefile targets
- pyproject/ruff/mypy/pytest configuration
2) If ambiguous, use safe defaults and clearly state assumptions.

### JS/TS default candidates
- Install: `bun install` (or `pnpm install` / `npm ci`)
- Lint: `bun run lint`
- Typecheck: `bun run typecheck`
- Test: `bun run test`
- Build: `bun run build`

### Python default candidates
- Install: `uv sync` (or `poetry install` / `pip install -r requirements.txt`)
- Lint/Format check: `ruff check .` and/or `ruff format --check .`
- Typecheck: `mypy .` (only if configured)
- Test: `pytest`
- Build: `python -m build` (only if packaging is configured; otherwise skip and state why)

---

## Mandatory Procedure (stop on first failure)
1) Confirm target base branch (default: `main`).
2) Sync branch with base:
   - Merge or rebase base into working branch (follow repo/team policy).
3) Run checks in strict order:
   1) Lint / static analysis
   2) Typecheck / compile checks
   3) Tests
   4) Build
4) If Build fails:
   - identify root cause
   - apply minimal fix
   - rerun build
   - repeat until build succeeds
5) Only after Build succeeds:
   - draft PR title + body (copy/paste ready)

---

## Output Format (copy/paste)
### Preflight Results
- Base sync: ✅/❌ (merge/rebase, base=<branch>)
- Lint: ✅/❌ (`<command>`)
- Typecheck: ✅/❌ (`<command>` or "SKIPPED (not configured)")
- Tests: ✅/❌ (`<command>`)
- Build: ✅/❌ (`<command>` or "SKIPPED (not applicable)")

### Scope
- Intended change:
- Out of scope confirmed:
- Unrelated changes removed? (yes/no)

### Risk
- Level: Low / Medium / High
- Reason:
- Rollback plan:

### PR Draft
**Title:**
- <one-line title>

**Description:**
- What / Why
- What changed
- Verification evidence (commands + outcomes)
