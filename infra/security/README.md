# security

Entry point for infra/security.

## Purpose
- Scope: local secret/config handling baseline and secure commit hygiene.
- Owner: repository maintainers (`@indianaPoly`).

## Contents
- `.env.example`: canonical local configuration template with placeholders only.
- `docs/runbooks/security_config_baseline_c4_3.md`: rules, prohibited patterns, and pre-push checklist.
- `.gitignore`: protections for `.env` and secret-bearing local files.

## Supported Today
- Placeholder-only env template and explicit ignore rules.
- Runbook-level guidance for local-only credentials and rotation expectations.

## Not Supported Yet
- Secret manager integration (Vault/SSM/GCP Secret Manager).
- Automated secret scanning policy in CI.

## Security Checklist
1. Copy config from template: `cp .env.example .env`
2. Confirm `.env` is untracked: `git status --short`
3. Ensure no real keys/secrets are in staged changes before push.
