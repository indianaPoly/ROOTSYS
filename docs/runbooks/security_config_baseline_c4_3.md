# C4-3 Security and Config Baseline

This baseline defines minimum local configuration and secret-handling rules for MVP development.

## Configuration template

- Use `.env.example` as the canonical template.
- Create a local `.env` from the template:

```bash
cp .env.example .env
```

## Secret handling rules

- Never commit `.env` or environment-specific secret files.
- Keep only placeholders in `.env.example`.
- Rotate any credential immediately if accidentally exposed.

## Prohibited commit patterns

- API keys, OAuth secrets, DB passwords in source files, fixtures, or docs.
- Real production DSNs or tokens in command history snippets.
- Hardcoded secrets in interface JSON definitions.

## Git protection

- `.gitignore` includes:
  - `.env`
  - `.env.*`
  - allowlist exception for `.env.example`

## Operational checklist

1. Confirm `.env` is ignored by git.
2. Confirm `.env.example` contains placeholders only.
3. Run local bootstrap with non-production credentials only.
4. Before push, inspect staged changes for sensitive strings.
