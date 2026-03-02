# ROOTSYS UI (English)

Next.js 16 runtime dashboard for artifact visibility.

## Purpose
- Display artifact status from smoke/complex executions
- Show per-artifact record counts, unique IDs, sources, and sample IDs

## Run
```bash
npm install
npm run dev
```

Open `http://localhost:3000`.

## Artifact paths consumed
- `/tmp/rootsys-smoke/*`
- `/tmp/rootsys-complex/*`

## Build checks
```bash
npm run typecheck
npm run build
```
