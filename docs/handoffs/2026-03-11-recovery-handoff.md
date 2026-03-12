# Recovery Handoff

Date: 2026-03-11
Status: archived handoff note retained as a pointer only

This handoff has been collapsed into the canonical recovery docs:

- `docs/runbooks/recovery-plan-2026-03-11.md` — incident summary, damage scope, safe rebuild order, and acceptance gates
- `docs/runbooks/historical-ingestion-runbook.md` — canonical command reference and season-loop execution examples

Unique operating takeaway retained from the original handoff:

- Do not run destructive DB rebuilds.
- Treat `--allow-unsafe-pitcher-context` as a debugging-only escape hatch, not a canonical recovery tool.

If this file grows again, fold the content into one of the runbooks above instead of recreating a parallel source of truth.
