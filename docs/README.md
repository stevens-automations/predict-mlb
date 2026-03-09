# Documentation Index (Current)

This docs set is optimized for **current operations**, **readability**, and **future agent handoff**.

## Start Here

1. `docs/STATUS.md` — current implemented state and known gaps
2. `docs/PLAN.md` — prioritized remaining work and acceptance gates
3. `docs/decisions.md` — active open decisions for fast unblock
4. `docs/day0-launch-reliability-checklist.md` — go-live/rehearsal checks

## Operational Guides

- `docs/simulation-mode.md` — deterministic preseason replay mode
- `docs/detection-tuning-guide.md` — anomaly warning threshold tuning
- `docs/preseason-replay-explanation-schema.md` — explanation guardrails contract
- `docs/sqlite-cutover.md` — SQLite cutover summary
- `docs/sqlite-operating-runbook.md` — SQLite runtime operations

## Research / Strategy

- `docs/research/accuracy-improvement-memo.md` — ranked improvement options (no full model overhaul)

## Archive

- `docs/archive/2026-03/` — incremental checkpoint/progress/history docs retained for traceability.

---

## Documentation Rules (for future changes)

- Keep **one canonical doc per concern** (status, plan, runbook, schema).
- Avoid adding date-stamped progress docs in root `docs/`.
- Put temporary milestone reports in `docs/archive/<YYYY-MM>/`.
- Update `STATUS.md` + `PLAN.md` whenever major implementation changes land.
