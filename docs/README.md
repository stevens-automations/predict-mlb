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
- `docs/runbooks/historical-ingestion-runbook.md` — scaffold-phase historical ingestion runbook
- `docs/runbooks/training-architecture.md` — current training scaffold and execution entrypoints
- `docs/runbooks/model-optimization-plan.md` — canonical training roadmap, promotion gates, and first-run commands

## Research / Strategy

- `docs/research/historical-mlb-ingestion-architecture.md` — historical ingestion architecture + scaffold status
- `docs/research/accuracy-improvement-memo.md` — ranked improvement options (no full model overhaul)
- `docs/research/feature-contract-v1.md` — canonical feature contract for 2020-2025 backfill and daily inference

## Archive

- `docs/archive/2026-03/` — incremental checkpoint/progress/history docs retained for traceability.

---

## Documentation Rules (for future changes)

- Keep **one canonical doc per concern** (status, plan, runbook, schema).
- Put model-training roadmap, gates, and operator commands in `docs/runbooks/model-optimization-plan.md`.
- Avoid adding date-stamped progress docs in root `docs/`.
- Put temporary milestone reports in `docs/archive/<YYYY-MM>/`.
- Update `STATUS.md` + `PLAN.md` whenever major implementation changes land.
