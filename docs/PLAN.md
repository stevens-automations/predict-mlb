# Remaining Work Plan (Reliability-First)

Last updated: 2026-03-09

## Priority 1 — Data Reliability Contract (Hard Gate)

Define and implement explicit requirements for every prediction input:
- Required fields and source ownership
- Freshness SLA
- Missingness threshold
- Fail-closed behavior for must-have features
- Degrade-gracefully behavior for optional features

### Acceptance criteria
- Machine-checkable preflight validates all required inputs before prediction run.
- Run aborts safely on must-have contract violation.

---

## Priority 2 — Retrain + Evaluate with Real Historical Data

Use `scripts/model_refresh.py` with production-like historical dataset.
- Compare baseline vs stronger candidate in walk-forward validation.
- Promote only if metrics improve within reliability and complexity limits.

### Acceptance criteria
- Evaluation report generated and stored under `docs/research/` or `docs/archive/`.
- Clear model promotion decision with rollback-ready baseline retained.

---

## Priority 3 — Implement Top Accuracy Quick Wins (No Overhaul)

Execute highest ROI/lowest risk items from:
- `docs/research/accuracy-improvement-memo.md`

### Acceptance criteria
- Changes are incremental, feature-flagged where appropriate.
- Backtest + replay results demonstrate measurable quality lift or are reverted.

---

## Priority 4 — Shadow / Replay Validation Gate

Run deterministic preseason rehearsal end-to-end:
- Simulation mode replay
- No-post dry run
- Reliability path verification (retry/circuit/fallback behavior)

### Acceptance criteria
- Test suite passes.
- Preflight + healthcheck pass.
- No critical guardrail violations.

---

## Priority 5 — Promotion Readiness

Only after priorities 1–4 pass:
- Freeze staging branch
- Final go-live checklist review
- Decide promotion to `main`

### Acceptance criteria
- Explicit go/no-go decision documented.
- Rollback path documented and tested.
