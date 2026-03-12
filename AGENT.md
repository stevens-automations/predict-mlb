# AGENT.md

Purpose: operating contract for agents working in `predict-mlb`.

## 1) Mission

- Primary mission: maintain and improve the historical rebuild, feature materialization, and offline training system.
- Current operating phase: post-promotion stabilization of the recovered canonical historical DB.
- Secondary/legacy scope: the older daily prediction / tweeting runtime remains in-repo, but it is not the primary architectural center for current work unless Steven explicitly redirects there.

## 2) Canonical System

- Canonical local DB: `data/mlb_history.db`
- Canonical schema: `scripts/sql/history_schema.sql`
- Canonical historical CLI: `scripts/history_ingest.py`
- Canonical stable training contract: `feature_rows(feature_version='v1')`
- Canonical training code: `train/`
- Canonical training CLIs: `scripts/training/train_lgbm.py`, `scripts/training/experiment_runner.py`, `scripts/training/run_when_ready.py`
- Canonical training configs: `configs/training/`
- Canonical local model artifact home: `artifacts/model_registry/`
- Demoted legacy homes: `legacy/` and `scripts/legacy_runtime/`

## 3) Hard Guardrails

- Do not treat `data/mlb_history.db` as disposable or scratch state.
- Prefer scratch DB paths when validating rebuild logic or testing mutating workflows.
- Mutating the canonical DB through `scripts/history_ingest.py` requires explicit opt-in via `--allow-canonical-writes`.
- Preserve train / inference parity for approved feature families.
- Do not introduce overlapping “temporary” docs when an existing canonical doc already owns that concern.
- Do not perform external write actions without explicit Steven approval.

## 4) Current Priorities

Execute work in this order unless Steven changes it:

1. Protect and simplify the canonical DB workflow.
2. Finish the smallest durable rebuild path / CLI.
3. Perform broader repo cleanup and retire one-off surfaces.
4. Keep root and canonical docs aligned with the promoted architecture.
5. Cut a clean checkpoint before renewed training execution.

## 5) Repo Ground Truth

- Historical rebuild/materialization lives around `scripts/history_ingest.py` and `scripts/sql/history_schema.sql`.
- The supported historical scope is seasons `2020-2025`.
- `feature_rows(v1)` is the approved stable baseline for training.
- `v2_phase1` exists in code/tests, but it is not the default baseline contract.
- Training and evaluation are script/module based, not notebook dependent.
- Older files such as `predict.py`, `server/`, and tweet/runtime storage paths are legacy or secondary unless the task explicitly concerns them.
- High-confidence legacy artifacts that do not belong on the active surface should be moved under `legacy/` or `scripts/legacy_runtime/` rather than deleted when traceability still matters.

## 6) Standard Commands

- Historical CLI help: `python3 scripts/history_ingest.py --help`
- Training config inspection: `python3 scripts/training/train_lgbm.py --config configs/training/baseline_lgbm.json --print-only`
- Baseline training run: `python3 scripts/training/train_lgbm.py --config configs/training/baseline_lgbm.json`
- Experiment suite: `python3 scripts/training/experiment_runner.py --config configs/training/experiment_suite.json`
- Tests: `python3 -m unittest discover -s tests -p 'test*.py' -v`

Use the project virtualenv where available.

## 7) Documentation Spine

One canonical file per concern:

- `README.md`: root repo orientation
- `docs/README.md`: docs map
- `docs/STATUS.md`: state
- `docs/PLAN.md`: ordered gates
- `docs/TODO.md`: short queue
- `docs/decisions.md`: locked/open decisions

If a note belongs in one of those files, update that file instead of creating a parallel status memo.

## 8) Working Style

- Make the smallest correct change that strengthens the canonical system.
- Prefer consolidating around existing entrypoints instead of adding new ones.
- When touching docs, remove ambiguity about what is canonical, what is legacy, and what is merely archival.
- When touching scripts, favor fewer durable surfaces and clearer ownership boundaries.
- State assumptions when they affect DB safety, rebuild semantics, or training validity.

## 9) Definition Of Done

- The requested change matches the current post-promotion architecture.
- Any touched guidance points to the canonical DB and canonical workflows.
- Validation was run where practical and reported honestly.
- No new overlapping source of truth was introduced.

## 10) Handoff Format

- Status: done / blocked / needs-review
- What changed:
- Evidence:
- Risks:
- Next actions:
