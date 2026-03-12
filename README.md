# predict-mlb

`predict-mlb` is a historical MLB rebuild, feature materialization, and offline training repo.

The canonical local system is the SQLite historical database at `data/mlb_history.db`, built and maintained through `scripts/history_ingest.py`. Training reads from that database, primarily through `feature_rows + labels`, with `feature_rows(feature_version='v1')` as the current stable baseline contract.

## Current Phase

The historical DB has already been recovered and promoted. The repo is now in post-promotion stabilization:

1. Protect the canonical DB workflow.
2. Finish the durable rebuild path / CLI.
3. Clean up one-off scripts and artifacts.
4. Keep the canonical docs compact and current.
5. Cut a clean checkpoint before the next serious training pass.

## Start Here

Read in this order:

1. `README.md`
2. `docs/README.md`
3. `docs/STATUS.md`
4. `docs/PLAN.md`
5. `docs/runbooks/historical-ingestion-runbook.md`

## Canonical Architecture

- Historical schema: `scripts/sql/history_schema.sql`
- Historical ingest / rebuild entrypoint: `scripts/history_ingest.py`
- Preferred multi-season rebuild command: `python scripts/history_ingest.py rebuild-history ...`
- Canonical DB: `data/mlb_history.db`
- Stable training baseline: `feature_rows(feature_version='v1')`
- Training package: `train/`
- Training CLIs: `scripts/training/train_lgbm.py`, `scripts/training/experiment_runner.py`, `scripts/training/run_when_ready.py`
- Training configs: `configs/training/`
- Local model artifacts: `artifacts/model_registry/`
- Demoted legacy surfaces: `legacy/` and `scripts/legacy_runtime/`

The older runtime prediction / tweeting flow still exists in the repo, but it is no longer the primary architectural center or the canonical source of truth for historical rebuild and training work. Clear legacy artifacts have been moved under `legacy/` and `scripts/legacy_runtime/` so the active rebuild/training surface is easier to scan.

## Working Rules

- Treat `data/mlb_history.db` as protected canonical state, not a scratch database.
- Use `scripts/history_ingest.py` as the primary historical DB interface.
- Prefer `rebuild-history` for full rebuild orchestration and narrower subcommands for targeted repair/backfill work.
- Mutating the canonical DB requires explicit opt-in with `--allow-canonical-writes`.
- Prefer scratch DB paths for rebuild validation and workflow testing.
- Preserve train / inference parity for approved feature families.
- Keep `feature_rows(feature_version='v1')` as the approved training baseline unless a decision doc explicitly changes that.

## Canonical Docs

- `docs/README.md`: canonical docs map
- `docs/STATUS.md`: current repo state
- `docs/PLAN.md`: ordered pre-training gates
- `docs/TODO.md`: short execution queue
- `docs/decisions.md`: locked decisions and open decisions
- `docs/runbooks/historical-ingestion-runbook.md`: ingestion and rebuild commands
- `docs/runbooks/training-architecture.md`: training flow and entrypoints

## Cleanup Priorities

- Reduce durable script surface area around one rebuild path.
- Archive or fold superseded notes into canonical docs.
- Retire or clearly demote legacy / one-off paths that conflict with the historical rebuild-first architecture.
