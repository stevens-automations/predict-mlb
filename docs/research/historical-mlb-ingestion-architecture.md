# Historical MLB Ingestion & Storage Architecture (statsapi + SQLite)

Date: 2026-03-09

## Decision Snapshot (Current)

- Canonical historical store: `data/mlb_history.db` (SQLite)
- Backfill target scope: `2020–2025`
- Historical odds backfill: **disabled** (odds are forward-only, in-season capture)
- Data contracts: strict; degraded fallback required; no silent game skipping
- API strategy: minimize statsapi requests with bounded retries/backoff + request budget + checkpoint resume
- Incremental cadence v1: pre-game + post-game only
- Primary model metric v1: log loss

## Scaffold Delivered

### CLI
- `scripts/history_ingest.py`
  - `init-db`
  - `backfill` (safe stub)
  - `incremental` (safe stub)
  - `dq` (placeholder checks)

### Schema (DDL)
- `scripts/sql/history_schema.sql`
- Tables:
  - `games`
  - `game_team_stats`
  - `game_pitcher_context`
  - `feature_rows`
  - `labels`
  - `ingestion_runs`
  - `ingestion_checkpoints`
  - `dq_results`
  - `odds_snapshot` (**forward-only**)

### Reliability/Control Plane
- Run ledger and checkpoint plumbing in place.
- Idempotent upsert helpers scaffolded for checkpoints and `games`.
- Configurable request policy included in CLI/config surface:
  - timeout, max attempts, exponential backoff, jitter, per-run request budget.

## Contract Model (Operational Intent)

1. **Must-have fields missing**: do not silently skip; mark degraded path with reason codes.
2. **Optional fields missing**: allow processing with warnings and observability.
3. **Partition processing**: checkpointed and resumable.
4. **Safety-first rollout**: no automatic season-scale pulls from scaffold commands.

## What is intentionally deferred

- Full statsapi fetch loops for backfill/incremental.
- Full DQ implementation (currently placeholder entry only).
- Historical odds ingestion.

## Activation Path (after explicit approval)

1. Enable bounded real fetch loop in `backfill` by season/month partition.
2. Enable pre-game/post-game incremental logic.
3. Add full DQ checks (completeness/null/dup/freshness).
4. Materialize training data from `feature_rows + labels` and evaluate by log loss first.

## Runbooks

- Scaffold-phase safe commands: `docs/runbooks/historical-ingestion-runbook.md`
- Post-approval execution/verification sequence: `docs/runbooks/historical-ingestion-post-approval-plan.md`
