# Historical Ingestion Runbook (Bounded Schedule + Labels)

Last updated: 2026-03-09

## Purpose

This runbook covers bounded historical ingestion for schedule + labels into SQLite.
Scope includes game schedule pulls, idempotent game/label upserts, and checkpoint/run-ledger metadata.

## Canonical Decisions Encoded

- Canonical historical store: `data/mlb_history.db` (SQLite)
- Initial backfill target scope: seasons `2020-2025`
- Historical odds backfill: **disabled** (odds are forward-only during active season)
- Data contracts: strict with degraded fallback path (no silent game skipping)
- Incremental cadence v1: pre-game + post-game only
- Primary model metric v1: log loss
- Reliability defaults: bounded retry/backoff/jitter/timeouts/request budget + checkpoint resume

## Commands

From repo root:

```bash
python scripts/history_ingest.py init-db
python scripts/history_ingest.py backfill --season 2024
python scripts/history_ingest.py incremental --date 2026-03-09
python scripts/history_ingest.py dq --partition season=2024
```

### Expected behavior

- `init-db` creates/updates schema and run ledger row in `ingestion_runs`.
- `backfill` performs bounded `statsapi.schedule` pulls by season partition and upserts:
  - `games` rows for final/relevant MLB games
  - `labels` rows for final games (`did_home_win`, `run_differential`, `total_runs`)
- `incremental` performs bounded one-day `statsapi.schedule` ingest with the same game/label upserts.
- `dq` writes a placeholder DQ result row to prove run plumbing.
- Historical odds ingest remains disabled; `odds_snapshot` is not written by backfill/incremental.

## Verification Checklist

1. Schema tables exist:
   - `games`
   - `game_team_stats`
   - `game_pitcher_context`
   - `feature_rows`
   - `labels`
   - `ingestion_runs`
   - `ingestion_checkpoints`
   - `dq_results`
   - `odds_snapshot` (forward-only note)
2. `ingestion_runs` contains rows for each command invocation.
3. `ingestion_checkpoints` has entries for backfill/incremental partitions with periodic progress updates and final `success`/`failed`.
4. Re-running backfill/incremental remains idempotent (`games`/`labels` upsert, no duplicate primary keys).
5. `ingestion_runs.request_count` increments with each bounded statsapi request attempt.
5. Tests pass:

```bash
.venv/bin/python -m unittest discover -s tests -p 'test_history_ingest.py'
```

## Explicitly Out Of Scope

- No historical odds ingestion
