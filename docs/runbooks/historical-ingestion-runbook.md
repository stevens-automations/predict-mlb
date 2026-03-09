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
- Successful backfill/incremental runs now write structured observability metrics to stdout, `ingestion_runs.note`, and checkpoint `cursor_json`.

### Observability counters

- `schedule_rows_fetched`: raw rows returned by `statsapi.schedule` for the partition.
- `relevant_rows_processed`: rows that survived relevance filtering and produced a game upsert attempt.
- `distinct_games_touched`: unique `game_id` values written during the run; this is the deduped game count for the run.
- `games_inserted` / `games_updated`: split of `distinct_games_touched` based on whether the `games` row existed before the run started on that partition.
- `labels_inserted` / `labels_updated`: split of distinct final-game labels written during the run.
- `final_distinct_counts_snapshot`: post-run distinct row counts for the affected partition.

Interpretation:

- `relevant_rows_processed` can be higher than `distinct_games_touched` when the schedule includes duplicate/rescheduled entries for the same `game_id`.
- `games_inserted + games_updated` should equal `distinct_games_touched`.
- `final_distinct_counts_snapshot` is the trustworthy post-run partition size; it may be lower than cumulative upsert counts across reruns because upserts update existing rows.

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
6. `ingestion_runs.note` / checkpoint `cursor_json` contain the structured observability counters above; use `final_distinct_counts_snapshot` for final partition row counts.
7. Tests pass:

```bash
.venv/bin/python -m unittest discover -s tests -p 'test_history_ingest.py'
```

## Explicitly Out Of Scope

- No historical odds ingestion
