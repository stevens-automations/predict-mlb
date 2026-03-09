# Historical Ingestion Runbook (2020 Parity-Safe Enrichment + V1 Feature Rows)

Last updated: 2026-03-09

## Purpose

This runbook covers bounded historical ingestion plus the current 2020 enrichment/materialization path into SQLite.
Scope includes schedule ingest, team stats backfill, parity-safe pitcher context, canonical `feature_rows(v1)`, and checkpoint/run-ledger metadata.

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
python scripts/history_ingest.py backfill-team-stats --season 2020
python scripts/history_ingest.py backfill-pitcher-context-2020
python scripts/history_ingest.py materialize-feature-rows --season 2020 --feature-version v1
python scripts/history_ingest.py dq --partition season=2024
```

### Expected behavior

- `init-db` creates/updates schema and run ledger row in `ingestion_runs`.
- `backfill` performs bounded `statsapi.schedule` pulls by season partition and upserts:
  - `games` rows for final/relevant MLB games
  - `labels` rows for final games (`did_home_win`, `run_differential`, `total_runs`)
- `incremental` performs bounded one-day `statsapi.schedule` ingest with the same game/label upserts.
- `backfill-team-stats` backfills `game_team_stats` for completed 2020 games from boxscores.
- `backfill-pitcher-context-2020` writes `game_pitcher_context` using probable starters from schedule plus season-to-date pitcher stats derived only from prior completed game boxscores. When live schedule/boxscore calls are unavailable, it falls back to existing stored probable starter identity and rewrites season metrics to explicit leakage-safe nulls instead of keeping leakage-prone aggregates. Provenance contract:
  - `season_stats_scope='season_to_date_prior_completed_games'`
  - `season_stats_leakage_risk=0`
  - when a probable starter is known but has no prior completed pitching data, season stat fields stay `NULL`
- `materialize-feature-rows` writes one canonical `feature_rows(feature_version='v1')` row per 2020 game using only prior completed team results plus the already-materialized pitcher context. Stable key:
  - `(game_id, feature_version, as_of_ts)`
  - `as_of_ts` is `scheduled_datetime` when available, else `game_dateT00:00:00Z`
  - reruns update the same row instead of creating duplicates, and stale snapshots for the same `(game_id, feature_version)` are deleted before insert
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
.venv/bin/python -m unittest discover -s tests -p 'test_validate_phase2_2020.py'
```

## Phase 2 QA Verification (2020-only)

`backfill --season 2020` only populates `games` and `labels`. Enrichment/materialization remains explicit and ordered.

Before validation, run the three 2020 jobs in order:

```bash
.venv/bin/python scripts/history_ingest.py --db data/mlb_history.db backfill-team-stats --season 2020
.venv/bin/python scripts/history_ingest.py --db data/mlb_history.db backfill-pitcher-context-2020
.venv/bin/python scripts/history_ingest.py --db data/mlb_history.db materialize-feature-rows --season 2020 --feature-version v1
```

Then run validation:

```bash
.venv/bin/python scripts/validate_phase2_2020.py \
  --db data/mlb_history.db \
  --season 2020 \
  --output docs/reports/phase2-validation-2020.md \
  --rerun-cmd ".venv/bin/python scripts/history_ingest.py --db data/mlb_history.db materialize-feature-rows --season 2020 --feature-version v1"
```

Interpretation guide:

- **Row coverage vs 2020 games**
  - PASS: exactly 2 rows/game in both `game_team_stats` and `game_pitcher_context`, plus exactly 1 `feature_rows(v1)` row/game.
  - FAIL: enrichment/materialization is incomplete for one or more canonical tables.
- **Pitcher provenance is parity-safe**
  - PASS: all 2020 pitcher rows have `season_stats_leakage_risk=0`.
  - PASS: rows with known probable starters use `season_stats_scope='season_to_date_prior_completed_games'`.
  - FAIL: any leakage-prone season scope/risk remains in 2020 backfill output.
- **Missingness per key feature field**
  - PASS/WARN only when rows exist and required columns are present.
  - FAIL when there are zero rows or required feature columns are absent from schema/data.
- **Idempotency checks after rerun**
  - PASS: no duplicate PK groups and table digests unchanged before/after rerun, including `feature_rows`.
  - FAIL: duplicate keys, post-rerun content drift, or rerun command failure.
- **Sanity ranges for major numeric fields**
  - PASS: no out-of-range values on non-null numeric fields.
  - FAIL: rows missing entirely, schema columns missing, or range violations.
- **Checkpoint/run observability consistency**
  - PASS: latest `ingestion_runs.note` counters for `season=2020` align with checkpoint `cursor_json`.
  - FAIL: missing run/checkpoint rows or counter mismatches.

Go/No-Go rule for moving to 2021:

- **GO** only if all validation checks are PASS.
- **NO-GO** if any check is FAIL; resolve blockers and rerun validator.

## Explicitly Out Of Scope

- No historical odds ingestion
