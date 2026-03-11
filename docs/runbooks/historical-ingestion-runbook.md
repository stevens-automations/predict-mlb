# Historical Ingestion Runbook (Season-Parameterized Parity-Safe Enrichment + V1 Feature Rows)

Last updated: 2026-03-10

## Purpose

This runbook covers bounded historical ingestion plus the current season-parameterized enrichment/materialization path into SQLite.
Scope includes schedule ingest, venue/weather support, team stats backfill, raw pitcher appearance backfill, bullpen support, lineup/handedness/platoon support, parity-safe pitcher context, canonical `feature_rows(v1)`, and checkpoint/run-ledger metadata.

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
python scripts/history_ingest.py backfill-team-stats --season 2021
python scripts/history_ingest.py backfill-pitcher-appearances --season 2021
python scripts/history_ingest.py backfill-bullpen-support --season 2021 --top-n-values 3,5
python scripts/history_ingest.py backfill-lineup-support --season 2021
python scripts/history_ingest.py backfill-pitcher-context --season 2021
python scripts/history_ingest.py sync-venues --season 2021
python scripts/history_ingest.py backfill-game-weather --season 2021
python scripts/history_ingest.py update-lineup-support --date 2026-03-10
python scripts/history_ingest.py update-game-weather-forecasts --date 2026-03-10 --as-of-ts 2026-03-10T15:00:00Z
python scripts/history_ingest.py materialize-feature-rows --season 2021 --feature-version v1
python scripts/history_ingest.py materialize-feature-rows --season 2021 --feature-version v2_phase1
python scripts/history_ingest.py dq --partition season=2024
```

### Expected behavior

- `init-db` creates/updates schema and run ledger row in `ingestion_runs`.
- `backfill` performs bounded `statsapi.schedule` pulls by season partition and upserts:
  - `games` rows for final/relevant MLB games
  - `labels` rows for final games (`did_home_win`, `run_differential`, `total_runs`)
- `incremental` performs bounded one-day `statsapi.schedule` ingest with the same game/label upserts.
  - additive schedule extensions: `games.venue_id` and `games.day_night` are populated when present in the feed row
- `sync-venues` populates the durable `venue_dim` from `games.venue_id` values already stored locally. It fetches venue metadata from the MLB Stats API venue endpoint and stores coordinates, timezone, and first-pass roof/weather-exposure context.
- `backfill-game-weather` writes one `game_weather_snapshots` row per completed game-season target using the Open-Meteo Archive API as the canonical numeric source.
  - first-pass historical policy: one `snapshot_type='observed_archive'` row aligned to the nearest hourly point to scheduled first pitch
  - `as_of_ts` is set to `games.scheduled_datetime` so the historical observed-vs-live forecast mismatch remains explicit in storage
- `update-game-weather-forecasts --date <YYYY-MM-DD> --as-of-ts <UTC>` writes prediction-time `snapshot_type='forecast'` rows for not-yet-started games on the target date using the Open-Meteo Forecast API.
  - runtime cutoff policy: games with `scheduled_datetime <= as_of_ts` are skipped so the stored snapshot remains pregame by construction
  - first-pass source path: hourly forecast value nearest first pitch, using the same hourly field family as archive backfill
- `backfill-team-stats` backfills `game_team_stats` for completed games in the selected season (`2020-2025`) from boxscores.
- `backfill-pitcher-appearances` backfills canonical per-pitcher completed-game rows into `game_pitcher_appearances` using one bounded boxscore fetch per game. Reruns are idempotent on `(game_id, pitcher_id)` and preserve checkpoint progress by season.
- `backfill-bullpen-support` derives `team_bullpen_game_state` and `team_bullpen_top_relievers` from already-stored `game_pitcher_appearances` plus prior game order only. It does not call player season endpoints and keeps the top-N selection rule inspectable via `ranking_method` and `selected_pitcher_ids_json`.
  - First pass caveat: workload windows are calendar-day based and do not yet add a separate same-day/doubleheader stress feature.
- `backfill-lineup-support` fetches one game boxscore payload per target game and fills four additive tables:
  - `player_handedness_dim`
  - `game_lineup_snapshots`
  - `team_lineup_game_state`
  - `team_platoon_splits`
  Historical lineup snapshots use explicit `snapshot_type` / `lineup_status` tagging. Completed-game backfill currently stores lineup rows as `fallback` snapshots because exact one-hour historical announced-lineup parity is not guaranteed.
- `update-lineup-support --date <YYYY-MM-DD>` is the same-day path for the prediction-time contract. It captures whatever lineup state exists for that date, updates handedness for seen players, and rebuilds `team_lineup_game_state` / `team_platoon_splits` for only the target games using prior completed local history.
- First-pass lineup quality caveat: richer player-offense support is intentionally deferred. `team_lineup_game_state.lineup_quality_mean` / `top3_lineup_quality_mean` / `top5_lineup_quality_mean` remain null for now, while `lineup_quality_metric` explicitly records either:
  - `handedness_affinity_proxy_v1` when only handedness-based lineup-vs-hand affinity could be derived
  - `unavailable__player_offense_support_not_built` when no usable lineup quality proxy exists
- `team_platoon_splits` is leakage-safe and local-history-only. Because the current support path does not yet store full team plate appearances directly, `plate_appearances`, `strikeout_rate`, and `walk_rate` are derived from a documented team-stat proxy built from stored `game_team_stats`.
- `backfill-pitcher-context --season <year>` writes `game_pitcher_context` using probable starters from schedule plus season-to-date pitcher stats derived only from prior completed game boxscores. `backfill-pitcher-context-2020` remains as a legacy alias for the 2020 path. When live schedule/boxscore calls are unavailable, it falls back to existing stored probable starter identity and rewrites season metrics to explicit leakage-safe nulls instead of keeping leakage-prone aggregates. Provenance contract:
  - `season_stats_scope='season_to_date_prior_completed_games'`
  - `season_stats_leakage_risk=0`
  - when a probable starter is known but has no prior completed pitching data, season stat fields stay `NULL`
- `materialize-feature-rows` writes one canonical `feature_rows(feature_version='v1')` row per selected-season game using only prior completed team results plus the already-materialized pitcher context. Stable key:
- `materialize-feature-rows --feature-version v2_phase1` reuses the same `v1` team/starter spine and adds the first integrated support blocks:
  - bullpen aggregate state + top-3 reliever support
  - lineup structure + platoon fallback keyed by opposing starter hand
  - coarse venue/weather context with explicit availability flags
  Training on `v2_phase1` remains gated on the separate validation/readiness review.
- `audit-support-coverage` reports the exact residual support gaps by season, including missing weather/lineup game IDs and whether the selected integrated `feature_version` has been materialized yet.
- `materialize-feature-rows` writes one canonical `feature_rows(feature_version='v1')` row per selected-season game using only prior completed team results plus the already-materialized pitcher context. Stable key:
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
   - `venue_dim`
   - `game_weather_snapshots`
   - `game_team_stats`
   - `game_pitcher_context`
   - `game_pitcher_appearances`
   - `team_bullpen_game_state`
   - `team_bullpen_top_relievers`
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
7. Lineup/platoon support tables exist and can be inspected directly:
   - `player_handedness_dim`
   - `game_lineup_snapshots`
   - `team_lineup_game_state`
   - `team_platoon_splits`
8. Tests pass:

```bash
.venv/bin/python -m unittest discover -s tests -p 'test_history_ingest.py'
.venv/bin/python -m unittest discover -s tests -p 'test_validate_phase2_2020.py'
```

## Weather / Venue Notes

- Canonical weather numeric source:
  - historical: Open-Meteo Archive API
  - live inference-time refresh: Open-Meteo Forecast API
- `precipitation_probability` is forecast-only in Open-Meteo practice for this pipeline:
  - forecast rows store the API-native `0-100` percent value
  - archive rows should be expected to leave `precipitation_probability` null and rely on `precipitation_mm` instead
- First-pass integrated feature rows should not require or derive a probability-based precipitation feature from archive history:
  - keep `precipitation_probability` as forecast-only snapshot storage
  - use `precipitation_mm` plus snapshot/source flags in the first-pass weather contract
- MLB Stats API weather text remains audit-only and is currently stored as nullable breadcrumb fields on `game_weather_snapshots`; it is not the canonical numeric weather source.
- First-pass parity stance is explicit:
  - historical rows are `observed_archive`
  - live rows are `forecast`
  - downstream feature selection must respect `snapshot_type` and `as_of_ts`
- Weather alignment rule:
  - convert `games.scheduled_datetime` into the venue timezone from `venue_dim`
  - request hourly weather over a local date window around first pitch
  - select the nearest hourly point within the bounded alignment window
  - store `hour_offset_from_first_pitch` for auditability

## Phase 2 QA Verification

`backfill --season <year>` only populates `games` and `labels`. Enrichment/materialization remains explicit and ordered.

Before validation, run the three season-specific jobs in order. Example for 2021:

```bash
.venv/bin/python scripts/history_ingest.py --db data/mlb_history.db backfill-team-stats --season 2021
.venv/bin/python scripts/history_ingest.py --db data/mlb_history.db backfill-pitcher-appearances --season 2021
.venv/bin/python scripts/history_ingest.py --db data/mlb_history.db backfill-bullpen-support --season 2021 --top-n-values 3,5
.venv/bin/python scripts/history_ingest.py --db data/mlb_history.db backfill-lineup-support --season 2021
.venv/bin/python scripts/history_ingest.py --db data/mlb_history.db backfill-pitcher-context --season 2021
.venv/bin/python scripts/history_ingest.py --db data/mlb_history.db materialize-feature-rows --season 2021 --feature-version v1
.venv/bin/python scripts/history_ingest.py --db data/mlb_history.db audit-support-coverage --feature-version v2_phase1
```

Then run validation:

```bash
.venv/bin/python scripts/validate_phase2_2020.py \
  --db data/mlb_history.db \
  --season 2021 \
  --output docs/reports/phase2-validation-2021.md \
  --rerun-cmd ".venv/bin/python scripts/history_ingest.py --db data/mlb_history.db materialize-feature-rows --season 2021 --feature-version v1"
```

Interpretation guide:

- **Row coverage vs `<season>` games**
  - PASS: exactly 2 rows/game in both `game_team_stats` and `game_pitcher_context`, plus exactly 1 `feature_rows(v1)` row/game.
  - FAIL: enrichment/materialization is incomplete for one or more canonical tables.
- **Pitcher provenance is parity-safe**
  - PASS: all target-season pitcher rows have `season_stats_leakage_risk=0`.
  - PASS: rows with known probable starters use `season_stats_scope='season_to_date_prior_completed_games'`.
  - FAIL: any leakage-prone season scope/risk remains in the target-season backfill output.
- **Missingness per key feature field**
  - PASS/WARN only when rows exist and required columns are present.
  - FAIL when there are zero rows or required feature columns are absent from schema/data.
- **Idempotency checks after rerun**
  - PASS: no duplicate PK groups and table digests unchanged before/after rerun, including `feature_rows`.
  - FAIL: duplicate keys, post-rerun content drift, or rerun command failure.
- **Sanity ranges for major numeric fields**
  - PASS: no out-of-range values on non-null numeric fields. Pitcher ceilings intentionally allow rare tiny-sample season-to-date spikes (for example, one disastrous `0.2 IP` outing can legitimately push `season_era`/`season_runs_per_9` to `135.0` and `season_whip` to `21.0`).
  - FAIL: rows missing entirely, schema columns missing, or range violations.
- **Checkpoint/run observability consistency**
  - PASS: latest `ingestion_runs.note` counters for `season=<year>` align with checkpoint `cursor_json`.
  - FAIL: missing run/checkpoint rows or counter mismatches.

Go/No-Go rule for moving to 2021:

- **GO** only if all validation checks are PASS.
- **NO-GO** if any check is FAIL; resolve blockers and rerun validator.

## Explicitly Out Of Scope

- No historical odds ingestion
