# Canonical Recovery Plan

Last updated: 2026-03-11

## Incident Summary

A fallback-mode rerun downgraded canonical starter context in `data/mlb_history.db`.

- Canonical DB: `data/mlb_history.db`
- Damaged tables:
  - `game_pitcher_context`
  - `feature_rows` for `feature_version in ('v1', 'v2_phase1')`
- Primary symptom:
  - probable-starter rows in `game_pitcher_context` now have near-universal null season stats after the fallback rerun
- Current impact:
  - `feature_rows(feature_version='v1')` is suspect where rebuilt from damaged starter rows
  - `feature_rows(feature_version='v2_phase1')` is not safe to treat as canonical until starter context is repaired

## Untouched / Likely Intact Layers

- `games`
- `labels`
- `game_team_stats`
- `game_pitcher_appearances`
- `team_bullpen_game_state`
- `team_bullpen_top_relievers`
- `player_handedness_dim`
- `game_lineup_snapshots`
- `team_lineup_game_state`
- `team_platoon_splits`
- `venue_dim`
- `game_weather_snapshots`

## Canonical Write Safety Rules

- Never promote a pitcher-context repair run without `audit-pitcher-context --season <year>`.
- Use `backfill-pitcher-context --repair-mode` for any canonical recovery run.
- A null-safe fallback write must not replace an existing richer `game_pitcher_context` row.
- Do not bypass the `v2_phase1` pitcher-context safety gate with `--allow-unsafe-pitcher-context` for canonical rebuilds.
- If `schedule_fallback_used=true` in a repair-style run, treat the run as no-go for canonical promotion.

## Safe Rebuild Order

Run season by season for `2020` through `2025`.

1. Audit current starter-context damage:
   - `python scripts/history_ingest.py --db data/mlb_history.db audit-pitcher-context --season <year>`
2. Rebuild starter context in repair mode:
   - `python scripts/history_ingest.py --db data/mlb_history.db backfill-pitcher-context --season <year> --repair-mode`
3. Re-audit starter context:
   - require `safe_for_canonical_write=true`
4. Rebuild baseline feature rows:
   - `python scripts/history_ingest.py --db data/mlb_history.db materialize-feature-rows --season <year> --feature-version v1`
5. Rebuild integrated feature rows only after starter-context audit is clean:
   - `python scripts/history_ingest.py --db data/mlb_history.db materialize-feature-rows --season <year> --feature-version v2_phase1`
6. Re-run support coverage audit:
   - `python scripts/history_ingest.py --db data/mlb_history.db audit-support-coverage --season <year> --feature-version v2_phase1`

## Acceptance Gates Before Training

- All seasons `2020-2025` present in canonical DB.
- `audit-pitcher-context` passes for every season:
  - `safe_for_canonical_write=true`
  - `rows_with_nonzero_leakage_risk=0`
- `feature_rows(v1)` rematerialized for all seasons after the repair.
- `feature_rows(v2_phase1)` rematerialized only after clean pitcher-context audit.
- Bullpen support remains complete.
- Lineup / platoon support remains complete enough for integrated use.
- Weather / venue support remains complete enough for integrated use.
- Coverage and degraded-path review are documented before the first training run.
