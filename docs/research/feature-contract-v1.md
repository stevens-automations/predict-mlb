# Feature Contract V1

Date: 2026-03-09
Scope: Daily MLB game winner prediction for seasons 2020-2025 historical backfill and daily inference, constrained to data that can be pulled reliably enough from MLB StatsAPI and stored in the current SQLite scaffold.

## Objective

Define the canonical pregame feature contract for predicting `did_home_win` for each MLB game. This contract is meant to replace ad hoc feature assembly from [`data.py`](/Users/openclaw/.openclaw/workspace/projects/predict-mlb/data.py) and the legacy notebook flow in [`mlb-predict.ipynb`](/Users/openclaw/.openclaw/workspace/projects/predict-mlb/mlb-predict.ipynb) with a point-in-time safe feature store backed by the current historical schema in [`scripts/sql/history_schema.sql`](/Users/openclaw/.openclaw/workspace/projects/predict-mlb/scripts/sql/history_schema.sql).

The current repo state matters:

- Legacy training used a 44-feature home/away row from [`data.py`](/Users/openclaw/.openclaw/workspace/projects/predict-mlb/data.py) and [`mlb-predict.ipynb`](/Users/openclaw/.openclaw/workspace/projects/predict-mlb/mlb-predict.ipynb).
- Historical ingestion in [`scripts/history_ingest.py`](/Users/openclaw/.openclaw/workspace/projects/predict-mlb/scripts/history_ingest.py) now materializes `games`, `labels`, `game_team_stats`, parity-safe `game_pitcher_context`, and `feature_rows(feature_version='v1')`; newer support tables for pitcher appearances and bullpen state are implemented separately for the next feature wave.
- Existing research in [`docs/research/model-data-feasibility-audit-2026-03-09.md`](/Users/openclaw/.openclaw/workspace/projects/predict-mlb/docs/research/model-data-feasibility-audit-2026-03-09.md) already concluded that legacy parity cannot be trusted unless we enforce as-of snapshots and leakage guardrails.

Canonical output of this contract:

- One row per game in `feature_rows`
- `feature_version = "v1"`
- `as_of_ts` reflects the prediction snapshot time
- `feature_payload_json` contains only features allowed by this contract

Label contract:

- Join to `labels.did_home_win`
- No feature may use target-game outcome, target-game boxscore, or any source updated after `as_of_ts`

## Prediction Timing Options

### Option A: Morning snapshot

Timing:

- Snapshot once daily around 09:00-10:00 ET

Pros:

- Operationally simple
- Cheap request volume
- Stable for tweet/reporting workflow already centered around the morning

Cons:

- Probable starters are less reliable
- Lineups are not posted
- Weather/injury context is incomplete
- More games fall back to team-only features

### Option B: Near-first-pitch snapshot

Timing:

- Snapshot each game 60-30 minutes before scheduled first pitch

Pros:

- Better starter confirmation
- Better injury/active-roster signal if available
- Better weather and postponement awareness
- Best train/inference parity for same-day inference

Cons:

- More orchestration complexity
- More request fanout across the day
- Harder to support a single morning publication artifact

### Recommendation

Use a dual-snapshot policy, but make `near_first_pitch` the primary modeling contract.

- Train the main model on `near_first_pitch` snapshots because probable starter availability is materially better.
- Keep a `morning` snapshot variant for operational fallback and for the existing morning prediction/tweet workflow.
- If a game never receives a near-first-pitch snapshot, inference may fall back to the morning row only if the row passes contract validation and the response marks the prediction as degraded.

Default contract recommendation:

- Canonical training/inference snapshot: `scheduled_datetime - 60 minutes`
- Fallback snapshot: same-day `09:00 ET`

## Feature Families

### Must-Have

These are required for V1 model training.

#### 1. Game metadata and context

Concrete fields:

- `game_id`
- `season`
- `game_date`
- `scheduled_datetime`
- `home_team_id`
- `away_team_id`
- `game_type`
- `doubleheader_code` if available from schedule payload
- `series_game_number` if available from schedule payload
- `venue_id` if available from schedule payload

Granularity:

- One row per game

As-of rule:

- From schedule data valid at snapshot time

Leakage guardrails:

- Do not use final status, final score, winning team, or any postgame field
- Postponed/suspended state may be used only if known at snapshot time

Likely source:

- Wrapper: `statsapi.schedule(...)`
- Raw endpoint family: `/schedule`

Reliability notes:

- High for schedule identity fields
- Medium for ancillary fields that may vary by game type or doubleheader handling

#### 2. Team season-to-date record strength

Concrete fields:

- `home_team_win_pct`
- `away_team_win_pct`
- `home_team_games_played`
- `away_team_games_played`
- `home_team_run_diff_per_game`
- `away_team_run_diff_per_game`

Granularity:

- Team as-of target game snapshot, then flattened into home/away game row

As-of rule:

- Derived only from games completed strictly before target game `scheduled_datetime`

Leakage guardrails:

- Do not trust same-day standings blindly if semantics are unclear
- Prefer local derivation from prior `games + labels`
- Same-day earlier games count only if they were already final before `as_of_ts`

Likely source:

- Primary: local derivation from `games` + `labels`
- Secondary comparison-only reference: `statsapi.standings_data(...)`

Reliability notes:

- High if derived locally
- Medium if derived from standings API because same-day cutoff semantics are ambiguous

#### 3. Recent team form, rolling 10 games

Concrete fields:

- `home_last_10_games`
- `away_last_10_games`
- `home_last_10_win_pct`
- `away_last_10_win_pct`
- `home_last_10_runs_for_avg`
- `away_last_10_runs_for_avg`
- `home_last_10_runs_against_avg`
- `away_last_10_runs_against_avg`
- `home_last_10_run_diff_avg`
- `away_last_10_run_diff_avg`
- `home_last_10_hits_avg`
- `away_last_10_hits_avg`
- `home_last_10_hits_allowed_avg`
- `away_last_10_hits_allowed_avg`
- `home_last_10_batting_ops_avg`
- `away_last_10_batting_ops_avg`
- `home_last_10_batting_obp_avg`
- `away_last_10_batting_obp_avg`
- `home_last_10_batting_avg_avg`
- `away_last_10_batting_avg_avg`
- `home_last_10_batting_rbi_avg`
- `away_last_10_batting_rbi_avg`
- `home_last_10_pitching_strikeouts_avg`
- `away_last_10_pitching_strikeouts_avg`

Granularity:

- Team rolling window over prior completed games

As-of rule:

- Use the most recent 10 completed games before `as_of_ts`
- If fewer than 10 exist, use available prior games and expose count field

Leakage guardrails:

- Never include target game
- Never use games starting before but ending after `as_of_ts`
- Prefer a game-count window over legacy "last 10 days" because it is easier to backfill consistently

Likely source:

- `statsapi.boxscore_data(gamePk)` for historical per-game team stats
- `statsapi.schedule(...)` to enumerate prior games
- Raw endpoint family: `/game/{gamePk}/boxscore`, `/schedule`

Reliability notes:

- High for completed-game team stats
- Backfill cost is significant if done naively one game at a time; cache boxscore-derived rows in `game_team_stats`

#### 4. Probable starter identity and season-to-date form

Concrete fields:

- `home_probable_pitcher_id`
- `away_probable_pitcher_id`
- `home_probable_pitcher_known`
- `away_probable_pitcher_known`
- `home_starter_season_era`
- `away_starter_season_era`
- `home_starter_season_whip`
- `away_starter_season_whip`
- `home_starter_season_avg_allowed`
- `away_starter_season_avg_allowed`
- `home_starter_season_runs_per9`
- `away_starter_season_runs_per9`
- `home_starter_season_strike_pct`
- `away_starter_season_strike_pct`
- `home_starter_season_win_pct`
- `away_starter_season_win_pct`
- `home_starter_career_era`
- `away_starter_career_era`
- `home_starter_recent_5_starts_era`
- `away_starter_recent_5_starts_era`
- `home_starter_recent_5_starts_whip`
- `away_starter_recent_5_starts_whip`

Granularity:

- Pregame probable starter snapshot by side

As-of rule:

- Starter identity must come from the snapshot schedule/feed payload at `as_of_ts`
- Season and recent-start stats must be computed only from starts completed before `as_of_ts`
- Career ERA may use all prior MLB appearances before `as_of_ts`

Leakage guardrails:

- Do not use legacy `player_stat_data(..., type="yearByYear")` season totals directly for historical training because they can reflect end-of-season values
- Recompute season-to-date locally from historical appearances or completed games
- If starter is unknown, set all starter stat fields to `null` and set `*_known = 0`

Likely source:

- Starter identity: `statsapi.schedule(game_id=...)` and likely live game/feed endpoints
- Historical completed pitcher lines: `statsapi.boxscore_data(gamePk)` for actual pitcher usage
- Optional player reference: `statsapi.lookup_player(...)`
- Wrapper names for splits may vary; raw endpoint family likely includes `/people/{id}/stats`

Reliability notes:

- Medium for probable starter identity in morning snapshots
- Better near first pitch
- Low confidence on using direct StatsAPI season stat endpoints for point-in-time backfill without local recomputation

#### 5. Home-field and rest context

Concrete fields:

- `home_is_home = 1`
- `away_is_home = 0`
- `home_days_since_last_game`
- `away_days_since_last_game`
- `home_was_home_last_game`
- `away_was_home_last_game`
- `home_doubleheader_today`
- `away_doubleheader_today`
- `home_travel_flag`
- `away_travel_flag`

Granularity:

- Team context as of target game

As-of rule:

- Derived from prior schedule and same-day schedule known at `as_of_ts`

Leakage guardrails:

- Same-day later games do not count
- If game 1 of a doubleheader is not final at `as_of_ts`, do not include its result in game 2 rolling features

Likely source:

- `statsapi.schedule(...)`

Reliability notes:

- High

### Optional

These are useful if pull cost is acceptable and the source proves stable.

#### 6. Team roster availability / injuries

Concrete fields:

- `home_injured_list_count`
- `away_injured_list_count`
- `home_pitchers_injured_count`
- `away_pitchers_injured_count`
- `home_catcher_injured_count`
- `away_catcher_injured_count`
- `home_lineup_core_missing_count`
- `away_lineup_core_missing_count`

Granularity:

- Team-level count features only, not free-text injury notes

As-of rule:

- Snapshot same day at inference time
- Historical backfill only if the endpoint has reliable, dated transaction status for 2020-2025

Leakage guardrails:

- No manual retrospective tagging
- If historical injury state cannot be reconstructed cleanly, do not backfill and do not train on it

Likely source:

- Possible sources: roster endpoints, transaction endpoints, injured list status fields
- Wrapper function names are less certain than schedule/boxscore

Reliability notes:

- Medium to low historically
- V1 contract treats injuries as optional and degradable
- Fallback is explicit null plus `source_contract_status = degraded_optional_missing`

#### 7. Weather and venue environment

Concrete fields:

- `temperature_f`
- `wind_speed_mph`
- `wind_direction_bucket`
- `roof_status`

Granularity:

- Game-level same-day snapshot

As-of rule:

- Use only if available from StatsAPI or an already-approved internal source at snapshot time

Leakage guardrails:

- No postgame observed weather in training if inference uses pregame forecasts

Likely source:

- Not currently wired in repo

Reliability notes:

- Not part of minimum viable pull set

### Future

These are explicitly not V1 inputs.

#### 8. Team top-5 leader features

Legacy examples from [`data.py`](/Users/openclaw/.openclaw/workspace/projects/predict-mlb/data.py):

- `home-top5-hr-avg`
- `home-top5-rbi-avg`
- `home-top5-batting-avg`
- `home-top5-stolenBases-avg`
- `home-top5-totalBases-avg`

Why future only:

- Legacy implementation used `statsapi.team_leader_data(...)` by season, which likely returns season-level totals rather than as-of-game totals
- That is high leakage risk for historical backfill

Condition to promote later:

- Only if we can reconstruct player-level team stats point-in-time from per-game data or prove the endpoint supports historical as-of snapshots

#### 9. Betting odds as model features

Policy:

- Not allowed in V1 model inputs
- May be used only for comparison, pricing analysis, tweet/explanation context, and calibration monitoring

Why:

- Repo policy already treats `odds_snapshot` as forward-only
- No historical odds backfill will be attempted

## Data Contract Table

Canonical storage target:

- Raw/support tables: `games`, `game_team_stats`, `game_pitcher_context`
- Canonical model row: `feature_rows(feature_version="v1")`

| Feature name | Source | Required? | Fallback behavior | Validation rule |
| --- | --- | --- | --- | --- |
| `game_id` | `games` from schedule | yes | fail row | integer > 0 and unique within `feature_version/as_of_ts` |
| `season` | `games` from schedule | yes | derive from `game_date`; else fail row | `2020 <= season <= 2026` |
| `game_date` | `games` from schedule | yes | fail row | ISO date |
| `scheduled_datetime` | `games` from schedule | yes | fail row | valid ISO timestamp |
| `home_team_id` | `games` from schedule | yes | fail row | integer != `away_team_id` |
| `away_team_id` | `games` from schedule | yes | fail row | integer != `home_team_id` |
| `game_type` | schedule | yes | fail row | one of `R/F/D/L/W`, unless explicitly excluded upstream |
| `home_team_win_pct` | derived from prior `games+labels` | yes | if zero prior games, null and keep row | between 0 and 1 |
| `away_team_win_pct` | derived from prior `games+labels` | yes | if zero prior games, null and keep row | between 0 and 1 |
| `home_team_games_played` | derived | yes | `0` | integer >= 0 |
| `away_team_games_played` | derived | yes | `0` | integer >= 0 |
| `home_team_run_diff_per_game` | derived | yes | `0.0` if zero prior games | finite number |
| `away_team_run_diff_per_game` | derived | yes | `0.0` if zero prior games | finite number |
| `home_last_10_games` | derived from prior completed games | yes | integer count of available games | integer between 0 and 10 |
| `away_last_10_games` | derived from prior completed games | yes | integer count of available games | integer between 0 and 10 |
| `home_last_10_win_pct` | derived from prior completed games | yes | null if `home_last_10_games=0` | between 0 and 1 or null |
| `away_last_10_win_pct` | derived from prior completed games | yes | null if `away_last_10_games=0` | between 0 and 1 or null |
| `home_last_10_runs_for_avg` | `game_team_stats` rolling agg | yes | null if no prior games | finite number or null |
| `away_last_10_runs_for_avg` | `game_team_stats` rolling agg | yes | null if no prior games | finite number or null |
| `home_last_10_runs_against_avg` | `game_team_stats` rolling agg | yes | null if no prior games | finite number or null |
| `away_last_10_runs_against_avg` | `game_team_stats` rolling agg | yes | null if no prior games | finite number or null |
| `home_last_10_run_diff_avg` | derived | yes | null if no prior games | finite number or null |
| `away_last_10_run_diff_avg` | derived | yes | null if no prior games | finite number or null |
| `home_last_10_hits_avg` | `game_team_stats` rolling agg | yes | null if no prior games | finite number or null |
| `away_last_10_hits_avg` | `game_team_stats` rolling agg | yes | null if no prior games | finite number or null |
| `home_last_10_hits_allowed_avg` | opponent side in `game_team_stats` rolling agg | yes | null if no prior games | finite number or null |
| `away_last_10_hits_allowed_avg` | opponent side in `game_team_stats` rolling agg | yes | null if no prior games | finite number or null |
| `home_last_10_batting_ops_avg` | `game_team_stats` rolling agg | yes | null if no prior games | finite number or null |
| `away_last_10_batting_ops_avg` | `game_team_stats` rolling agg | yes | null if no prior games | finite number or null |
| `home_last_10_batting_obp_avg` | `game_team_stats` rolling agg | yes | null if no prior games | finite number or null |
| `away_last_10_batting_obp_avg` | `game_team_stats` rolling agg | yes | null if no prior games | finite number or null |
| `home_last_10_batting_avg_avg` | `game_team_stats` rolling agg | yes | null if no prior games | finite number or null |
| `away_last_10_batting_avg_avg` | `game_team_stats` rolling agg | yes | null if no prior games | finite number or null |
| `home_last_10_batting_rbi_avg` | `game_team_stats` rolling agg | yes | null if no prior games | finite number or null |
| `away_last_10_batting_rbi_avg` | `game_team_stats` rolling agg | yes | null if no prior games | finite number or null |
| `home_last_10_pitching_strikeouts_avg` | `game_team_stats` rolling agg | yes | null if no prior games | finite number or null |
| `away_last_10_pitching_strikeouts_avg` | `game_team_stats` rolling agg | yes | null if no prior games | finite number or null |
| `home_probable_pitcher_id` | schedule/feed snapshot | yes | null plus `home_probable_pitcher_known=0` | positive integer or null |
| `away_probable_pitcher_id` | schedule/feed snapshot | yes | null plus `away_probable_pitcher_known=0` | positive integer or null |
| `home_probable_pitcher_known` | derived | yes | `0` | in `{0,1}` |
| `away_probable_pitcher_known` | derived | yes | `0` | in `{0,1}` |
| `home_starter_season_era` | locally derived pregame pitcher stats | yes | null if starter unknown or no history | finite non-negative or null |
| `away_starter_season_era` | locally derived pregame pitcher stats | yes | null if starter unknown or no history | finite non-negative or null |
| `home_starter_season_whip` | locally derived | yes | null if starter unknown or no history | finite non-negative or null |
| `away_starter_season_whip` | locally derived | yes | null if starter unknown or no history | finite non-negative or null |
| `home_starter_season_avg_allowed` | locally derived | yes | null if starter unknown or no history | finite non-negative or null |
| `away_starter_season_avg_allowed` | locally derived | yes | null if starter unknown or no history | finite non-negative or null |
| `home_starter_season_runs_per9` | locally derived | yes | null if starter unknown or no history | finite non-negative or null |
| `away_starter_season_runs_per9` | locally derived | yes | null if starter unknown or no history | finite non-negative or null |
| `home_starter_season_strike_pct` | locally derived | yes | null if starter unknown or no history | between 0 and 1 or 0 and 100, but one encoding only |
| `away_starter_season_strike_pct` | locally derived | yes | null if starter unknown or no history | between 0 and 1 or 0 and 100, but one encoding only |
| `home_starter_season_win_pct` | locally derived | yes | null if starter unknown or no decisions | between 0 and 1 or null |
| `away_starter_season_win_pct` | locally derived | yes | null if starter unknown or no decisions | between 0 and 1 or null |
| `home_starter_career_era` | locally derived or direct career stats if parity-safe | yes | null if starter unknown | finite non-negative or null |
| `away_starter_career_era` | locally derived or direct career stats if parity-safe | yes | null if starter unknown | finite non-negative or null |
| `home_starter_recent_5_starts_era` | locally derived | optional | null | finite non-negative or null |
| `away_starter_recent_5_starts_era` | locally derived | optional | null | finite non-negative or null |
| `home_starter_recent_5_starts_whip` | locally derived | optional | null | finite non-negative or null |
| `away_starter_recent_5_starts_whip` | locally derived | optional | null | finite non-negative or null |
| `home_days_since_last_game` | schedule-derived | yes | null if no prior games | integer >= 0 or null |
| `away_days_since_last_game` | schedule-derived | yes | null if no prior games | integer >= 0 or null |
| `home_was_home_last_game` | schedule-derived | yes | null if no prior games | in `{0,1}` or null |
| `away_was_home_last_game` | schedule-derived | yes | null if no prior games | in `{0,1}` or null |
| `home_doubleheader_today` | same-day schedule | yes | `0` | in `{0,1}` |
| `away_doubleheader_today` | same-day schedule | yes | `0` | in `{0,1}` |
| `home_travel_flag` | schedule-derived | optional | `0` | in `{0,1}` |
| `away_travel_flag` | schedule-derived | optional | `0` | in `{0,1}` |
| `home_injured_list_count` | roster/transactions snapshot | optional | null | integer >= 0 or null |
| `away_injured_list_count` | roster/transactions snapshot | optional | null | integer >= 0 or null |

## Train/Inference Parity Rules

1. Canonical source for model rows is `feature_rows`.
2. Training rows and inference rows must use the same feature names, null handling, and transforms.
3. All derived features must be recomputed from raw/support tables using the same code path for backfill and daily inference.
4. No direct use of end-of-season `team_leader_data` or `player_stat_data(... yearByYear ...)` season values in training rows.
5. Features unavailable at inference time must not appear in training.
6. Odds are excluded from model inputs in both training and inference.
7. If a game is inferred from the `morning` snapshot fallback, the row must still comply with the same schema, with missing starter-dependent fields left null rather than imputed from future information.
8. Normalization, if used, must be fit on training folds only. The legacy notebook fit scaling before split; that behavior is not allowed.
9. Evaluation splits must be time-aware. Random shuffling from the legacy notebook is not allowed for model selection.

## Backfill Plan: 2020-2025

### Phase 1: Complete schedule and labels

Status:

- Already partially present through [`scripts/history_ingest.py`](/Users/openclaw/.openclaw/workspace/projects/predict-mlb/scripts/history_ingest.py)

Action:

- Finish all seasons 2020-2025 in `games` and `labels`
- Use season-partition checkpoints already present in ingestion scaffold

### Phase 2: Backfill completed-game team stats

Action:

- For each final game in `games`, fetch boxscore once
- Write one `game_team_stats` row per side with:
  - `runs`
  - `hits`
  - `batting_avg`
  - `obp`
  - `slg`
  - `ops`
  - `strikeouts`
  - `walks`
- Also store enough pitcher-side fields to support rolling opponent stats if available

Why:

- This unlocks the last-10 feature family without repeated API calls at training time

### Phase 3: Backfill starter context

Action:

- For each final game, capture actual starter identity from boxscore/live data if possible
- Populate `game_pitcher_context` for home and away sides
- Build local pitcher appearance history to support season-to-date and recent-start aggregates

Why:

- This is the minimum safe path to replace leakage-prone legacy starter features

### Phase 4: Materialize canonical feature rows

Action:

- For each target game in 2020-2025, compute one or more as-of snapshots:
  - `morning`
  - `near_first_pitch` if reconstructable
- Store final JSON row in `feature_rows`

Recommendation for historical training set:

- Use one snapshot per game for V1, preferably `near_first_pitch` if probable starter reconstruction is good enough
- If historical near-first-pitch snapshots cannot be reconstructed reliably for older seasons, train the first parity-safe model on the `morning` contract and treat starter features as opportunistic

## Daily Incremental Plan

1. Early morning:
   - Pull today’s schedule
   - Upsert `games`
   - Build `morning` feature rows for all scheduled games
2. Near first pitch:
   - Refresh game schedule/feed for games within the next 90 minutes
   - Update probable starter ids if newly known
   - Build `near_first_pitch` feature rows
3. Postgame:
   - Refresh final statuses
   - Upsert `labels`
   - Fetch one final boxscore per completed game
   - Upsert `game_team_stats` and `game_pitcher_context`
4. Recovery:
   - Re-run any failed partitions by date using `ingestion_checkpoints`

## Request Minimization and Rate-Limit Strategy

Principle:

- Pull once, derive many times.

Rules:

1. Use `statsapi.schedule(...)` by season/date as the primary index, not per-team schedule fanout.
2. Fetch `boxscore_data(gamePk)` once per completed game and persist team stats immediately.
3. Do not compute rolling form by refetching old boxscores repeatedly.
4. Build team rolling features from local SQLite tables.
5. Build pitcher rolling/season features from local SQLite history after the first backfill pass.
6. For same-day inference, refresh only games whose start time is near and whose starter ids are still missing.
7. Keep request budgets partitioned by run type:
   - season backfill budget
   - same-day morning budget
   - near-first-pitch refresh budget
8. Preserve bounded retries and checkpointing already present in [`scripts/history_ingest.py`](/Users/openclaw/.openclaw/workspace/projects/predict-mlb/scripts/history_ingest.py).

Expected minimum external pull pattern:

- Historical schedule: one request window per season/date chunk
- Historical boxscore: one per completed game
- Daily inference: one schedule pull for the day plus targeted refreshes for unresolved starters

## Minimum Viable Pull Set

This is the smallest pull set worth implementing next.

### Raw pulls

1. `statsapi.schedule(start_date=..., end_date=...)`
   - For backfill of `games`
   - For daily game discovery
2. `statsapi.schedule(game_id=...)`
   - For point-in-time game snapshot fields and probable starters
3. `statsapi.boxscore_data(gamePk)`
   - For completed-game team batting/pitching stats
   - For actual pitcher usage and starter reconstruction where possible

### Minimum tables to populate

1. `games`
2. `labels`
3. `game_team_stats`
4. `game_pitcher_context`
5. `feature_rows`

### Minimum V1 features to materialize

- Game metadata
- Team season-to-date win pct and run diff per game
- Team rolling last-10-games win pct, runs for, runs against
- Team rolling last-10-games hits, OPS, OBP, AVG
- Probable starter known flag and ids
- Starter season ERA, WHIP, runs per 9, AVG allowed, win pct
- Days rest / doubleheader flags

Current implemented 2020 contract:

- `feature_rows(feature_version='v1')` is materialized from existing tables only.
- Pitcher season stats must come from `game_pitcher_context` rows with:
  - `season_stats_scope='season_to_date_prior_completed_games'`
  - `season_stats_leakage_risk=0`
- If there is no prior completed support for a team or probable starter, the corresponding numeric features remain `null` and availability flags stay explicit in the payload instead of backfilling from future-aware endpoints.

If this pull set is completed, ingestion work can start immediately without waiting on injuries, weather, or top-5 leader reconstruction.

## Model Training Plan

### Stage 1: Baseline

Goal:

- Establish a leakage-safe baseline using only game metadata, team season-to-date strength, recent team form, and home/rest context

Inputs:

- No starter features required
- No optional injury/weather features

Model:

- LightGBM binary classifier, similar family to current repo

Evaluation:

- Walk-forward by date or season blocks
- Primary metric: log loss
- Secondary: Brier score, accuracy, calibration

Acceptance target:

- Beat naive baselines:
  - home-team always wins
  - better team season win pct wins
  - recent-form-only heuristic

### Stage 2: Parity-safe starter uplift

Goal:

- Add probable-starter and starter-history features using only point-in-time safe reconstruction

Evaluation:

- Same folds as baseline
- Measure incremental uplift in log loss and Brier
- Track coverage rate for known probable starters

Acceptance target:

- Net improvement over Stage 1 without increasing degraded-row rate beyond an acceptable threshold

### Stage 3: Optional uplift

Goal:

- Evaluate injuries and any additional same-day context only if they can be backfilled with parity

Explicit non-goal:

- Historical odds backfill

### Evaluation Methodology

1. Use expanding-window or season-block validation.
2. Report per-fold log loss, Brier, accuracy, and calibration plot summary.
3. Track degraded prediction rates:
   - missing starter
   - missing rolling-form support
   - optional feature unavailable
4. Compare against closing or live odds only for monitoring, not training.
5. Keep a stable holdout season once enough backfill exists:
   - Suggested initial holdout: 2025

## Open Risks and Mitigation

### Risk 1: Historical probable starters are incomplete

Impact:

- Starter feature coverage may be uneven, especially for morning snapshots and older seasons

Mitigation:

- Keep `*_known` flags
- Train a baseline that does not require starter features
- Allow null starter fields instead of unsafe imputation

### Risk 2: Direct StatsAPI season stats are not point-in-time safe

Impact:

- Recreates the legacy leakage problem

Mitigation:

- Recompute season-to-date aggregates locally from prior completed games and appearances
- Treat direct season endpoints as comparison/debug only until proven safe

### Risk 3: Injury history is not reliably reconstructable

Impact:

- Optional availability features may fail parity

Mitigation:

- Keep injuries optional
- Do not train on them until historical backfill quality is demonstrated

### Risk 4: Boxscore backfill is request-heavy

Impact:

- Multi-season backfill could be slow or brittle

Mitigation:

- One-time persist to `game_team_stats`
- Use checkpoints and bounded retries
- Backfill by season partitions

### Risk 5: Doubleheaders and same-day sequencing can leak information

Impact:

- Game 2 features may accidentally include Game 1 results before they were known

Mitigation:

- Enforce `completed_at < as_of_ts` logic for prior-game inclusion
- Maintain explicit snapshot timestamps

### Risk 6: Legacy comparability expectations may be unrealistic

Impact:

- Raw accuracy may drop relative to old notebook results

Mitigation:

- Treat the old 44-feature workflow as inspiration, not truth
- Optimize for parity-safe log loss and calibration first

## Decision Summary

V1 should ship with:

- Canonical `feature_rows` snapshots
- Team strength and rolling-form features derived locally
- Probable starter features only when point-in-time safe
- No odds as model inputs
- Injuries optional with explicit null fallback

V1 should not ship with:

- Legacy top-5 leader features from season leader endpoints
- Historical odds backfill
- Any training feature that cannot be produced identically at inference time
