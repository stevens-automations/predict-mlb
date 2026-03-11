# Predict-MLB schema expansion plan for bullpen, lineup, and weather

Date: 2026-03-10
Repo: `projects/predict-mlb`

## Executive summary

The current database and pipeline are already organized in a way that supports a clean additive expansion:

- `games` is the canonical game spine
- `labels` holds outcomes
- `game_team_stats` and `game_pitcher_context` are support tables for pregame feature derivation
- `feature_rows` is the canonical model-facing store
- `scripts/history_ingest.py` already follows the right general pattern: raw/support tables first, then materialize `feature_rows`

That means the next dataset upgrade should **not** rewrite the current schema. The practical move is to add a small set of new raw/support tables keyed off `game_id`, `team_id`, `player_id`, and `as_of_ts`, then extend feature materialization to read from them.

Strong recommendation:

- **Keep existing core tables unchanged wherever possible**
- **Add new support tables instead of widening `feature_rows` schema**
- **Keep `feature_rows` as JSON payload output only**
- **Use additive pregame snapshots for lineup and weather**
- **Use prior-completed-game derivation for bullpen and team/platoon quality**

Recommended roadmap:

1. Phase 1A: bullpen raw + derived support
2. Phase 1B: lineup/platoon raw + derived support
3. Phase 1C: extend feature materialization to emit a new feature version using both
4. Phase 2: weather/venue raw + derived support
5. Phase 2B: extend feature materialization again

---

## What exists today

## Current schema and pipeline shape

Current tables in `data/mlb_history.db`:

- `games`
- `game_team_stats`
- `game_pitcher_context`
- `feature_rows`
- `labels`
- `ingestion_runs`
- `ingestion_checkpoints`
- `dq_results`
- `odds_snapshot`

Observed row counts:

- `games`: 13,309
- `game_team_stats`: 26,614
- `game_pitcher_context`: 26,618
- `feature_rows`: 13,309
- `labels`: 13,307

Current materialization pattern in `scripts/history_ingest.py`:

1. Ingest schedule into `games`
2. Ingest labels into `labels`
3. Backfill completed-game team boxscore stats into `game_team_stats`
4. Build starter context into `game_pitcher_context`
5. Materialize model-ready JSON rows into `feature_rows`

Current feature materialization is stateful and local-first:

- team strength and rolling form are derived from prior local rows
- starter stats are built from prior completed games only
- `feature_rows` stores the canonical payload JSON
- training reads only from `feature_rows` + `labels`

This is exactly the pattern to preserve.

---

## Design principles for the expansion

1. **Additive over invasive**
   - Add new tables; do not rework existing core tables unless a tiny metadata extension is clearly worth it.

2. **Support-table-first design**
   - Raw/support tables should capture reusable baseball state.
   - `feature_rows` should stay the model contract output, not become the raw data warehouse.

3. **Pregame realism**
   - Bullpen/team/platoon quality must be derived from prior completed games only.
   - Announced lineup features must come from lineup snapshots captured at or before the target as-of time.
   - Weather should be tied to a prediction-time snapshot, not final observed game weather if inference will use forecast data.

4. **Reuse current keys and join logic**
   - `game_id` remains the main game-level join key.
   - `team_id` and `player_id` should be used consistently in new support tables.
   - `as_of_ts` should appear in snapshot-style tables.

5. **Allow multiple feature versions**
   - Existing `v1` rows stay intact.
   - New enrichment should land in a new feature version, e.g. `v2_phase1` then `v2_phase2` or a single `v2` once stable.

---

## Recommended schema expansion

## 1) Bullpen raw/support data

### New raw table: `game_pitcher_appearances`

Purpose:
Canonical per-pitcher per-game appearance history. This is the missing raw layer that makes bullpen quality and fatigue derivable without repeated boxscore refetching.

Recommended columns:

- `game_id INTEGER NOT NULL`
- `team_id INTEGER NOT NULL`
- `side TEXT NOT NULL CHECK(side IN ('home','away'))`
- `pitcher_id INTEGER NOT NULL`
- `pitcher_name TEXT`
- `appearance_order INTEGER`
- `is_starter INTEGER NOT NULL DEFAULT 0 CHECK(is_starter IN (0,1))`
- `is_reliever INTEGER NOT NULL DEFAULT 1 CHECK(is_reliever IN (0,1))`
- `outs_recorded INTEGER`
- `innings_pitched REAL`
- `batters_faced INTEGER`
- `pitches INTEGER`
- `strikes INTEGER`
- `hits INTEGER`
- `walks INTEGER`
- `strikeouts INTEGER`
- `runs INTEGER`
- `earned_runs INTEGER`
- `home_runs INTEGER`
- `holds INTEGER`
- `save_flag INTEGER`
- `blown_save_flag INTEGER`
- `inherited_runners INTEGER`
- `inherited_runners_scored INTEGER`
- `source_updated_at TEXT`
- `ingested_at TEXT NOT NULL DEFAULT (datetime('now'))`

Recommended key:

- `PRIMARY KEY (game_id, pitcher_id)`

Recommended relationships:

- `FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE`

Why this table exists:

- Current `game_pitcher_context` is starter-focused and game-side-level.
- Bullpen modeling needs **every pitcher appearance**, not just probable starters.
- This table also creates a reusable base for future starter recent-form logic, reliever role inference, and more advanced pitching features.

### New derived/support table: `team_bullpen_game_state`

Purpose:
Store per-team, per-target-game bullpen aggregates computed **as of that target game** from prior completed appearances only.

Recommended columns:

- `game_id INTEGER NOT NULL`
- `team_id INTEGER NOT NULL`
- `side TEXT NOT NULL CHECK(side IN ('home','away'))`
- `as_of_ts TEXT NOT NULL`
- `season_games_in_sample INTEGER`
- `bullpen_appearances_season INTEGER`
- `bullpen_outs_season INTEGER`
- `bullpen_era_season REAL`
- `bullpen_whip_season REAL`
- `bullpen_runs_per_9_season REAL`
- `bullpen_k_rate_season REAL`
- `bullpen_bb_rate_season REAL`
- `bullpen_k_minus_bb_rate_season REAL`
- `bullpen_hr_rate_season REAL`
- `bullpen_outs_last1d INTEGER`
- `bullpen_outs_last3d INTEGER`
- `bullpen_outs_last5d INTEGER`
- `bullpen_outs_last7d INTEGER`
- `bullpen_pitches_last1d INTEGER`
- `bullpen_pitches_last3d INTEGER`
- `bullpen_pitches_last5d INTEGER`
- `bullpen_appearances_last3d INTEGER`
- `bullpen_appearances_last5d INTEGER`
- `relievers_used_yesterday_count INTEGER`
- `relievers_used_last3d_count INTEGER`
- `relievers_back_to_back_count INTEGER`
- `relievers_2_of_last3_count INTEGER`
- `high_usage_relievers_last3d_count INTEGER`
- `freshness_score REAL`
- `source_updated_at TEXT`
- `ingested_at TEXT NOT NULL DEFAULT (datetime('now'))`

Recommended key:

- `PRIMARY KEY (game_id, side, as_of_ts)`

Why this table exists:

- Materializing bullpen state once per game avoids recomputing long rolling windows every time features are rebuilt.
- It matches the current repo pattern: support table first, then `feature_rows`.

### New derived/support table: `team_bullpen_top_relievers`

Purpose:
Store top-N reliever quality/fatigue summaries for each team as of each game. This directly satisfies the locked decision that first pass must include **both** aggregate bullpen state and top-N reliever summaries.

Recommended columns:

- `game_id INTEGER NOT NULL`
- `team_id INTEGER NOT NULL`
- `side TEXT NOT NULL CHECK(side IN ('home','away'))`
- `as_of_ts TEXT NOT NULL`
- `ranking_method TEXT NOT NULL`
- `top_n INTEGER NOT NULL`
- `n_available INTEGER NOT NULL`
- `topn_era_season REAL`
- `topn_whip_season REAL`
- `topn_k_rate_season REAL`
- `topn_bb_rate_season REAL`
- `topn_k_minus_bb_rate_season REAL`
- `topn_outs_last3d INTEGER`
- `topn_pitches_last3d INTEGER`
- `topn_appearances_last3d INTEGER`
- `topn_back_to_back_count INTEGER`
- `topn_freshness_score REAL`
- `quality_dropoff_vs_team REAL`
- `source_updated_at TEXT`
- `ingested_at TEXT NOT NULL DEFAULT (datetime('now'))`

Recommended key:

- `PRIMARY KEY (game_id, side, as_of_ts, top_n)`

Recommended default first pass:

- materialize rows for `top_n IN (3,5)`
- use a simple, stable ranking method like prior-season-to-date reliever quality with usage minimums

Why this table exists:

- It keeps top-N logic explicit and versionable.
- It avoids shoving many reliever-level intermediate calculations into JSON too early.
- It preserves future flexibility if the reliever ranking method changes.

---

## 2) Lineup / handedness raw/support data

### New raw table: `game_lineup_snapshots`

Purpose:
Store the announced lineup for a game at a specific snapshot time. This is the canonical raw table for announced-lineup-first-pass features.

Recommended columns:

- `game_id INTEGER NOT NULL`
- `team_id INTEGER NOT NULL`
- `side TEXT NOT NULL CHECK(side IN ('home','away'))`
- `as_of_ts TEXT NOT NULL`
- `snapshot_type TEXT NOT NULL`  
  Suggested values: `announced`, `confirmed`, `fallback`
- `lineup_status TEXT`  
  Suggested values: `full`, `partial`, `missing`
- `player_id INTEGER NOT NULL`
- `player_name TEXT`
- `batting_order INTEGER`
- `position_code TEXT`
- `bat_side TEXT`  
  Suggested values: `L`, `R`, `S`
- `pitch_hand TEXT`
- `source_updated_at TEXT`
- `ingested_at TEXT NOT NULL DEFAULT (datetime('now'))`

Recommended key:

- `PRIMARY KEY (game_id, side, as_of_ts, batting_order)`

Why this table exists:

- Lineup data is snapshot data, not stable game metadata.
- The same game may have multiple pregame snapshots; the model should use the one tied to the target `as_of_ts`.
- Keeping player rows rather than only a pre-aggregated summary preserves future flexibility.

### New support table: `player_handedness_dim`

Purpose:
Provide stable handedness lookup for players so lineup and matchup derivation does not depend on re-reading raw feed payloads.

Recommended columns:

- `player_id INTEGER PRIMARY KEY`
- `player_name TEXT`
- `bat_side TEXT`
- `pitch_hand TEXT`
- `primary_position_code TEXT`
- `source_updated_at TEXT`
- `ingested_at TEXT NOT NULL DEFAULT (datetime('now'))`

Why this table exists:

- Handedness is reused across lineup, platoon, and future roster logic.
- It keeps `game_lineup_snapshots` simpler and makes backfill easier when a player’s hand data has already been observed.

### New derived/support table: `team_lineup_game_state`

Purpose:
Store game-level lineup structure and lineup quality summaries as of the target game.

Recommended columns:

- `game_id INTEGER NOT NULL`
- `team_id INTEGER NOT NULL`
- `side TEXT NOT NULL CHECK(side IN ('home','away'))`
- `as_of_ts TEXT NOT NULL`
- `snapshot_type TEXT NOT NULL`
- `announced_lineup_count INTEGER`
- `lineup_known_flag INTEGER NOT NULL DEFAULT 0 CHECK(lineup_known_flag IN (0,1))`
- `lineup_l_count INTEGER`
- `lineup_r_count INTEGER`
- `lineup_s_count INTEGER`
- `top3_l_count INTEGER`
- `top3_r_count INTEGER`
- `top3_s_count INTEGER`
- `top5_l_count INTEGER`
- `top5_r_count INTEGER`
- `top5_s_count INTEGER`
- `lineup_lefty_pa_share_proxy REAL`
- `lineup_righty_pa_share_proxy REAL`
- `lineup_switch_pa_share_proxy REAL`
- `lineup_balance_score REAL`
- `lineup_quality_metric TEXT`
- `lineup_quality_mean REAL`
- `top3_lineup_quality_mean REAL`
- `top5_lineup_quality_mean REAL`
- `lineup_vs_rhp_quality REAL`
- `lineup_vs_lhp_quality REAL`
- `source_updated_at TEXT`
- `ingested_at TEXT NOT NULL DEFAULT (datetime('now'))`

Recommended key:

- `PRIMARY KEY (game_id, side, as_of_ts)`

Why this table exists:

- It gives the materializer a single, compact per-team lineup summary row.
- It handles the approved first-pass scope: announced lineup handedness structure plus lineup quality summaries.

### New derived/support table: `team_platoon_splits`

Purpose:
Store local, leakage-safe team offense vs LHP/RHP as-of each game, derived from prior completed games only.

Recommended columns:

- `game_id INTEGER NOT NULL`
- `team_id INTEGER NOT NULL`
- `side TEXT NOT NULL CHECK(side IN ('home','away'))`
- `as_of_ts TEXT NOT NULL`
- `vs_pitch_hand TEXT NOT NULL CHECK(vs_pitch_hand IN ('L','R'))`
- `games_in_sample INTEGER`
- `plate_appearances INTEGER`
- `batting_avg REAL`
- `obp REAL`
- `slg REAL`
- `ops REAL`
- `runs_per_game REAL`
- `strikeout_rate REAL`
- `walk_rate REAL`
- `source_updated_at TEXT`
- `ingested_at TEXT NOT NULL DEFAULT (datetime('now'))`

Recommended key:

- `PRIMARY KEY (game_id, side, as_of_ts, vs_pitch_hand)`

Why this table exists:

- Team-vs-handedness splits are useful even when a lineup is unavailable.
- This table gives a safe fallback and complements announced-lineup features.

### Optional future support table: `player_offense_daily`

Purpose:
Per-player, as-of-date offensive quality metrics for lineup quality summaries.

Recommendation for now:

- Plan for it, but do **not** make it mandatory in schema v1 of this expansion if lineup quality can be derived another simpler way.
- If needed, use a narrow version with only the metrics required for first-pass lineup quality summaries.

Why optional now:

- It adds material backfill cost.
- The user wants low-rework, not a massive player-stat warehouse immediately.

---

## 3) Weather / venue raw/support data

### New support/dimension table: `venue_dim`

Purpose:
Stable venue metadata for weather joins and park context.

Recommended columns:

- `venue_id INTEGER PRIMARY KEY`
- `venue_name TEXT NOT NULL`
- `city TEXT`
- `state TEXT`
- `timezone TEXT`
- `latitude REAL`
- `longitude REAL`
- `roof_type TEXT`  
  Suggested values: `open`, `retractable`, `dome`, `unknown`
- `weather_exposure_flag INTEGER NOT NULL DEFAULT 1 CHECK(weather_exposure_flag IN (0,1))`
- `park_factor_run REAL`  
  nullable; phase 2 or later
- `park_factor_hr REAL`  
  nullable; phase 2 or later
- `source_updated_at TEXT`
- `ingested_at TEXT NOT NULL DEFAULT (datetime('now'))`

Why this table exists:

- `games` currently does not carry `venue_id`.
- Venue attributes are stable dimension data and should not be duplicated into every snapshot row.

### New raw/support table: `game_weather_snapshots`

Purpose:
Store weather aligned to prediction time for each game.

Recommended columns:

- `game_id INTEGER NOT NULL`
- `as_of_ts TEXT NOT NULL`
- `snapshot_type TEXT NOT NULL`  
  Suggested values: `forecast`, `archive_observed`, `fallback_feed`
- `source TEXT NOT NULL`
- `temperature_f REAL`
- `wind_speed_mph REAL`
- `wind_direction_deg REAL`
- `precipitation_mm REAL`
- `precipitation_probability REAL`
- `humidity_pct REAL`
- `pressure_hpa REAL`
- `day_night_flag TEXT`  
  suggested values: `day`, `night`
- `weather_exposure_flag INTEGER`
- `source_updated_at TEXT`
- `ingested_at TEXT NOT NULL DEFAULT (datetime('now'))`

Recommended key:

- `PRIMARY KEY (game_id, as_of_ts, snapshot_type, source)`

Why this table exists:

- Weather is explicitly snapshot-time data.
- The same game may have multiple forecast refreshes.
- It allows training and inference to use comparable pregame states.

### Optional future support table: `game_park_context`

Purpose:
Per-game resolved park context after joining venue metadata plus simple derived weather buckets.

Recommendation:

- Not required at first.
- Can be skipped if the feature materializer joins `games -> venue_dim -> game_weather_snapshots` directly.

---

## What existing tables should stay unchanged

## Leave unchanged

### `labels`
No change needed.

### `feature_rows`
No schema change needed.

Reason:

- The repo already uses `feature_version` and JSON payloads.
- New feature families should land through a new materialization version, not table surgery.

### `ingestion_runs`, `ingestion_checkpoints`, `dq_results`, `odds_snapshot`
No schema changes required for this planning pass.

### `game_team_stats`
Leave unchanged for now.

Reason:

- It already provides completed-game team boxscore summaries and supports current rolling team form.
- Do not overload it with lineup or platoon state.

### `game_pitcher_context`
Leave largely unchanged.

Reason:

- It is already the starter/probable-pitcher support table.
- Bullpen belongs in new tables, not in further widening this starter table.

## Minimal extensions worth considering

Only two existing-table extensions are worth recommending, and both are optional:

### Optional extension A: add venue fields to `games`

Potential columns:

- `venue_id INTEGER`
- `venue_name TEXT`
- `day_night TEXT`
- `doubleheader_code TEXT`
- `series_game_number INTEGER`

Why this is worth considering:

- These are schedule-level game attributes, not separate support entities.
- They would simplify weather joins and phase-2 feature building.

Why it is optional rather than mandatory:

- The user asked for minimal rework.
- If changing `games` feels disruptive, these fields can be captured in a separate `game_metadata_snapshots` table instead.

My recommendation:

- **Add `venue_id` and `day_night` to `games` if the team is comfortable with a tiny schedule-table extension.**
- Everything else can remain additive.

### Optional extension B: add `pitch_hand` to `game_pitcher_context`

Why:

- Starter throwing hand is core matchup context and logically belongs with starter context.

Why still optional:

- It can also be derived from a player dimension at materialization time.

My recommendation:

- If one small extension is allowed here, adding `pitch_hand` is reasonable.
- If not, use `player_handedness_dim` and leave `game_pitcher_context` alone.

Net recommendation on existing-table changes:

- **Safe default: leave existing tables untouched except perhaps adding `venue_id` and `day_night` to `games`, plus optionally `pitch_hand` to `game_pitcher_context`.**

---

## How the new tables should feed feature materialization

## Recommended flow

### A. Bullpen path

1. Backfill/ingest final-game pitcher appearances into `game_pitcher_appearances`
2. For each target game and side, derive prior-only bullpen state into:
   - `team_bullpen_game_state`
   - `team_bullpen_top_relievers`
3. Materializer reads those support tables and emits features such as:
   - aggregate bullpen quality
   - recent bullpen fatigue
   - top-3/top-5 reliever quality/fatigue
   - home-away deltas

### B. Lineup/platoon path

1. Ingest announced lineup rows into `game_lineup_snapshots` at the target as-of time
2. Maintain `player_handedness_dim` for stable hand lookups
3. Derive:
   - `team_lineup_game_state` from announced lineup structure + lineup quality summaries
   - `team_platoon_splits` from prior completed games only
4. Materializer reads:
   - starter context from `game_pitcher_context`
   - lineup state from `team_lineup_game_state`
   - team-vs-hand splits from `team_platoon_splits`
5. Emit features such as:
   - lineup L/R/S counts
   - top-of-order handedness mix
   - lineup quality summaries
   - lineup-vs-opposing-starter-hand interaction terms
   - fallback team-vs-hand features when lineup is missing

### C. Weather/park path

1. Maintain `venue_dim`
2. Capture weather snapshot into `game_weather_snapshots` for the target as-of time
3. Materializer joins:
   - `games`
   - `venue_dim`
   - matching `game_weather_snapshots`
4. Emit features such as:
   - temperature
   - wind speed
   - wind direction
   - precipitation/rain risk
   - humidity
   - pressure
   - day/night
   - weather exposure / roof context

## Materialization contract recommendation

Do not replace `v1` in-place.

Recommended rollout:

- `v2_phase1` = bullpen + lineup/platoon
- `v2` or `v2_phase2` = adds weather/venue

Reason:

- lets training compare uplift by stage
- reduces rollout risk
- fits current `feature_version` contract cleanly

---

## High-level backfill design

## Historical backfill should work like this

### Step 1: bullpen raw history

For seasons 2020-2025:

1. enumerate completed games already in `games`
2. fetch/parse final boxscore or live-feed pitching lines
3. upsert one row per pitcher into `game_pitcher_appearances`
4. checkpoint by season as current ingestion already does

### Step 2: bullpen derived state

For each season in chronological order:

1. walk games by `game_date`, `scheduled_datetime`, `game_id`
2. maintain rolling prior appearance state by team and reliever
3. compute and upsert:
   - `team_bullpen_game_state`
   - `team_bullpen_top_relievers`
4. use only games completed before the target game `as_of_ts`

### Step 3: lineup raw snapshots

For historical backfill, use the project’s chosen pregame contract:

- if announced/confirmed historical lineup snapshots are reconstructable, store them at the canonical `as_of_ts`
- if not fully reconstructable, record what is available and allow degraded rows

Then:

1. populate `game_lineup_snapshots`
2. populate/update `player_handedness_dim`
3. derive `team_lineup_game_state`

### Step 4: platoon split support

1. derive starter pitch hand for prior games
2. aggregate team offense against LHP/RHP from prior completed games only
3. upsert `team_platoon_splits` per target game/side

### Step 5: weather/venue

1. populate `venue_dim` once
2. for each game, resolve venue coordinates + local start time
3. fetch/store weather snapshot aligned to target pregame cutoff in `game_weather_snapshots`

### Step 6: feature rematerialization

Run a new materializer that reads existing + new support tables and writes the new feature version into `feature_rows`.

---

## High-level daily inference-time update design

## Daily updates should work like this

### Morning / schedule discovery

1. refresh today’s schedule into `games`
2. ensure venue metadata exists for scheduled venues
3. create/update baseline schedule metadata for target games

### Near-first-pitch feature assembly (~1 hour before first pitch)

For each game approaching the target cutoff:

1. refresh probable starters into `game_pitcher_context`
2. ingest announced lineup into `game_lineup_snapshots`
3. derive/update:
   - `team_lineup_game_state`
   - `team_platoon_splits` for the target game
   - `team_bullpen_game_state`
   - `team_bullpen_top_relievers`
4. fetch/store `game_weather_snapshots`
5. materialize the target feature row into `feature_rows`

### Postgame support updates

After games finish:

1. update `games` status / scores
2. update `labels`
3. upsert `game_team_stats`
4. upsert `game_pitcher_appearances`
5. update starter actuals in `game_pitcher_context` if needed

This preserves the repo’s current rhythm:

- pregame snapshot tables for inference-time features
- postgame raw tables to support future games

---

## Biggest schema/design risks

## 1. Historical lineup timestamp realism

Biggest risk.

Using target-game actual lineups is only parity-safe if the system can reliably obtain announced lineups at the chosen prediction cutoff and historical backfill can represent that same state.

Recommendation:

- store snapshot type and lineup status explicitly
- allow lineup-driven rows to be degraded when announced lineups are unavailable
- keep team platoon splits as a robust fallback

## 2. Top-N reliever ranking definition

The project wants top-N reliever quality/fatigue summaries, but ranking relievers can become unstable.

Recommendation:

- use a simple, documented ranking rule at first, such as season-to-date leverage-agnostic quality with usage minimums
- version the ranking method in `team_bullpen_top_relievers.ranking_method`
- do not block the first build on perfect closer/setup inference

## 3. Weather train/inference mismatch

If training uses historical observed weather while live inference uses forecast weather, the model may get a slightly optimistic training signal.

Recommendation:

- keep weather feature set practical and modest in phase 2
- explicitly label snapshot type in `game_weather_snapshots`
- prefer forecast-like training snapshots when feasible later

## 4. Overbuilding a player warehouse too early

Lineup quality can tempt the project into a large player-stat system.

Recommendation:

- first build lineup structure + modest lineup quality summaries only
- do not expand into a broad player-feature mart unless needed
- keep `player_offense_daily` optional, not mandatory in the first schema expansion pass

## 5. Too much logic inside `feature_rows` JSON only

If the project skips support tables and computes everything directly into JSON, backfills and debugging will become painful fast.

Recommendation:

- materialize reusable support tables first
- keep `feature_rows` as the final contract, not the only storage layer

---

## Recommended implementation order

## Phase 1: bullpen + lineup/platoon coordinated build

This matches the approved direction and is practical if executed as one coordinated feature expansion.

### Order inside Phase 1

#### 1. Add `game_pitcher_appearances`
Reason: this is the indispensable raw table for bullpen work.

#### 2. Add `team_bullpen_game_state`
Reason: gives aggregate bullpen quality/fatigue features.

#### 3. Add `team_bullpen_top_relievers`
Reason: completes the approved top-N reliever requirement.

#### 4. Add `player_handedness_dim`
Reason: low-risk enabling table for lineup/platoon work.

#### 5. Add `game_lineup_snapshots`
Reason: raw announced-lineup capture.

#### 6. Add `team_lineup_game_state`
Reason: compact announced-lineup structure + lineup quality summaries.

#### 7. Add `team_platoon_splits`
Reason: safe lineup fallback and core matchup context.

#### 8. Extend feature materialization to write new phase-1 feature version
Reason: validate uplift before weather is added.

## Phase 2: weather / venue

#### 9. Add `venue_dim`
Reason: stable venue join layer.

#### 10. Add `game_weather_snapshots`
Reason: pregame weather snapshot support.

#### 11. Extend feature materialization again
Reason: produce weather/park-enriched feature version.

## Optional tiny schema extensions during either phase

Only if desired:

- `games.venue_id`
- `games.day_night`
- `game_pitcher_context.pitch_hand`

---

## Direct answers to the requested deliverable questions

## 1. What new raw tables should exist?

Recommended new raw tables:

- `game_pitcher_appearances`
- `game_lineup_snapshots`
- `game_weather_snapshots`

Recommended new stable dimension/support lookup tables:

- `player_handedness_dim`
- `venue_dim`

## 2. What new derived/support tables should exist?

Recommended new derived/support tables:

- `team_bullpen_game_state`
- `team_bullpen_top_relievers`
- `team_lineup_game_state`
- `team_platoon_splits`

Optional later:

- `player_offense_daily`
- `game_park_context`

## 3. What should remain in existing tables unchanged?

Leave unchanged by default:

- `labels`
- `feature_rows`
- `game_team_stats`
- `game_pitcher_context`
- `ingestion_runs`
- `ingestion_checkpoints`
- `dq_results`
- `odds_snapshot`

Potential minimal extensions only if helpful:

- add `venue_id` and `day_night` to `games`
- add `pitch_hand` to `game_pitcher_context`

## 4. How should historical backfill work at a high level?

1. Backfill raw pitcher appearances for completed games
2. Derive bullpen aggregate and top-N support rows chronologically
3. Backfill lineup snapshots where reconstructable; otherwise store degraded/fallback status
4. Derive lineup structure + team platoon split support rows
5. Populate venue dimension and weather snapshots
6. Rematerialize a new feature version into `feature_rows`

## 5. How should daily inference-time updates work at a high level?

1. Refresh today’s schedule and starters
2. Capture announced lineups around the target pregame cutoff
3. Build bullpen and lineup/platoon game-state support rows for target games
4. Fetch/store weather snapshot for the same target cutoff
5. Materialize the final feature row into `feature_rows`
6. Postgame, update completed-game raw/support tables for future use

## 6. Where are the biggest schema/design risks?

Biggest risks:

- historical announced-lineup parity/timestamp realism
- unstable top-N reliever ranking logic
- forecast-vs-observed weather mismatch
- overbuilding player-level storage too early
- skipping reusable support tables and hiding too much in final JSON payloads

## 7. What is the recommended implementation order?

Recommended implementation order:

1. `game_pitcher_appearances`
2. `team_bullpen_game_state`
3. `team_bullpen_top_relievers`
4. `player_handedness_dim`
5. `game_lineup_snapshots`
6. `team_lineup_game_state`
7. `team_platoon_splits`
8. feature materialization for phase 1
9. `venue_dim`
10. `game_weather_snapshots`
11. feature materialization for phase 2

---

## Final recommendation

The cleanest low-rework design is:

- keep the existing game/label/feature spine
- add a bullpen raw layer
- add lineup snapshot + handedness/platoon support tables
- add a small venue/weather snapshot layer
- materialize all of it into a new `feature_rows` version

That gets the project the approved dataset expansion without forcing a redesign of the current database or training pipeline.