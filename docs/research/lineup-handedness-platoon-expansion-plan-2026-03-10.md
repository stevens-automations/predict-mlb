# Predict-MLB lineup / handedness / platoon expansion plan

_Date: 2026-03-10_

## Purpose

This memo defines the next additive data expansion block for:
- announced lineups
- hitter/pitcher handedness
- lineup quality summaries
- platoon / team-vs-handedness support

This is written against the repo’s current state, not a blank-sheet design.

Important current-state note:
- bullpen raw/support tables already exist in the live schema (`game_pitcher_appearances`, `team_bullpen_game_state`, `team_bullpen_top_relievers`)
- this plan is therefore the **lineup / handedness / platoon follow-on**, designed to plug into that existing Phase 1 foundation

The goal is to keep the design:
- additive
- leakage-safe
- train/inference parity safe
- practical to backfill and run daily
- low churn for the existing DB and materialization flow

---

## Recommended scope boundary

This pass should add only the data needed to support:
1. lineup structure at prediction time
2. opposing starter handedness matchup context
3. first-pass lineup quality summaries
4. team-vs-handedness fallback/support features

This pass should **not** try to build a full player warehouse.

That means:
- raw tables capture snapshots and stable player hand metadata
- support tables capture reusable game-as-of summaries
- model features are still emitted later into `feature_rows`

---

## 1) Exact new raw/support tables needed

## New raw tables

### A. `game_lineup_snapshots`

Purpose:
Store the announced lineup state for a team in a game at a specific pregame snapshot time.

Why it is needed:
- lineups are time-sensitive snapshot data, not static game metadata
- the same game may have multiple pregame updates
- training and inference both need the same as-of snapshot concept

Recommended columns:
- `game_id INTEGER NOT NULL`
- `team_id INTEGER NOT NULL`
- `side TEXT NOT NULL CHECK(side IN ('home', 'away'))`
- `as_of_ts TEXT NOT NULL`
- `snapshot_type TEXT NOT NULL`
  - first-pass values: `announced`, `confirmed`, `fallback`
- `lineup_status TEXT NOT NULL`
  - first-pass values: `full`, `partial`, `missing`
- `player_id INTEGER NOT NULL`
- `player_name TEXT`
- `batting_order INTEGER`
- `position_code TEXT`
- `bat_side TEXT`
  - `L`, `R`, `S`, nullable if not known
- `pitch_hand TEXT`
  - player throwing hand if present in source, nullable
- `source_updated_at TEXT`
- `ingested_at TEXT NOT NULL DEFAULT (datetime('now'))`

Recommended key:
- `PRIMARY KEY (game_id, side, as_of_ts, batting_order)`

Recommended first-pass behavior:
- one row per batting-order slot observed at snapshot time
- allow fewer than 9 rows when only a partial lineup is available
- do not overwrite older snapshots; append new snapshots by `as_of_ts`

### B. `player_handedness_dim`

Purpose:
Stable lookup table for hitter batting side and pitcher throwing hand.

Why it is needed:
- handedness is reused across lineup summaries, platoon support, and starter matchup joins
- it avoids repeatedly scraping the same player metadata from raw feeds
- it supports both historical backfill and daily inference

Recommended columns:
- `player_id INTEGER PRIMARY KEY`
- `player_name TEXT`
- `bat_side TEXT`
- `pitch_hand TEXT`
- `primary_position_code TEXT`
- `source_updated_at TEXT`
- `ingested_at TEXT NOT NULL DEFAULT (datetime('now'))`

Recommended first-pass usage:
- populate from lineup payloads, player metadata endpoints, or game feed player objects when available
- treat this as the canonical lookup for hitter bat side and starter pitch hand unless a game-specific source is fresher

## New support tables

### C. `team_lineup_game_state`

Purpose:
Compact game-level summary of the lineup snapshot actually used for modeling.

Why it is needed:
- materialization should read one summarized row per team/game/as-of, not recompute lineup structure every time
- this cleanly separates raw snapshot storage from derived support state

Recommended columns:
- `game_id INTEGER NOT NULL`
- `team_id INTEGER NOT NULL`
- `side TEXT NOT NULL CHECK(side IN ('home', 'away'))`
- `as_of_ts TEXT NOT NULL`
- `snapshot_type TEXT NOT NULL`
- `lineup_status TEXT NOT NULL`
- `lineup_known_flag INTEGER NOT NULL DEFAULT 0 CHECK(lineup_known_flag IN (0,1))`
- `announced_lineup_count INTEGER NOT NULL DEFAULT 0`
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

### D. `team_platoon_splits`

Purpose:
Store leakage-safe team offense splits versus left-handed and right-handed starting pitchers as of each target game.

Why it is needed:
- it gives a useful core matchup feature even when lineup data is missing
- it provides a parity-safe fallback when announced lineups are unavailable historically or on inference day
- it keeps team-vs-hand support separate from lineup snapshots

Recommended columns:
- `game_id INTEGER NOT NULL`
- `team_id INTEGER NOT NULL`
- `side TEXT NOT NULL CHECK(side IN ('home', 'away'))`
- `as_of_ts TEXT NOT NULL`
- `vs_pitch_hand TEXT NOT NULL CHECK(vs_pitch_hand IN ('L','R'))`
- `stats_scope TEXT NOT NULL DEFAULT 'prior_completed_games_only'`
- `games_in_sample INTEGER NOT NULL DEFAULT 0`
- `plate_appearances INTEGER NOT NULL DEFAULT 0`
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

## Optional later table, not required for this pass

### `player_offense_daily`

Recommendation:
Do **not** require this for the first build.

Reason:
- it increases backfill cost and schema surface area a lot
- first-pass lineup quality can be supported with a narrower lookup or precomputed player quality logic without building a full player mart

---

## 2) Where hitter handedness and pitcher handedness should come from

## Hitter handedness

Recommended source of truth:
1. `player_handedness_dim.bat_side`
2. if a lineup/game payload includes player batting side, use it to populate or refresh `player_handedness_dim`
3. copy the observed value into `game_lineup_snapshots.bat_side` when available for snapshot auditability

Recommendation:
- use `player_handedness_dim` as the canonical long-lived lookup
- keep `game_lineup_snapshots.bat_side` as the snapshot-level observed value when present
- if the two disagree, favor the snapshot value for that specific row and refresh the dimension after review rules are defined

## Pitcher handedness

Two different pitcher-hand use cases matter.

### A. Opposing starter hand for modeling

Recommended source order:
1. add/use starter hand on the pregame starter context path if practical
2. otherwise resolve starter hand from `player_handedness_dim.pitch_hand` using `game_pitcher_context.probable_pitcher_id`

Practical recommendation:
- do **not** create a separate lineup-only pitcher-hand table
- use the existing `game_pitcher_context` + `player_handedness_dim` join
- if a tiny schema extension is acceptable later, adding starter `pitch_hand` directly to `game_pitcher_context` is clean, but it is not mandatory for this pass

### B. Historical split aggregation for prior games

For `team_platoon_splits`, the historical opponent starter hand should come from:
- actual/probable starter ids already captured in `game_pitcher_context`
- starter hand resolved through `player_handedness_dim`

Bottom line:
- hitter handedness source = `player_handedness_dim` plus snapshot copy on lineup rows
- pitcher handedness source = `game_pitcher_context` starter id resolved through `player_handedness_dim`

---

## 3) How announced lineup snapshots should be stored historically and at inference time

## Historical storage rule

Store lineup snapshots as **append-only snapshot rows** in `game_lineup_snapshots`.

That means:
- keep every observed pregame snapshot by `as_of_ts`
- do not collapse historical snapshots into a single final row
- derive the model-facing support row from the best eligible snapshot at or before the target prediction cutoff

## Canonical as-of rule

For modeling, define one canonical target cutoff per game, for example:
- approximately 60 minutes before scheduled first pitch

Then for both historical training and live inference:
- choose the latest lineup snapshot with `as_of_ts <= target_cutoff_ts`
- if no qualifying lineup exists, mark lineup unavailable/degraded and fall back to team platoon support

This is the key train/inference parity rule.

## Historical backfill behavior

Historical lineup data will not always be reconstructable at the exact target cutoff.

So first pass should allow three states:
- `full`: all 9 hitters known at or before cutoff
- `partial`: some hitters/order known, but not full lineup
- `missing`: no valid lineup snapshot before cutoff

Recommendation:
- keep `snapshot_type` and `lineup_status` explicit in both raw and support layers
- let `team_lineup_game_state` represent degraded reality instead of pretending every historical game has a perfect lineup snapshot

## Inference-time storage rule

At inference time:
- ingest snapshots repeatedly during the pregame window if needed
- append each observation to `game_lineup_snapshots`
- materialize the lineup support row using the same cutoff-selection rule used in training

This preserves parity and makes late lineup changes auditable.

---

## 4) How lineup quality summaries should be represented in first pass

First pass should keep lineup quality simple and additive.

## Recommended first-pass representation

Represent lineup quality only as **compact summary metrics** inside `team_lineup_game_state`.

Recommended fields to populate:
- `lineup_quality_metric`
- `lineup_quality_mean`
- `top3_lineup_quality_mean`
- `top5_lineup_quality_mean`
- `lineup_vs_rhp_quality`
- `lineup_vs_lhp_quality`

## Practical definition

Use a single documented player-quality metric for first pass and stick to it.

Best practical recommendation:
- use a season-to-date offensive quality proxy available as of game date
- aggregate it across announced hitters
- also compute top-3 and top-5 order summaries
- compute split-specific lineup summaries versus RHP and LHP if the player-level inputs support it cleanly

## What not to do in first pass

Do not try to store:
- full player-level feature blobs in the lineup support table
- many competing quality metrics at once
- injury/star-missing logic unless timestamp-safe and easy

## Why this is the right first pass

This gives the model:
- lineup strength level
- concentration of quality at the top of the order
- basic split-aware lineup strength

without forcing a full player warehouse immediately.

---

## 5) How platoon / team-vs-handedness support should be represented

Use `team_platoon_splits` as the canonical support table.

## Representation rules

- one row per `game_id / side / as_of_ts / vs_pitch_hand`
- only `vs_pitch_hand IN ('L','R')`
- derive from prior completed games only
- aggregate at team level, not player level

## Recommended first-pass metrics

Keep this table narrow and stable:
- `games_in_sample`
- `plate_appearances`
- `batting_avg`
- `obp`
- `slg`
- `ops`
- `runs_per_game`
- `strikeout_rate`
- `walk_rate`

## Why this shape works

It supports both:
- direct model features
- fallback logic when lineup snapshots are partial or missing

It also avoids schema churn because later richer split logic can be added without changing the core contract.

---

## 6) Historical backfill path

## Step 1: finalize player handedness lookup

Build `player_handedness_dim` first.

Backfill sources can include:
- player metadata attached to historical game feeds
- lineup payloads when available
- existing starter/player records already observed during other ingest paths

Goal:
- high coverage for players who appear in lineups or as probable starters

## Step 2: backfill historical lineup snapshots where source supports it

For seasons already in scope:
- walk games chronologically
- collect any reconstructable pregame lineup snapshots
- write append-only rows into `game_lineup_snapshots`

Important rule:
- do not fake exact historical timing if it is not known
- if the source only supports weaker historical reconstruction, record `snapshot_type` and `lineup_status` honestly

## Step 3: derive `team_lineup_game_state`

For each game/side and canonical target cutoff:
- choose the latest eligible snapshot at or before the cutoff
- compute lineup structure metrics
- compute first-pass quality summaries
- write one support row per team/game/as-of

## Step 4: derive `team_platoon_splits`

For each target game/side:
- look back only at prior completed games
- determine opponent starter hand for each prior game
- aggregate team offense versus LHP and RHP separately
- write two support rows when possible: one for `L`, one for `R`

## Step 5: rematerialize feature rows

After the support layer exists:
- extend feature materialization into a new feature version
- do not mutate or replace existing `v1`
- write new enriched rows under a new feature version such as `v2_phase1`

---

## 7) Daily inference-time update path

## Morning / early-day prep

1. refresh today’s `games`
2. refresh probable starters into `game_pitcher_context`
3. ensure starter hand can be resolved through `player_handedness_dim`
4. ensure expected hitters already known from any early source refresh handedness lookup coverage where possible

## Pregame update window

During the hour or so before first pitch:

1. poll/refresh lineup source for today’s games
2. append every observed lineup snapshot into `game_lineup_snapshots`
3. update `player_handedness_dim` from any newly seen players
4. at target cutoff, derive `team_lineup_game_state`
5. derive `team_platoon_splits` for the target games using prior completed games only
6. materialize the new `feature_rows` version for prediction

## Late changes

If a lineup changes after an earlier snapshot:
- append a new snapshot row
- if the prediction has not yet been finalized, recompute support rows using the latest eligible snapshot before the actual prediction run time

## Postgame

No special lineup writeback is needed beyond preserving snapshots already stored.

The postgame path mainly matters for:
- future platoon sample growth
- future lineup quality inputs if player daily offense support is added later

---

## 8) Leakage and parity risks

## Risk 1: using final/actual lineup instead of announced lineup

This is the biggest risk.

Bad pattern:
- training on the final lineup that actually played
- inference on whatever was announced pregame

Required safeguard:
- train only on the latest eligible snapshot at or before the prediction cutoff
- if no pregame snapshot exists, mark lineup unavailable and use fallback logic

## Risk 2: hidden timestamp mismatch in historical backfill

If historical lineups are reconstructed from sources without reliable timestamps, the dataset can quietly leak information.

Required safeguard:
- preserve `snapshot_type` and `lineup_status`
- prefer degraded truth over false precision
- treat low-confidence historical lineup coverage as a contract issue, not a reason to silently use better-than-live information

## Risk 3: pitcher handedness joined from postgame/incorrect starter

For platoon context, the model should use the starter hand that would have been known at prediction time.

Required safeguard:
- source starter hand from the probable starter path used at inference
- do not let actual postgame substitution logic rewrite the pregame matchup context

## Risk 4: lineup quality built from future-season aggregates

This is the same class of leakage that existed in older approaches.

Required safeguard:
- any player quality metric used in lineup summaries must be as-of-date only
- no end-of-season totals for historical rows

## Risk 5: feature availability asymmetry

If historical lineup coverage is much worse than live coverage, the model may overfit to mixed availability patterns.

Required safeguard:
- include lineup availability / status indicators in support or final features
- explicitly test model behavior on games with and without lineup support

## Risk 6: overbuilding too early

A full player-stat warehouse increases delivery time and increases the chance of quiet parity bugs.

Required safeguard:
- keep first pass to lineup structure + compact quality summaries + team platoon fallback

---

## 9) Recommended implementation order

Because bullpen foundation is already present, the recommended order for this track is:

### 1. `player_handedness_dim`
Why first:
- low-risk enabling table
- needed by both lineup and platoon logic
- also supports starter hand resolution

### 2. `game_lineup_snapshots`
Why second:
- raw snapshot capture is the foundation for announced-lineup features
- append-only design should be locked before support derivation begins

### 3. `team_lineup_game_state`
Why third:
- this turns raw snapshots into a stable, compact support interface
- easiest place to represent lineup known/partial/missing state cleanly

### 4. `team_platoon_splits`
Why fourth:
- useful even before lineup coverage is complete
- gives a robust fallback path and protects parity

### 5. feature materialization update for `v2_phase1`
Why fifth:
- once lineup support and platoon support are stable, emit the new model-facing feature version
- validate uplift and coverage before adding more data families

### 6. optional later: narrow player offense support only if needed
Why later:
- only add a `player_offense_daily`-style table if first-pass lineup quality cannot be computed cleanly enough from a narrower approach

---

## Final recommendation

The cleanest next build is:
- treat bullpen as already-established support infrastructure
- add one raw lineup snapshot table
- add one stable player handedness dimension
- add two support tables: lineup state and team platoon splits
- keep lineup quality first-pass summary-only
- make the cutoff-selection rule identical for training and inference

That gives the project the highest-value lineup/platoon context without forcing a large schema rewrite or a full player warehouse.

---

## Short answer summary

### Recommended new tables
- `game_lineup_snapshots`
- `player_handedness_dim`
- `team_lineup_game_state`
- `team_platoon_splits`
- optional later only: `player_offense_daily`

### Handedness sourcing
- hitter handedness: `player_handedness_dim` as canonical lookup, snapshot copy on lineup rows when observed
- pitcher handedness: resolve starter hand from `game_pitcher_context` probable starter id joined to `player_handedness_dim`

### First-pass lineup quality representation
- summary fields only in `team_lineup_game_state`
- do not build a broad player warehouse yet

### Historical/inference parity rule
- use the latest lineup snapshot at or before the canonical prediction cutoff
- otherwise degrade gracefully and fall back to `team_platoon_splits`
