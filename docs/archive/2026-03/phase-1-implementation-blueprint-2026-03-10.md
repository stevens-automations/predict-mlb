# Predict-MLB Phase 1 implementation blueprint

_Date: 2026-03-10_

## Purpose

This is the execution blueprint for **Phase 1** of the approved dataset expansion.

Phase 1 is the first major upgrade to the pregame feature stack. It should make the model materially more baseball-aware without rewriting the current system.

This doc is meant to be good enough for:
- Mako review
- Steven approval
- direct Codex handoff after approval

This is a planning document only. It does **not** implement code.

---

## 1) Exact Phase 1 implementation scope

## In scope

Phase 1 includes exactly two new feature families:

1. **Bullpen state / quality / fatigue**
2. **Lineup / handedness / platoon context**

Approved Phase 1 decisions already locked:
- Bullpen first pass includes **both**:
  - aggregate bullpen features
  - top-N reliever quality/fatigue summaries
- Lineup/platoon first pass includes:
  - announced lineup handedness structure
  - lineup quality summaries
  - unless major implementation issues force a controlled fallback
- Prediction timing target is **~1 hour before first pitch**
- Announced lineups are **in scope**
- There is **no two-stage prediction system**

## Out of scope

The following are explicitly **not** implementation targets for Phase 1:
- weather/venue feature implementation
- park factor modeling
- new run-margin model work
- injury/news systems
- odds/market features
- broad player warehouse beyond what is needed for lineup summaries
- retrospective schema redesign of current v1 tables
- replacing the current training pipeline

## Phase 1 outcome target

At the end of Phase 1, the repo should support a new feature version that:
- preserves the current `games -> support tables -> feature_rows -> labels` pattern
- can be backfilled historically for 2020-2025
- can be refreshed daily around ~1 hour before first pitch
- remains reconstructible from information available by the chosen pregame cutoff
- produces a stable, testable, leakage-aware feature block for bullpen + lineup/platoon

---

## 2) Implementation principles

These are the design rules for Phase 1.

### A. Additive, not invasive
Do not redesign the database. Add support tables and a new feature version.

### B. Reuse the existing repo pattern
Current repo pattern:
1. ingest schedule/results into `games` / `labels`
2. ingest support tables
3. materialize model-facing rows into `feature_rows`

Phase 1 should follow the same pattern.

### C. Pregame realism beats perfect richness
If a feature is hard to source correctly as-of ~1 hour before first pitch, use a simpler fallback instead of building something fragile.

### D. Train/inference parity is mandatory
If a feature is used in training, there must be a documented runtime computation or retrieval path so the same information class can be used for live daily predictions.
Phase 1 should assume the project DB is refreshed in real time (or near-real time) each day so bullpen and lineup/platoon state can be recomputed for inference using the same logic family as historical training.

### D. Keep feature logic inspectable
Important derived state should live in support tables, not only inside final JSON.

### E. Stage the work so every checkpoint is verifiable
Do not build the full stack in one opaque jump.

---

## 3) Recommended schema plan for Phase 1

Phase 1 should use the already approved schema direction.

## New support tables

### 1. `game_pitcher_appearances`
**Purpose:** canonical per-pitcher per-game appearance history.

This is the raw foundation for bullpen features.

**Why it exists:**
- current `game_pitcher_context` is starter-focused
- bullpen features require every pitcher appearance, not only the probable starter
- this table becomes the reusable source of truth for bullpen workload and reliever quality history

**Minimum required fields:**
- `game_id`
- `team_id`
- `side`
- `pitcher_id`
- `pitcher_name`
- `appearance_order` if derivable
- `is_starter`
- `is_reliever`
- `outs_recorded`
- `innings_pitched`
- `batters_faced` if available
- `pitches`
- `strikes`
- `hits`
- `walks`
- `strikeouts`
- `runs`
- `earned_runs`
- `home_runs`
- `holds`
- `save_flag`
- `blown_save_flag`
- `inherited_runners`
- `inherited_runners_scored`
- `source_updated_at`

### 2. `game_lineup_snapshots`
**Purpose:** player-level announced lineup snapshot for a game at the prediction-time cutoff.

**Why it exists:**
- lineups are snapshot data, not stable game metadata
- a game may have multiple pregame snapshots later, even if Phase 1 only uses one canonical one
- player-level rows preserve future flexibility while still supporting simple aggregate derivations

**Minimum required fields:**
- `game_id`
- `team_id`
- `side`
- `as_of_ts`
- `snapshot_type` (`announced`, `confirmed`, `fallback`)
- `lineup_status` (`full`, `partial`, `missing`)
- `player_id`
- `player_name`
- `batting_order`
- `position_code`
- `source_updated_at`

### 3. `player_handedness_dim`
**Purpose:** stable lookup for player batting/pitching handedness.

**Why it exists:**
- lineup features need handedness repeatedly
- this avoids repeated live-feed parsing when materializing features
- this is small, reusable, and low risk

**Minimum required fields:**
- `player_id`
- `player_name`
- `bat_side`
- `pitch_hand`
- `primary_position_code` if available
- `source_updated_at`

### 4. `team_bullpen_game_state`
**Purpose:** aggregate bullpen state for a team as of a target game.

**Why it exists:**
- keeps heavy rolling-window bullpen logic out of final feature JSON
- makes debugging easier
- matches current repo pattern of support-table-first materialization

**Minimum required fields:**
- `game_id`
- `team_id`
- `side`
- `as_of_ts`
- season-to-date aggregate bullpen quality metrics
- last 1/3/5/7 day workload metrics
- recent reliever usage counts
- freshness score
- source metadata

### 5. `team_bullpen_top_relievers`
**Purpose:** top-N reliever quality/fatigue summaries as of a target game.

**Why it exists:**
- this is explicitly in the approved scope
- keeps top-N logic versioned and inspectable
- avoids burying reliever selection logic inside the materializer

**Minimum required fields:**
- `game_id`
- `team_id`
- `side`
- `as_of_ts`
- `ranking_method`
- `top_n`
- `n_available`
- top-N quality summary metrics
- top-N recent workload/fatigue summary metrics
- `quality_dropoff_vs_team`
- source metadata

**Phase 1 default:**
- materialize `top_n = 3` and `top_n = 5`

### 6. `team_lineup_game_state`
**Purpose:** compact game-level summary of announced lineup structure and lineup quality.

**Why it exists:**
- this is the main support table for lineup features
- feature materialization should read a simple per-team lineup state row, not recompute it each time

**Minimum required fields:**
- `game_id`
- `team_id`
- `side`
- `as_of_ts`
- `snapshot_type`
- `lineup_known_flag`
- `announced_lineup_count`
- lineup L/R/S counts
- top-3 L/R/S counts
- top-5 L/R/S counts
- lineup handedness balance metrics
- lineup quality summary metrics
- lineup-vs-RHP quality summary
- lineup-vs-LHP quality summary
- source metadata

### 7. `team_platoon_splits`
**Purpose:** leakage-safe team offensive splits vs LHP/RHP as of each game.

**Why it exists:**
- lineup may be missing or partial for some games
- this gives a robust fallback and a useful core matchup feature even when lineup data exists
- should be derived locally from prior completed games only

**Minimum required fields:**
- `game_id`
- `team_id`
- `side`
- `as_of_ts`
- `vs_pitch_hand`
- `games_in_sample`
- `plate_appearances`
- split offense metrics such as `obp`, `slg`, `ops`, `runs_per_game`, `strikeout_rate`, `walk_rate`
- source metadata

## Existing tables that should stay unchanged

Leave these alone in Phase 1 unless a tiny migration becomes clearly necessary:
- `labels`
- `feature_rows`
- `game_team_stats`
- `game_pitcher_context`
- `ingestion_runs`
- `ingestion_checkpoints`
- `dq_results`
- `odds_snapshot`

## Optional tiny extension

A small extension to `game_pitcher_context` for `pitch_hand` is reasonable, but not required.

Recommendation: **do not require it for Phase 1**. Use `player_handedness_dim` instead unless implementation friction proves otherwise.

---

## 4) Table-by-table purpose and dependencies

This is the practical dependency map.

## Table dependency summary

### `game_pitcher_appearances`
**Built from:**
- `games`
- Stats API boxscore/live feed for completed games

**Used by:**
- `team_bullpen_game_state`
- `team_bullpen_top_relievers`
- future pitching features if needed

### `player_handedness_dim`
**Built from:**
- Stats API player metadata from game feeds / lineups

**Used by:**
- `game_lineup_snapshots`
- `team_lineup_game_state`
- starter hand lookup if needed

### `game_lineup_snapshots`
**Built from:**
- schedule/game IDs in `games`
- pregame lineup source at or near the target cutoff
- `player_handedness_dim` enrichment when available

**Used by:**
- `team_lineup_game_state`

### `team_bullpen_game_state`
**Built from:**
- `games`
- `game_pitcher_appearances`
- target-game `as_of_ts`

**Used by:**
- feature materializer

### `team_bullpen_top_relievers`
**Built from:**
- `games`
- `game_pitcher_appearances`
- reliever ranking rule
- target-game `as_of_ts`

**Used by:**
- feature materializer

### `team_lineup_game_state`
**Built from:**
- `game_lineup_snapshots`
- `player_handedness_dim`
- lineup quality lookup logic

**Used by:**
- feature materializer

### `team_platoon_splits`
**Built from:**
- prior completed games
- team offense grouped by opposing starter hand
- starter hand lookup from `game_pitcher_context` or `player_handedness_dim`

**Used by:**
- feature materializer
- lineup fallback logic

### `feature_rows` new version
**Built from:**
- current v1 sources
- `team_bullpen_game_state`
- `team_bullpen_top_relievers`
- `team_lineup_game_state`
- `team_platoon_splits`
- existing `game_pitcher_context`

**Used by:**
- training
- offline validation
- inference

---

## 5) Exact Phase 1 feature plan

This section defines what should actually land in the new feature version.

## A. Bullpen features

### Aggregate bullpen quality
Include team-level prior-only bullpen metrics such as:
- bullpen season ERA proxy
- bullpen season WHIP proxy
- bullpen season runs per 9
- bullpen K rate
- bullpen BB rate
- bullpen K-BB rate
- bullpen HR rate if available
- bullpen sample-size field(s)

### Aggregate bullpen fatigue / workload
Include:
- bullpen outs last 1/3/5/7 days
- bullpen pitches last 1/3/5 days
- bullpen appearances last 3/5 days
- relievers used yesterday count
- relievers used last 3 days count
- back-to-back relievers count
- relievers used 2 of last 3 days count
- high-usage relievers count
- bullpen freshness score

### Top-N reliever quality / fatigue
For `top_n = 3` and `top_n = 5`, include:
- top-N season ERA proxy
- top-N season WHIP proxy
- top-N K-BB rate proxy
- top-N outs last 3 days
- top-N pitches last 3 days
- top-N appearances last 3 days
- top-N back-to-back count
- top-N freshness score
- team-vs-topN dropoff measure

### Home-away interaction / delta features
Include materialized deltas such as:
- home bullpen freshness minus away bullpen freshness
- home bullpen quality minus away bullpen quality
- home top-3 freshness minus away top-3 freshness
- home top-5 quality minus away top-5 quality

## Bullpen ranking rule recommendation

Phase 1 should use a **simple, documented, stable ranking rule**.

Recommended rule:
- consider only relievers with a minimum usage threshold in season-to-date prior games
- rank relievers by a simple composite of:
  - outs recorded
  - K-BB rate proxy
  - WHIP proxy
  - runs-per-9 / ERA proxy
- do not try to infer true leverage or manager trust in Phase 1

Reason: simple and durable beats clever and noisy for the first build.

## B. Lineup / handedness / platoon features

### Announced lineup structure
Include:
- lineup known flag
- lineup announced count
- lineup left/right/switch counts
- top-3 left/right/switch counts
- top-5 left/right/switch counts
- lineup handedness balance score
- lefty PA share proxy
- righty PA share proxy
- switch PA share proxy

### Lineup quality summaries
Include first-pass summaries if they can be built cleanly:
- lineup quality mean
- top-3 lineup quality mean
- top-5 lineup quality mean
- lineup vs RHP quality
- lineup vs LHP quality

### Platoon context
Include:
- home team offense vs opposing starter hand
- away team offense vs opposing starter hand
- home team split stats vs LHP and vs RHP
- away team split stats vs LHP and vs RHP
- split sample-size fields

### Matchup interaction features
Include:
- lineup-vs-opposing-starter-handedness score
- top-of-order platoon balance score
- lineup quality vs starter-hand interaction
- fallback team-platoon matchup score when lineup is missing

## Lineup quality metric recommendation

Phase 1 should avoid a huge player warehouse.

Recommended first pass:
- use a simple local player-quality summary derived from already available season-to-date hitting information if that can be sourced cheaply
- if that becomes a major blocker, keep Phase 1 scope by shipping:
  - announced lineup handedness structure
  - team platoon splits
  - lineup known/missing flags
and defer richer lineup quality summaries to a follow-up checkpoint

Strong recommendation:
- **try to include lineup quality summaries in Phase 1**
- **do not let them block the broader Phase 1 rollout if sourcing turns into major churn**

---

## 6) Historical backfill flow

Historical backfill should be chronological, resumable, and rate-limited.

## Backfill target window
- seasons: 2020-2025
- game scope: all relevant regular-season games already in `games`
- output: new support tables plus a new `feature_rows.feature_version`

## Backfill sequence

### Step 1. Schema migration
Add the new Phase 1 tables.

Deliverable:
- updated `scripts/sql/history_schema.sql`
- minimal migration handling in `ensure_schema()` style logic if needed

### Step 2. Backfill `game_pitcher_appearances`
For each completed historical game:
- fetch final boxscore/live feed
- extract one row per pitcher appearance
- classify starter vs reliever
- upsert rows
- checkpoint by season and game cursor

Deliverable:
- complete appearance history for all available seasons

### Step 3. Backfill `player_handedness_dim`
Populate/update player handedness opportunistically from:
- lineup sources
- game feed player metadata
- pitcher/starter lookups where needed

Deliverable:
- stable handedness coverage for players appearing in Phase 1 features

### Step 4. Backfill `game_lineup_snapshots`
For historical games, load the canonical lineup snapshot used for the project’s chosen ~1 hour pregame contract.

Important practical note:
- if exact historical timestamp parity is imperfect, the implementation must explicitly tag snapshot quality and allow degraded rows

Deliverable:
- one canonical Phase 1 lineup snapshot per game/side/as_of_ts
- degraded status where unavailable or partial

### Step 5. Build `team_bullpen_game_state`
Walk games in chronological order and compute bullpen aggregate state from **prior completed games only**.

Deliverable:
- per-game, per-side bullpen aggregate support rows

### Step 6. Build `team_bullpen_top_relievers`
Using the same chronological pass or a second derived pass:
- rank relievers using the approved simple rule
- compute top-3 and top-5 summaries
- compute freshness/workload summaries

Deliverable:
- per-game, per-side, top-N bullpen support rows

### Step 7. Build `team_platoon_splits`
For each target game and side:
- derive prior-only team offense vs LHP and vs RHP
- store split metrics plus sample size

Deliverable:
- leakage-safe platoon support rows

### Step 8. Build `team_lineup_game_state`
For each target game and side:
- summarize lineup structure from `game_lineup_snapshots`
- join handedness
- compute lineup quality summaries if available

Deliverable:
- compact game-level lineup state rows

### Step 9. Materialize new feature version
Build a new feature version, recommended name:
- `v2_phase1`

Do not overwrite `v1`.

Deliverable:
- one new Phase 1 feature row per game
- source contract status and issue tracking preserved

### Step 10. Run DQ + validation suite
Validate row counts, leakage protections, null rates, and feature consistency.

---

## 7) Daily inference-time update flow

The project’s live operating model should mirror the historical contract as closely as practical.

## Daily operating window
Target feature assembly time:
- approximately **1 hour before first pitch**

## Same-day update flow

### Step 1. Refresh schedule spine
Update or confirm today’s games in `games`.

### Step 2. Refresh starter context
Populate/refresh `game_pitcher_context` for target games.

### Step 3. Compute bullpen state for target games
Using all completed games already in the DB up to the cutoff:
- derive `team_bullpen_game_state`
- derive `team_bullpen_top_relievers`

### Step 4. Capture announced lineups
At the target cutoff:
- fetch/store `game_lineup_snapshots`
- update `player_handedness_dim` for any unseen players

### Step 5. Build lineup/platoon state
For each target game:
- build `team_lineup_game_state`
- build or refresh `team_platoon_splits`

### Step 6. Materialize final feature row
Write one `feature_rows` record per game with:
- `feature_version = v2_phase1`
- `as_of_ts` aligned to the chosen pregame cutoff
- degraded contract status if any required source is missing

### Step 7. Postgame raw support update
After games finish:
- refresh labels/results
- add final pitcher appearances for those games
- preserve the support base for future dates

## Practical same-day fallback rules

If a lineup is unavailable at the cutoff:
- still materialize the row
- mark lineup-related issues in `source_contract_issues_json`
- fall back to team platoon split features + lineup missing flags

If probable starter is unavailable:
- preserve null-safe features
- include starter-known flags and degrade status

This is better than skipping the entire row.

---

## 8) Feature materialization flow / dependency graph

This is the high-level dependency graph Codex should implement.

```text
games -------------------------------┐
labels ------------------------------┤
                                     │
completed game boxscores/live feeds -> game_pitcher_appearances -----> team_bullpen_game_state -----┐
                                                   └---------------> team_bullpen_top_relievers ----┤
                                                                                                      │
pregame lineup source -------------> game_lineup_snapshots -----> team_lineup_game_state ------------┤
                                  └-> player_handedness_dim ------------------------------------------┤
                                                                                                      │
prior completed games + starter hand ---------------------------> team_platoon_splits ----------------┤
                                                                                                      │
existing game_pitcher_context ------------------------------------------------------------------------┤
existing team form from v1 path ----------------------------------------------------------------------┤
                                                                                                      │
                                                                                           feature_rows(v2_phase1)
```

## Materializer behavior recommendation

The new materializer should:
- start from the existing `cmd_materialize_feature_rows` pattern
- preserve current team-form and starter-context logic
- add Phase 1 support-table joins
- emit a new feature payload with:
  - raw home/away values
  - explicit matchup deltas
  - missingness/sample-size flags
  - source degradation status

## Recommended feature naming style

Stay consistent with current payload style and avoid overly clever nesting.

Prefer flat keys such as:
- `home_bullpen_freshness_score`
- `away_bullpen_freshness_score`
- `bullpen_freshness_delta`
- `home_lineup_l_count`
- `away_lineup_l_count`
- `home_team_ops_vs_rhp_prior`
- `away_team_ops_vs_lhp_prior`
- `home_lineup_known_flag`

Recommendation:
- include both side-specific fields and a small set of explicit deltas
- do not rely on training code to derive all matchup interactions later

---

## 9) API usage and rate-limit strategy

This section matters because Phase 1 adds more external lookups.

## Primary design goal
Minimize Stats API calls and keep the build resumable.

## General strategy

### A. Reuse the current bounded request pattern
The repo already has:
- request budget control
- bounded retries
- checkpointing

Phase 1 should extend that pattern rather than invent a new fetch system.

### B. Backfill in large local-first passes
Best practice for historical backfill:
- fetch each completed game once for the richest needed payload
- extract all usable pitcher + lineup/player metadata in the same pass where possible
- avoid separate per-player lookup loops unless coverage gaps require it

### C. Favor game-level payloads over player-level fan-out
Preferred order:
1. game boxscore/live feed
2. only then player-level lookup for missing handedness edge cases

This keeps request volume manageable.

### D. Checkpoint aggressively by season
Recommended checkpoint units:
- season-level jobs
- cursor by last processed `game_id`
- commit every N games, similar to existing `checkpoint_every`

### E. Daily inference should only touch today’s games
Same-day runs should not re-query broad historical data. They should:
- compute from local DB
- fetch only fresh starter/lineup data required for today’s slate

## Recommended request policy by job

### Historical `game_pitcher_appearances` backfill
- primary source: one game-level payload per completed game
- budget: highest among Phase 1 jobs
- retries: bounded, existing defaults acceptable
- checkpoint every 25-50 games

### Historical lineup snapshot backfill
- if historical lineup source is game-level feed-based, combine with pitcher appearance extraction where practical
- only use additional lookups when lineup or player metadata is missing

### Historical handedness fill-in
- run as a low-volume repair pass, not the main ingestion path
- only query players missing from `player_handedness_dim`

### Daily pregame run
- fetch only same-day target games
- one pass for starters
- one pass for lineups near cutoff
- derive everything else locally

## Rate-limit fallback strategy

If API usage becomes too expensive or unstable:
1. prioritize bullpen raw history first
2. prioritize daily lineup snapshots over perfect historical lineup richness
3. allow degraded historical lineup rows where needed
4. do not block Phase 1 on perfect historical lineup parity

That is the right tradeoff because bullpen + platoon fallback still provide meaningful value.

---

## 10) Validation and test strategy

Phase 1 should not be considered complete without a real validation plan.

## A. Schema and ingestion tests

### Required checks
- new tables created successfully on empty DB
- migrations are idempotent
- upserts are stable
- primary keys prevent duplicate rows
- checkpoint resume works after interruption

### Recommended tests
- fixture-based tests for parsing pitcher appearances
- fixture-based tests for lineup snapshot parsing
- tests for handedness extraction normalization

## B. Data quality checks

### Required DQ checks
- one `team_bullpen_game_state` row per game side for eligible games
- expected `team_bullpen_top_relievers` rows per game side for `top_n in (3,5)`
- one `team_lineup_game_state` row per game side in inference/backfill output, even if degraded
- exactly two `team_platoon_splits` rows per game side (`vs L`, `vs R`)
- null rates and degraded rates reported by season

### Coverage checks
- pitcher appearance coverage vs completed games
- lineup snapshot coverage by season
- handedness coverage for lineup players
- starter-hand coverage for platoon split joins

## C. Leakage checks

These are non-negotiable.

### Bullpen leakage test
Confirm bullpen state for game G only uses pitcher appearances from games completed before G’s `as_of_ts`.

### Platoon leakage test
Confirm `team_platoon_splits` for game G only uses prior completed games.

### Lineup parity test
Confirm lineup features are sourced from the target pregame snapshot contract, not postgame actuals silently substituted without marking degradation.

### Feature as-of ordering test
For sampled games, verify all support-table source timestamps are `<= feature_rows.as_of_ts` where that timestamp exists.

## D. Feature-contract tests

The new `v2_phase1` payload should have:
- stable key set
- deterministic naming
- null-safe defaults
- explicit missingness/sample-size flags

### Recommended contract checks
- payload contains every required Phase 1 feature key
- no impossible values
- no negative counts where not allowed
- delta fields equal home minus away fields for sampled rows

## E. Model validation checks

This is still a planning-phase implementation blueprint, but Phase 1 rollout should include:
- rematerialize `v2_phase1`
- train the existing modeling pipeline on `v2_phase1`
- compare against `v1`
- inspect not only aggregate metrics, but also coverage and degradation

Primary checks:
- log loss
- Brier score
- calibration
- source degradation rate
- recent-season performance stability

---

## 11) Rollout and checkpoint sequence

This should be executed in controlled checkpoints, not as one giant merge.

## Checkpoint 0 — Blueprint approval
**Goal:** lock the plan before coding.

Approval items:
- Phase 1 scope accepted
- fallback policy accepted
- `v2_phase1` naming accepted
- top-N reliever ranking rule accepted

## Checkpoint 1 — Schema + raw table scaffolding
**Deliverables:**
- schema updates for all new Phase 1 tables
- upsert helpers
- migration/idempotency tests

**Exit criteria:**
- DB initializes cleanly
- new tables exist
- no existing pipeline regressions

## Checkpoint 2 — Historical pitcher appearance backfill
**Deliverables:**
- `game_pitcher_appearances` extraction job
- season checkpointing
- row count / coverage reporting

**Exit criteria:**
- completed games have strong appearance coverage
- parsing looks correct on sampled games

## Checkpoint 3 — Bullpen derived state
**Deliverables:**
- `team_bullpen_game_state`
- `team_bullpen_top_relievers`
- DQ checks for coverage and leakage

**Exit criteria:**
- support tables populated for target seasons
- sampled feature calculations look sane

## Checkpoint 4 — Lineup + handedness raw state
**Deliverables:**
- `player_handedness_dim`
- `game_lineup_snapshots`
- coverage/degraded reporting

**Exit criteria:**
- lineup snapshot ingestion works
- handedness coverage is sufficient for most lineup rows

## Checkpoint 5 — Lineup/platoon derived state
**Deliverables:**
- `team_lineup_game_state`
- `team_platoon_splits`
- fallback logic for missing lineups

**Exit criteria:**
- every target game produces usable lineup/platoon support rows
- degraded handling is explicit, not silent

## Checkpoint 6 — Feature materialization
**Deliverables:**
- `v2_phase1` materializer
- feature contract tests
- source issue propagation

**Exit criteria:**
- full historical `v2_phase1` feature backfill completes
- row counts align with expected game counts

## Checkpoint 7 — Modeling and validation pass
**Deliverables:**
- training/evaluation run on `v2_phase1`
- metric comparison vs `v1`
- degradation and coverage report

**Exit criteria:**
- feature stack is usable
- no major leakage or data integrity failures
- uplift or at least strong readiness evidence exists

## Checkpoint 8 — Daily run rehearsal
**Deliverables:**
- dry-run same-day update flow
- daily timing and API budget assessment
- failure-mode checklist

**Exit criteria:**
- same-day pipeline can assemble features near target cutoff
- fallbacks work when lineup/starter data is incomplete

---

## 12) Risks and fallback plans

## Risk 1 — Historical lineup parity is weaker than planned

### Why it matters
Lineup features are in scope, but historical announced-lineup timing may not perfectly match the chosen production cutoff.

### Fallback
- keep `game_lineup_snapshots` with explicit snapshot quality/status
- degrade rows when lineup parity is incomplete
- rely on `team_platoon_splits` + lineup known flags as the safe baseline

### Decision rule
If historical lineup quality summaries become a major implementation sink, do **not** block Phase 1. Ship lineup structure + platoon fallback first.

## Risk 2 — Top-N reliever selection is noisy

### Why it matters
If the ranking logic is too clever, the resulting features will be unstable and hard to trust.

### Fallback
- use a simple documented ranking rule in Phase 1
- version `ranking_method`
- prefer stability over perfect realism

### Decision rule
Do not introduce leverage inference or manager-role modeling in Phase 1.

## Risk 3 — API volume is higher than expected

### Why it matters
Historical backfill could become slow or brittle if it fans out too much.

### Fallback
- consolidate around game-level payloads
- use player-level lookup only for missing handedness repair
- checkpoint aggressively
- prioritize bullpen completion first if rate limits bite

## Risk 4 — Lineup quality summaries require too much new player-stat infrastructure

### Why it matters
This is the biggest Phase 1 scope-creep risk.

### Fallback
- treat lineup quality summaries as preferred, not scope-breaking
- first ship:
  - lineup structure
  - lineup known/missing status
  - team platoon features
- add richer lineup quality metrics immediately after if needed

## Risk 5 — Support-table logic becomes too opaque

### Why it matters
If derivations are buried in one giant materializer, debugging will be painful.

### Fallback
- keep derived state in support tables
- write DQ summaries per table
- sample rows during development checkpoints

---

## 13) Definition of done

Phase 1 is done when all of the following are true.

## Data / schema done
- all approved Phase 1 support tables exist
- schema init/migration is idempotent
- no existing `v1` pipeline breakage

## Historical backfill done
- `game_pitcher_appearances` is backfilled for supported seasons with acceptable coverage
- `team_bullpen_game_state` and `team_bullpen_top_relievers` are populated for supported seasons
- `game_lineup_snapshots`, `player_handedness_dim`, `team_lineup_game_state`, and `team_platoon_splits` are populated with explicit degraded handling where needed
- `v2_phase1` exists in `feature_rows` for the full historical training set

## Quality / correctness done
- leakage checks pass
- key row-count and null-rate DQ checks pass
- feature contract tests pass
- fallback behavior is explicit and tested

## Operational readiness done
- same-day rehearsal for ~1 hour before first pitch works end to end
- API usage is within acceptable bounds
- checkpoint resume works for long backfills

## Modeling handoff done
- `v2_phase1` is ready for training runs
- validation notes summarize:
  - coverage
  - degraded-rate profile
  - major limitations
  - recommended next tuning/evaluation steps

If these are not all true, Phase 1 is not done.

---

## 14) Recommended implementation order for Codex

This is the practical build order.

1. Add Phase 1 schema tables and upsert helpers
2. Build historical `game_pitcher_appearances` ingestion
3. Build `team_bullpen_game_state`
4. Build `team_bullpen_top_relievers`
5. Build `player_handedness_dim`
6. Build `game_lineup_snapshots`
7. Build `team_platoon_splits`
8. Build `team_lineup_game_state`
9. Build `v2_phase1` materializer
10. Add DQ/tests and run historical backfill validation
11. Rehearse daily near-first-pitch flow

## Why this order
- bullpen is the strongest signal and the clearest dependency chain
- lineup/platoon should build after handedness and raw snapshot capture exist
- feature materialization should come only after support tables are inspectable

---

## 15) Opinionated final recommendations

1. **Use `v2_phase1`, not an in-place `v1` rewrite.** This preserves comparability and reduces rollout risk.
2. **Build bullpen first.** It is the highest-value Phase 1 family and the cleanest to derive historically.
3. **Treat lineup quality summaries as preferred but non-blocking.** Do not let them explode scope.
4. **Derive platoon splits locally from prior games.** Do not rely on retrospective full-season team split endpoints for training.
5. **Keep degradation explicit.** Missing lineup/starter data should produce degraded rows, not silent substitutions.

---

## 16) Recommended first coding task

**First coding task for Codex:**

Implement the Phase 1 schema additions and ingestion scaffolding for `game_pitcher_appearances`, including:
- schema updates in `scripts/sql/history_schema.sql`
- idempotent migration handling in `history_ingest.py`
- upsert helper(s)
- a historical backfill command that extracts per-pitcher appearance rows from completed games with checkpointing and bounded request usage

Why this first:
- it unlocks the whole bullpen path
- it is the most concrete, lowest-ambiguity Phase 1 dependency
- it fits the current repo architecture cleanly

---

## Short close

Phase 1 should be built as an additive `v2_phase1` support-table expansion, not as a redesign.

The winning shape is:
- bullpen aggregate + top-N reliever state
- announced lineup structure + platoon context
- explicit degraded fallbacks
- chronological backfill
- same-day refresh aligned to ~1 hour before first pitch

That is the cleanest path to a materially better pregame MLB dataset without unnecessary schema churn.
