# Model Data Feasibility Audit

Date: 2026-03-09
Scope: Assess whether the current historical ingestion schema and currently populated data in `predict-mlb` are sufficient to train a feasible game-winner model that can match or beat prior model performance, given the legacy workflow and expected inference-time parity.

## Executive Verdict

Short answer: **No, not yet.**

The current historical store is sufficient for a **schedule/label baseline only**, but it is **not sufficient for parity with the old 44-feature LightGBM workflow** and is also **not sufficient for a credible improvement program** beyond very simple priors. The main blockers are:

1. The database is currently populated with only **schedule-level game rows and labels**, not pregame feature snapshots.
2. The richer schema tables (`game_team_stats`, `game_pitcher_context`, `feature_rows`) are currently **empty**.
3. The old workflow depended on several feature families that are either missing outright or, in the legacy implementation, were retrieved in a way that created **train/inference parity problems and likely leakage**.
4. The currently populated dataset is only **season 2020** (`953` games, `951` labels), which is too narrow to expect stable parity with the old reported multi-season model.

Recommended decision: **Do not start parity-model training yet.** Start only a limited baseline experiment using current schedule/label data, while first adding leakage-safe pregame feature backfill for team-form and probable-starter context.

## Evidence Reviewed

### Current ingestion and schema

- `scripts/history_ingest.py`
- `scripts/sql/history_schema.sql`
- `docs/STATUS.md`
- `docs/PLAN.md`

### Legacy modeling workflow

- `README.md`
- `data.py`
- `legacy/notebooks/mlb-predict.ipynb`

### Actual local DB state observed

From `data/mlb_history.db` on 2026-03-09:

- `games`: `953`
- `labels`: `951`
- `game_team_stats`: `0`
- `game_pitcher_context`: `0`
- `feature_rows`: `0`
- `odds_snapshot`: `0`

Observed season coverage:

- `2020`: `953` games, `951` labels
- date span: `2020-07-23` to `2020-10-27`

This means the database is **not empty**, but it is still far from a usable historical feature store.

## Current Historical Store: What Exists vs What Is Populated

### Schema capability

The schema can hold:

- basic game schedule/outcome data in `games`
- game labels in `labels`
- per-game team stats in `game_team_stats`
- limited pitcher context in `game_pitcher_context`
- generic versioned feature snapshots in `feature_rows`
- forward-only odds snapshots in `odds_snapshot`

Relevant references:

- `games` / `labels`: `scripts/sql/history_schema.sql`
- generic feature storage: `feature_rows`
- richer context tables exist but are not wired into current ingestion

### What current ingestion actually writes

`scripts/history_ingest.py` currently fetches only `statsapi.schedule(...)` rows and writes:

- `games`
- `labels` for final games
- ingestion ledgers/checkpoints

It does **not** populate:

- `game_team_stats`
- `game_pitcher_context`
- `feature_rows`
- `odds_snapshot`

Relevant implementation facts:

- `fetch_schedule_bounded(...)` only wraps `statsapi.schedule(...)`
- `game_row_from_schedule(...)` builds schedule-level game rows
- `label_row_from_game(...)` derives winner/score labels
- there is no write path for richer feature tables

### Documentation mismatch

There is a planning/status mismatch worth noting:

- `docs/STATUS.md` says the historical training dataset is not yet materialized.
- `docs/PLAN.md` and `docs/research/historical-mlb-ingestion-architecture.md` still describe backfill/incremental as scaffold or safe stub.
- In reality, `scripts/history_ingest.py` now contains functioning bounded schedule backfill/incremental loops, and `data/mlb_history.db` already contains a real 2020 season pull.

This mismatch is not just cosmetic. It creates planning risk because the repo narrative still implies less data exists than actually does, but the actual populated data is also much thinner than the schema names imply.

## Legacy Feature Set and Training Assumptions

The old model used one row per game in home-vs-away format with **44 trainable features** plus label/non-train columns.

Primary feature families in the old pipeline:

1. **Team season win percentage**
   - `home-win-percentage`
   - `away-win-percentage`

2. **Last-10-day team form**
   - runs
   - runs allowed
   - hits
   - hits allowed
   - OPS
   - strikeouts
   - OBP
   - AVG
   - RBI
   - all duplicated for home and away

3. **Starting pitcher statistics**
   - career ERA
   - season ERA
   - season AVG allowed
   - season runs per 9
   - season win percentage
   - season WHIP
   - season strike percentage
   - for home and away probable starters

4. **Top-5 player averages**
   - top-5 HR average
   - top-5 RBI average
   - top-5 batting average
   - top-5 stolen bases average
   - top-5 total bases average
   - for home and away teams

5. **Label pipeline**
   - binary label `did-home-win`
   - non-training columns dropped before model fit: `game-id`, `date`, `home-team`, `away-team`

6. **Training assumptions**
   - missing-value tolerance: drop rows missing more than `10` fields
   - min-max scaling fit on full selected data
   - random row shuffle
   - `85/15` train/test split
   - LightGBM binary classifier
   - README reports about `66%` test accuracy for the `mlb4year` model using 2020-2023 data

## Gap Analysis by Legacy Feature Family

Status definitions used here:

- **Captured**: the current store has the data at usable fidelity for historical training with inference-time parity
- **Partially captured**: some ingredients exist, but not enough to reproduce the feature family safely
- **Missing**: not currently stored in usable historical form

| Legacy feature family | Current status | Why |
| --- | --- | --- |
| Label: `did-home-win` | Captured | `labels` contains home/away score, winner, run differential, total runs. |
| Basic game identity/date/home-away teams | Captured | `games` has `game_id`, `season`, `game_date`, `home_team_id`, `away_team_id`, schedule metadata. |
| Team season win percentage prior to game | Partially captured | Raw game outcomes exist, so prior win pct can be derived offline from `games`/`labels`, but no explicit as-of snapshot exists. |
| Last-10-day team form | Partially captured in theory, missing in practice | Could be derived offline from historical game outcomes if enough per-game team stats existed. But `game_team_stats` is empty, and `games`/`labels` alone do not contain OPS/OBP/AVG/RBI/strikeout inputs. |
| Last-10-day runs and runs allowed | Partially captured | These can be derived from `labels` plus home/away orientation. |
| Last-10-day hits / hits allowed | Missing | Requires boxscore/team stat backfill; `game_team_stats` is empty. |
| Last-10-day OPS / OBP / AVG / RBI / strikeouts | Missing | Requires richer batting/pitching stat history; not stored now. |
| Starting pitcher identity for each game | Missing | Current schema does not store actual/probable starter ids in populated data. `game_pitcher_context` is empty. |
| Starting pitcher career stats | Missing | Not stored historically. |
| Starting pitcher season-to-date stats as of game time | Missing | Not stored historically; cannot be reconstructed from current populated tables. |
| Top-5 player leader averages | Missing | No player-level or team-leader snapshot tables are populated. |
| Generic feature snapshot row | Missing in practice | `feature_rows` exists but has zero rows. |
| Historical odds | Missing by policy | `odds_snapshot` exists but is intentionally forward-only. |

## Leakage Risk and Train/Inference Parity Risk

This is the most important section for judging whether old performance is reproducible.

### 1. Old `top5` team-leader features are high-risk for leakage

In `data.py`, `get_team_leaders(...)` calls `statsapi.team_leader_data(team_id, stat, season=season)` and uses season-level leader totals. For historical training rows, that likely returns the team leaders for the **full season**, not the values as of the game date.

Implication:

- A July 2020 training row can contain player leader totals accumulated in August/September/October 2020.
- At inference time for an upcoming game, the same code would only have access to the current partial season.

This is a direct **train/inference mismatch** and likely a meaningful source of **future leakage** in the historical training set.

### 2. Old starting-pitcher season stats are also high-risk for leakage

In `get_starting_pitcher_stats(...)`, the code fetches `yearByYear` season stats for the pitcher's season and uses those values directly. For historical training, that strongly suggests use of the pitcher’s **end-of-season aggregate line**, not a game-date snapshot.

Implication:

- A game early in the season may be trained using a starter’s season ERA/WHIP/win% that already includes many future starts.
- At prediction time, inference only has current season-to-date stats.

This again creates **parity failure** and likely inflates historical backtest performance.

### 3. Team win percentage is lower-risk but still ambiguous

`get_win_percentage(...)` uses division standings with `request_date=game_date`. If the underlying API resolves standings as-of the beginning of that date, it is acceptable. If it resolves after that day’s games, it leaks the target game result.

I cannot prove the exact semantics from local code alone, so the correct conclusion is:

- **Leakage risk: medium**
- It should not be trusted as a parity-safe feature source without explicit contract testing

### 4. Last-10-day form is the cleanest part of the old design

`get_last10_stats(...)` uses games from `game_date - 11 days` through `game_date - 1 day`, so it intentionally excludes the target game. That is good.

However:

- it still depends on boxscore-derived stats not currently backfilled into the new DB
- it was fetched on demand from live API calls rather than from versioned feature snapshots

So the *logic* is comparatively safe, but the *current historical store* does not support it.

### 5. Old evaluation was optimistic even aside from feature leakage

The notebook shuffles all rows randomly and then uses an `85/15` split. That means:

- training and test examples from the same season regime are mixed together
- there is no walk-forward separation
- the scaler is fit before split, which is a minor evaluation leak

Therefore the legacy reported `~66%` accuracy should be treated as **not directly comparable** to a leakage-safe, parity-safe rebuild.

## Sufficiency Assessment

### A. Is the current schema, as actually populated now, sufficient for baseline model parity with the old approach?

**No.**

Why:

1. The old approach depended heavily on feature families that are absent in the populated DB.
2. Only 2020 is present in the local history DB.
3. No pregame feature snapshots exist.
4. No starter identity/history is stored.
5. No player-depth or richer team-form metrics are stored.

The current DB can support only a much simpler baseline than the old 44-feature model.

### B. Is the current schema, as actually populated now, sufficient for improved model performance potential?

**Not yet in populated form. Conditionally yes in structural form.**

Interpretation:

- The schema has a generic place to hold richer versioned features (`feature_rows`), which is enough to support a good future design.
- But the actual data now does not provide enough signal to meaningfully improve beyond simple priors/home-field/team-strength estimates.

So:

- **Current population**: insufficient
- **Schema direction**: potentially sufficient if feature snapshots are actually materialized and governed

## Concrete Recommendation

### Is the current pull strategy sufficient now?

**Conditional no.**

It is sufficient only for:

- schedule/outcome backfill
- label generation
- a simple baseline experiment

It is not sufficient for:

- parity with the old model
- leakage-safe recreation of old feature families
- a meaningful model-improvement loop

### Minimal additional data pulls/tables required before serious training should start

The minimum requirement is not historical odds. It is **historical pregame feature state**.

At minimum, add the following before parity-level training:

1. **Probable starter snapshot by game/side**
   - `game_id`
   - `side`
   - `pitcher_id`
   - `pitcher_name`
   - source/update timestamp

2. **Pitcher as-of-date season snapshot**
   - keyed by `(pitcher_id, as_of_date)` or materialized directly into game feature rows
   - ERA, WHIP, AVG allowed, runs/9, strike%, win%
   - must reflect values **before first pitch**

3. **Team as-of-date rolling/season snapshot**
   - keyed by `(team_id, as_of_date)` or materialized into game feature rows
   - season win percentage prior to game
   - rolling offensive/defensive stats used for last-10 form
   - at minimum: runs, runs allowed
   - parity-level: hits, hits allowed, OPS, OBP, AVG, RBI, strikeouts

4. **Team depth/offense snapshot**
   - if you want strict old-feature parity, store the top-5 leader aggregates as-of date
   - if not, replace this family with cleaner team-level batting-strength aggregates computed from roster or rolling batting stats

5. **Versioned game-level feature materialization**
   - use `feature_rows` or an equivalent derived table as the canonical training source
   - every row must be tied to an explicit `as_of_ts`

### Recommended table strategy

`feature_rows` can be the final canonical training table, but relying on only a JSON blob is weak for auditing and DQ. A practical minimum is:

1. `game_probable_starters` or expand `game_pitcher_context` into a true pregame snapshot table
2. `team_daily_features`
3. `pitcher_daily_features`
4. `feature_rows` as the final assembled training row

If the team wants to avoid more tables, it can still use `feature_rows`, but then the feature assembly job must be strong enough to produce reproducible, contract-checked as-of snapshots. Right now that assembly path does not exist.

### What can be trained immediately right now as a baseline?

With current data only, you can train a **schedule/label baseline**, for example:

1. home-field baseline
2. team identity strength baseline using home/away team ids and season priors
3. rolling win-percentage / rolling run-differential baseline derived only from past `games` + `labels`

This baseline would be useful for:

- pipeline validation
- dataset materialization checks
- walk-forward experiment plumbing
- establishing a low bar to beat

It would **not** be a valid proxy for old-model parity.

## Practical Phased Plan for Training Readiness

### Phase A: immediate baseline from current data

Goal: validate training pipeline and establish a leakage-safe floor using only currently populated tables.

Use:

- `games`
- `labels`

Features:

- home/away team ids
- season/day-of-season or month
- home prior win percentage
- away prior win percentage
- home/away prior run differential or rolling runs scored/allowed from labels only
- home-field intercept

Required evaluation approach:

- time-ordered split or walk-forward only
- log loss as primary metric, accuracy secondary

Expected outcome:

- workable baseline
- not parity with old model

### Phase B: parity-level features

Goal: reproduce the useful parts of the old model with train/inference parity.

Add:

- probable starters by game
- pitcher season-to-date snapshots
- team rolling batting/pitching form snapshots
- either top-5 leader aggregates as-of date or a replacement team-strength family
- populated `feature_rows`

Requirements:

- every feature must be computable from information available pregame
- no end-of-season summaries for historical rows
- DQ checks for missingness, freshness, and impossible values

Expected outcome:

- first serious parity attempt
- lower leakage than the old workflow
- likely more realistic but possibly lower headline backtest accuracy than the legacy random-split result

### Phase C: potential uplift features

Goal: move beyond old parity once the base historical store is sound.

Candidates:

- bullpen rest/workload
- travel/rest days
- lineup confirmation / scratched starter handling
- richer pitcher rolling form
- market priors from forward-captured odds during current season only
- calibration and threshold policy improvements

Expected outcome:

- real opportunity for uplift, but only after Phase B parity-safe data is stable

## Final Answer to the Goal Question

Is the current historical ingestion schema/data strategy sufficient to train a feasible game-winner model that can match or beat prior model performance?

**No, not in its current populated state.**

More precise answer:

- **Feasible model at all:** yes, but only a simple schedule/outcome baseline
- **Match old model architecture/features:** no
- **Credibly beat old model performance:** no, not before parity-safe feature backfill
- **Long-term schema direction:** acceptable if `feature_rows` becomes a real, versioned pregame feature store and if starter/team snapshots are backfilled

## Confidence Rating

**Confidence: 0.84 / 1.00**

This is high-confidence on the main conclusion because:

- the current DB contents are directly inspectable
- the current ingestion code clearly only writes schedule/label data
- the legacy feature families are explicit in `README.md`, `data.py`, and `legacy/notebooks/mlb-predict.ipynb`

The remaining uncertainty is mainly around exact historical semantics of specific `statsapi` endpoints used in the old workflow.

## Top Risks

1. **Historical leakage disguised as good prior performance**
   - especially season-level pitcher stats and team-leader features

2. **Train/inference parity failure**
   - training on end-of-season aggregates, predicting with in-season partial aggregates

3. **Insufficient historical breadth**
   - only 2020 is currently populated, which is not enough for stable model selection

4. **False sense of readiness from schema shape**
   - existing empty tables suggest richer data than actually exists

5. **Evaluation optimism**
   - old random split and full-dataset scaler fitting likely overstated performance

## Recommended Next Decision

Proceed in this order:

1. Train a very small leakage-safe baseline from current `games` + `labels`.
2. Before any parity claim, backfill pregame starter and team-form snapshots into a reproducible feature store.
3. Evaluate only with time-ordered validation.
4. Treat the old `~66%` accuracy as a historical reference point, not as a trustworthy parity target until the leakage issues are removed.
