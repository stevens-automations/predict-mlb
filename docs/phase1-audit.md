# Phase 1 Audit — predict-mlb

Date: 2026-03-08  
Scope: reliability-first hardening of current MLB prediction + X posting pipeline (X-only), no new paid APIs by default.

## Current State

### Architecture (inferred)
- **Scheduler entrypoint**: `main.py`
  - Runs APScheduler daily at 09:30 ET, calls `predict.check_and_predict`.
- **Daily orchestration**: `predict.py`
  - Loads previous unchecked predictions from Excel.
  - Marks result accuracy after games finalize.
  - Generates today’s predictions using `LeagueStats.predict_game`.
  - Builds tweet lines and schedules X posts around 09:45 ET.
- **Feature/data engine**: `data.py`
  - Heavy direct use of `statsapi` for game, standings, pitcher, and leader stats.
  - Builds single-game feature row (44 train features + metadata).
  - Loads LightGBM model/scaler and predicts with light perturbation averaging.
- **Odds + tweet formatting**: `server/`
  - `get_odds.py`: fetches odds from The Odds API, caches JSON to disk.
  - `prep_tweet.py` + `tweet_generator.py`: updates odds in sheet + renders tweet lines/body.
  - `tweet.py`: posts to X via tweepy.
- **Persistence**
  - Primary runtime state is **Excel file** (`data/predictions.xlsx` from env/default).
  - Additional local files: `data/todays_odds.json`, `server/tweets.txt`.
- **Model assets**
  - Local LightGBM model file (`models/mlb4year.txt`) + scaler pickle.

### Operational posture
- Functional single-node bot implementation with moderate resilience intent (chunked historical retrieval script, retries in some loops).
- Reliability is limited by tight coupling, mutable Excel state, inconsistent path handling, and low observability.

---

## Risk Register (ranked)

1. **Critical — Incorrect probability input due to bug in `get_win_percentage`**
   - `away_pct` uses `h_wins` instead of `a_wins` in `data.py`.
   - Directly corrupts one core model feature; silent quality degradation.

2. **High — Fragile state store (Excel as source-of-truth under concurrent workflow)**
   - Read/modify/write cycles in multiple functions; lock only protects one path.
   - Failure mid-write can corrupt daily state or lose idempotency.

3. **High — Timezone/date handling errors likely**
   - Mixed naive/aware datetimes; manual `- timedelta(hours=4)` in odds processing.
   - Can mismatch “today” games, mis-schedule tweets, or skip valid games.

4. **High — String identity comparison bug**
   - `predicted_winner_location` uses `is` instead of `==` in `predict.py`.
   - Non-deterministic behavior depending on Python interning.

5. **High — Path inconsistency can break runtime**
   - Mixed absolute/relative file paths (`cwd` joins vs plain `get_data_path()` calls).
   - Running from different CWD can fail to read/write sheets/models.

6. **Medium — External API dependency fragility (statsapi + paid odds API)**
   - The Odds API is paid/credit-constrained; no robust fallback path.
   - Network/API failures are partially handled but not systematically retried/circuit-broken.

7. **Medium — Scheduler/process complexity without strong guardrails**
   - Nested scheduler pattern (`main.py` + per-run scheduler in `predict.py`) increases failure modes.
   - Recovery/at-least-once semantics are not explicit.

8. **Medium — Minimal validation and schema checks**
   - DataFrame columns and row assumptions are implicit; missing defensive checks.
   - Example: direct index access `df.loc[...,].index[0]` may throw if missing.

9. **Low/Medium — Security/secrets hygiene not enforced in code**
   - Relies on `.env`; no startup checks for required vars, no secret scope validation.

10. **Low — Testing/CI absent**
   - No automated regression tests for core transformations, scheduling, or tweet assembly.

---

## Refactor Opportunities (ranked)

1. **Fix correctness defects first (small, high ROI)**
   - `away_pct` formula bug, string `is` comparison, datetime normalization.

2. **Replace Excel runtime state with SQLite (or at minimum strict atomic writes)**
   - Keep same columns, add unique constraints (`game_id`, `date`), transaction-safe updates.
   - If Excel must remain for now: write temp + atomic rename, centralize all sheet IO.

3. **Create a single orchestration service with explicit phases**
   - `check_results -> generate_predictions -> queue_posts -> dispatch_posts`.
   - Eliminate nested scheduler where possible.

4. **Normalize time handling end-to-end**
   - Use timezone-aware UTC internally, convert to ET only at display boundaries.

5. **Introduce adapter interfaces for external data sources**
   - `OddsProvider`, `StatsProvider`, `Poster` abstraction.
   - Enables no-paid-API default via free/local fallback providers.

6. **Centralized config + startup validation**
   - One config module; validate env vars, paths, model files, writable dirs on startup.

7. **Idempotency and replay protection**
   - Deterministic job keys (`date+game_id+tweet_type`), avoid duplicate posts on restart.

8. **Observability improvements**
   - Structured logging and daily run summary (counts predicted, posted, skipped, failed).

9. **Test harness for critical functions**
   - Unit tests for feature calculations, odds merge, tweet generation, schedule decisions.

---

## Execution Plan

### Wave 1 (Reliability Patch Set, 1–3 days)
- Patch known correctness bugs:
  - `away_pct` calculation.
  - `is` → `==` for winner location.
  - Remove manual ET offset subtraction; standardize parsing.
- Harden file/path usage:
  - Canonical absolute path resolver used everywhere.
- Add preflight checks:
  - Validate required env vars and model/scaler existence.
- Add robust guards:
  - Defensive checks for empty DF lookups, missing odds, missing columns.
- Add minimal logging standard:
  - run_id, stage, game_id, outcome.

### Wave 2 (State + Idempotency, 3–7 days)
- Introduce storage abstraction (`PredictionStore`).
- Migrate runtime state from Excel to SQLite (keep optional Excel export).
- Enforce unique keys and posted flags transactionally.
- Implement idempotent posting logic and safe retries with backoff.
- Simplify scheduler model (single scheduler, explicit jobs).

### Wave 3 (Provider decoupling + quality, 1–2 weeks)
- Build provider interfaces and pluggable implementations:
  - `OddsProviderPaid`, `OddsProviderFree/None` (default no new paid APIs).
- Add integration tests with fixture snapshots for one full day pipeline.
- Add dry-run mode for X-only pipeline verification without posting.
- Add lightweight healthcheck endpoint/CLI summary for daily ops.

---

## Open Decisions

1. **State backend now**: SQLite immediately vs phased Excel-hardening first?
2. **Odds policy**: keep The Odds API (existing) or switch to free/no-odds fallback by default?
3. **Posting strategy**: continue direct tweepy posting or queue + worker model?
4. **Schedule behavior after missed window**: post immediately, delay fixed amount, or skip?
5. **Model governance**: retraining cadence and acceptance criteria before model swap?

---

## First Build Slice (recommended)

**Goal:** Ship a low-risk reliability slice that does not change product behavior.

### Slice contents
1. Fix `away_pct` bug and `is` comparison bug.
2. Introduce `paths.py` helper and route all file access through it.
3. Introduce `time_utils.py` and remove manual timezone offset math.
4. Add startup `validate_runtime()` with clear failure messages.
5. Add minimal tests:
   - `test_get_win_percentage_away_pct()`
   - `test_predicted_winner_location_equality()`
   - `test_today_filter_timezone_consistency()`

### Acceptance criteria
- Existing daily flow still runs end-to-end.
- No regressions in tweet output format.
- Deterministic test pass locally.
- Logs clearly show phase outcomes and failures.

### Why this slice first
- Removes known silent prediction-quality defect.
- Reduces most likely day-of failures.
- Keeps blast radius small before state-store migration.
