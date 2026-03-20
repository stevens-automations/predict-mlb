# predict-mlb: Daily Pipeline Spec
Last updated: 2026-03-19
Status: Approved for implementation

---

## Overview

A single always-running Python process (`run_daily.py`) that:
1. Manages all scheduled daily jobs via APScheduler
2. Maintains a FastAPI backend serving pipeline state/logs
3. Is started manually from terminal, runs in foreground (nohup-able), resumable on restart

Startup: `./start.sh` → launches scheduler + FastAPI backend
Dashboard: `http://localhost:8765` (read-only for MVP)

---

## Architecture

```
run_daily.py                  ← APScheduler (BlockingScheduler) entry point
start.sh                      ← shell script: starts run_daily.py + uvicorn in background
server/
  api.py                      ← FastAPI app, reads from SQLite, serves dashboard data
  static/                     ← React dashboard build output (served by FastAPI)
scripts/
  jobs/
    ingest_yesterday.py       ← pull completed game data → update raw DB tables
    update_layer2.py          ← recompute Layer 2 derived feature tables
    evaluate_yesterday.py     ← score yesterday's predictions vs actual results
    fetch_todays_games.py     ← pull today's schedule + game times from statsapi
    fetch_odds.py             ← pull today's odds from The Odds API (once/day, cached)
    predict_today.py          ← compute features + run model → write predictions
    schedule_tweets.py        ← compute tweet time per game (1hr before first pitch)
  inference/
    feature_builder.py        ← compute live feature row for a single game
    scorer.py                 ← load model artifact, return win_prob + confidence tier
```

---

## Daily Job Schedule

Jobs fire once per day, chained in sequence starting at 8:00 AM ET.
APScheduler fires the first job; each job triggers the next on completion.

### 8:00 AM — Morning chain

**Step 1: `ingest_yesterday`**
- Pull all MLB games from yesterday via `statsapi.schedule(date=yesterday)`
- For each completed game: upsert into `game_team_stats`, `game_pitcher_appearances`, `game_lineup_snapshots`
- Idempotent — safe to re-run

**Step 2: `update_layer2`**
- Recompute all Layer 2 derived tables for yesterday's games:
  - `team_pregame_stats`, `starter_pregame_stats`, `bullpen_pregame_stats`
  - `lineup_pregame_context`, `team_vs_hand_pregame_stats`
- Only recomputes rows affected by yesterday's new data

**Step 3: `evaluate_yesterday`**
- Pull yesterday's actual results from DB labels
- Compare vs `daily_predictions` where `game_date = yesterday`
- Update: `actual_winner`, `did_predict_correct`, `home_score`, `away_score`
- Log daily accuracy stats to `pipeline_log` table

**Step 4: `fetch_todays_games`**
- Pull today's schedule from `statsapi.schedule(date=today)`
- Write to `today_schedule` table: game_id, home_team, away_team, first_pitch_et
- Filter: regular season only (game_type='R')

**Step 5: `fetch_odds`**
- Pull from The Odds API v4 (h2h, us region, American format)
- Cache to `data/todays_odds.json` (don't re-fetch if file exists and < 23h old)
- For each game: find best odds per team across all bookmakers (same logic as old project — pick highest underdog odds to make predictions look most attractive)
- Write odds to `today_schedule` table

**Step 6: `predict_today`**
- For each game in `today_schedule`:
  - Call `inference/feature_builder.py` to compute live feature row using current Layer 2 tables
  - Call `inference/scorer.py` to get `win_prob` (home win probability) + `confidence_tier`
  - Determine predicted winner
- Write all predictions to `daily_predictions` table
- Skip games already in `daily_predictions` for today (idempotent)

**Step 7: `schedule_tweets`**
- For each prediction where `confidence_tier IN ('high', 'medium')` (win_prob ≥ 0.60):
  - Compute `tweet_at = first_pitch_et - 1 hour`
  - If tweet_at is in the past (e.g. early games): tweet immediately
  - Register tweet job with APScheduler at `tweet_at` time
- Write `tweet_scheduled_at` to `daily_predictions`

### Dynamic tweet jobs (variable time, per game)
- Fire 1 hour before each predicted game's first pitch
- Format tweet line for that game (winner + win probability + odds if available)
- Post via Tweepy
- Update `daily_predictions.tweeted = 1`

### 11:00 PM — `evaluate_yesterday` cleanup sweep
- Re-run evaluation for any games that finished late
- Update any remaining NULL `did_predict_correct` rows

---

## Database Tables

### `daily_predictions`
```sql
CREATE TABLE IF NOT EXISTS daily_predictions (
    game_id             INTEGER PRIMARY KEY,
    game_date           TEXT NOT NULL,
    home_team           TEXT,
    away_team           TEXT,
    home_team_id        INTEGER,
    away_team_id        INTEGER,
    first_pitch_et      TEXT,
    predicted_winner    TEXT,
    home_win_prob       REAL,
    confidence_tier     TEXT,  -- 'high' (≥0.65), 'medium' (0.60-0.65), 'low' (<0.60)
    home_odds           TEXT,
    away_odds           TEXT,
    best_odds_bookmaker TEXT,
    tweet_scheduled_at  TEXT,
    tweeted             INTEGER DEFAULT 0,
    actual_winner       TEXT,
    home_score          INTEGER,
    away_score          INTEGER,
    did_predict_correct INTEGER,  -- 1/0/NULL until result known
    result_tweeted      INTEGER DEFAULT 0,
    created_at          TEXT DEFAULT (datetime('now')),
    updated_at          TEXT DEFAULT (datetime('now'))
);
```

### `today_schedule`
```sql
CREATE TABLE IF NOT EXISTS today_schedule (
    game_id         INTEGER PRIMARY KEY,
    game_date       TEXT,
    home_team       TEXT,
    away_team       TEXT,
    home_team_id    INTEGER,
    away_team_id    INTEGER,
    first_pitch_et  TEXT,
    home_odds       TEXT,
    away_odds       TEXT,
    odds_bookmaker  TEXT,
    fetched_at      TEXT
);
```

### `pipeline_log`
```sql
CREATE TABLE IF NOT EXISTS pipeline_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT DEFAULT (datetime('now')),
    job         TEXT,
    status      TEXT,  -- 'started'|'completed'|'failed'
    message     TEXT,
    duration_s  REAL
);
```

---

## Inference Feature Builder (`scripts/inference/feature_builder.py`)

For a given game_id (or home_team_id, away_team_id, game_date, probable_starter_ids):
1. Load latest `team_pregame_stats` for home + away team (most recent row for current season)
2. Load `starter_pregame_stats` for probable starters (most recent season row)
3. Load `player_career_pitching_stats` for career ERA fallback
4. Load `bullpen_pregame_stats` for home + away (most recent)
5. Load `lineup_pregame_context` if lineup announced (most recent)
6. Load `team_vs_hand_pregame_stats` for each team vs opposing starter hand
7. Fetch weather from Open-Meteo forecast API for venue lat/lon
8. Assemble flat feature dict matching exactly the FEATURE_COLS used in training
9. Return as dict (scorer handles conversion to model input)

**Cold-start handling:**
- If team has < 15 games: use prior season's final `team_pregame_stats` row as warm-start
- If starter has 0 starts this season: use career ERA, set `stats_available_flag=0`
- If lineup unknown: set handedness features to NULL (LightGBM handles natively)

---

## Scorer (`scripts/inference/scorer.py`)

- Load model from `artifacts/model_registry/matchup_lgbm_v4_tuned__*/model.pkl` (or latest)
- Accept feature dict from feature_builder
- Return: `{"home_win_prob": 0.62, "predicted_winner": "NYY", "confidence_tier": "medium"}`
- Confidence tiers: high ≥ 0.65, medium 0.60-0.65, low < 0.60

---

## FastAPI Dashboard Backend (`server/api.py`)

Endpoints (read-only MVP):
- `GET /api/predictions/today` — today's predictions with confidence + tweet status
- `GET /api/predictions/{date}` — predictions for any date
- `GET /api/log` — last 50 pipeline_log entries
- `GET /api/accuracy` — season accuracy by confidence tier
- `GET /api/status` — scheduler status, last job run times

---

## React Dashboard (`server/static/`)

Simple single-page app. Sections:
1. **Today's Predictions** — table: away @ home, predicted winner, win prob, confidence, tweet time, odds
2. **Recent Activity** — last 20 pipeline log entries with status icons
3. **Season Stats** — accuracy overall + by confidence tier, record (W-L)
4. **Yesterday's Results** — how we did yesterday

Stack: React + fetch (no Redux needed). Build output goes to `server/static/`. FastAPI serves it.

---

## Startup Script (`start.sh`)

```bash
#!/bin/bash
cd "$(dirname "$0")"
source .venv/bin/activate

# Start FastAPI dashboard in background
uvicorn server.api:app --host 0.0.0.0 --port 8765 --reload &
UVICORN_PID=$!
echo "Dashboard: http://localhost:8765 (PID $UVICORN_PID)"

# Start scheduler in foreground (blocking)
python run_daily.py

# Cleanup on exit
kill $UVICORN_PID 2>/dev/null
```

Run: `nohup ./start.sh > logs/run.log 2>&1 &`
Stop: Ctrl+C or kill the process

---

## Key Constraints
- **One odds API call per day** — cache to `data/todays_odds.json`, check age before re-fetching
- **Best odds logic** — for each team, take highest (most favorable to underdog) odds across all bookmakers
- **Idempotent jobs** — all jobs safe to re-run without duplicating data
- **Confidence threshold** — only tweet games with win_prob ≥ 0.60 (configurable via env var `MIN_TWEET_CONFIDENCE=0.60`)
- **Tweet timing** — 1 hour before each game's first pitch, per game (not a single batch time)
- **No data loss on restart** — all state in SQLite, scheduler re-reads DB on start to pick up pending tweets
- **No OpenClaw at runtime** — fully standalone, no LLM calls during operation
- **Twitter credentials** — via `.env`: `TWITTER_API_KEY`, `TWITTER_API_SECRET`, `TWITTER_ACCESS_TOKEN`, `TWITTER_ACCESS_TOKEN_SECRET`

---

## Build Order (priority for March 26 deadline)
1. `scripts/inference/feature_builder.py`
2. `scripts/inference/scorer.py`
3. `scripts/jobs/fetch_todays_games.py`
4. `scripts/jobs/fetch_odds.py`
5. `scripts/jobs/predict_today.py`
6. `scripts/jobs/ingest_yesterday.py`
7. `scripts/jobs/update_layer2.py`
8. `scripts/jobs/evaluate_yesterday.py`
9. `run_daily.py` (APScheduler wiring)
10. `server/api.py` (FastAPI)
11. `server/static/` (React dashboard)
12. `start.sh`
13. Tweet jobs (deferred — scaffold only, no posting)

## Out of Scope for MVP
- Tweet posting (scaffold + format logic only, no actual posting)
- LLM-generated tweet text
- Dashboard edit capability
- Model retraining automation
