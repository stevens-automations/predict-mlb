# predict-mlb: Daily Pipeline Spec
Last updated: 2026-03-30

## Overview

A single always-running Python process (`run_daily.py`) manages all scheduled jobs via APScheduler and serves a FastAPI dashboard.

- Startup: `./start.sh` → scheduler + FastAPI backend
- Dashboard: `http://localhost:8765`

## Architecture

```
run_daily.py                  ← APScheduler (BlockingScheduler) entry point
start.sh                      ← shell script: starts run_daily.py + uvicorn
server/
  api.py                      ← FastAPI app, reads from SQLite, serves dashboard
  static/                     ← Web dashboard
  tweet_generator_llm.py      ← Tweet text generation (deterministic default)
scripts/
  jobs/
    ingest_yesterday.py       ← pull completed game data → update raw DB tables
    update_layer2.py          ← recompute Layer 2 derived feature tables
    evaluate_yesterday.py     ← score yesterday's predictions vs actual results
    fetch_todays_games.py     ← pull today's schedule from statsapi
    fetch_odds.py             ← pull today's odds from The Odds API
    predict_today.py          ← compute features + run model → write predictions
    schedule_tweets.py        ← score interestingness, mark tweet-eligible games
    post_tweet.py             ← manual CLI tweet poster
  inference/
    feature_builder.py        ← compute live feature row for a single game
    scorer.py                 ← load model, return win_prob + confidence tier
    explainer.py              ← SHAP-based prediction explanations
```

## Morning Chain (8:00 AM ET)

Jobs run sequentially. Non-critical jobs (steps 1–3) are error-isolated; failures log but don't abort the chain. Steps 4–7 are critical path.

### Step 1: `ingest_yesterday`
- Pull completed MLB games from yesterday via statsapi
- Upsert into `games`, `game_team_stats`, `game_pitcher_appearances`, `game_lineup_snapshots`
- Idempotent — safe to re-run. Retries on transient API failures.

### Step 2: `update_layer2`
- Recompute Layer 2 derived tables for yesterday's games
- Tables: `team_pregame_stats`, `starter_pregame_stats`, `bullpen_pregame_stats`, `lineup_pregame_context`, `team_vs_hand_pregame_stats`

### Step 3: `evaluate_yesterday`
- Compare yesterday's predictions against actual results
- Update `daily_predictions`: `actual_winner`, `did_predict_correct`, `home_score`, `away_score`
- Log accuracy to `pipeline_log`
- On Mondays: generates weekly recap (season W/L record)

### Step 4: `fetch_todays_games`
- Pull today's schedule from statsapi
- Write to `today_schedule` table
- Filter: regular season only (game_type='R')

### Step 5: `fetch_odds`
- Pull from The Odds API v4 (h2h, us region, American format)
- Cache with 23h TTL — skip if fresh cache exists
- Select best odds per team across bookmakers
- Write to `today_schedule`

### Step 6: `predict_today`
- For each game: build features → score model → run SHAP explainer
- Compute implied odds, odds gap, interestingness score
- Generate tweet text for eligible games
- Write to `daily_predictions` (idempotent — skips existing)

### Step 7: `register_tweet_jobs`
- For tweet-eligible games: schedule APScheduler job 1 hour before first pitch
- Only fires for games with `tweet_eligible = 1` and `tweet_text IS NOT NULL`

## Tweet Eligibility Logic

A game is tweet-eligible when ALL conditions are met:
1. `confidence_tier` is `medium` (0.60–0.65) or `high` (>= 0.65)
2. `tweet_score >= 2` (interestingness scoring — odds gap, SHAP strength, underdog status)
3. Max 3 tweets per day (top 3 by score)

Interestingness scoring:
- +3: odds gap >= 30 ML points favoring our pick
- +2: high confidence tier
- +2: strong SHAP factor (|shap| >= 0.04)
- +1: medium confidence tier
- +1: predicted winner is market underdog

## Dashboard Endpoints

- `GET /api/predictions/today` — today's predictions with confidence + tweet status
- `GET /api/predictions/{date}` — predictions for any date
- `GET /api/log` — last 50 pipeline_log entries
- `GET /api/accuracy` — season accuracy by confidence tier
- `GET /api/status` — scheduler status, last job run times

## Database Tables (Live Pipeline)

### `daily_predictions`
Primary output table. One row per game per day. Key columns: `game_id`, `game_date`, `predicted_winner`, `home_win_prob`, `confidence_tier`, `implied_home_ml`, `odds_gap`, `shap_reasons_json`, `tweet_score`, `tweet_eligible`, `tweet_text`, `tweeted`, `actual_winner`, `did_predict_correct`.

### `today_schedule`
Today's game schedule with odds. Refreshed each morning. Key columns: `game_id`, `game_date`, `home_team`, `away_team`, `first_pitch_et`, `home_odds`, `away_odds`.

### `pipeline_log`
Append-only job execution log. Columns: `id`, `ts`, `job`, `status`, `message`, `duration_s`.

### Layer 2 Tables (Feature Engineering)
- `team_pregame_stats` — cumulative team batting/record stats
- `starter_pregame_stats` — starter ERA/WHIP/K% from appearances
- `bullpen_pregame_stats` — bullpen quality + fatigue metrics
- `lineup_pregame_context` — lineup handedness vs opposing starter
- `team_vs_hand_pregame_stats` — team OPS vs LHP/RHP
- `player_career_pitching_stats` — career pitching stats
- `game_matchup_features` — flat training/inference feature rows

## Key Constraints
- One odds API call per day (23h TTL cache)
- All jobs idempotent — safe to re-run
- Tweet timing: 1 hour before each game's first pitch
- No data loss on restart — all state in SQLite
- No LLM calls at runtime by default (USE_LLM=False)
