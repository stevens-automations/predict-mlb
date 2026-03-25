# predict-mlb: Daily Operations Plan
Last updated: 2026-03-18

## Overview
A single long-running Python process (`run_daily.py`) manages all scheduled tasks using APScheduler (BlockingScheduler — same pattern as original predictMLB). Runs as a background process on the Mac mini. No OpenClaw involvement at runtime.

Season start: **March 26, 2026 (Wednesday)**

---

## Daily Schedule (all times ET)

| Time | Job | Description |
|------|-----|-------------|
| 8:00 AM | `ingest_yesterday` | Pull completed game data → update raw DB tables |
| 8:15 AM | `update_layer2` | Recompute Layer 2 derived stats using updated raw data |
| 8:30 AM | `predict_today` | Fetch today's schedule, compute features, score with model |
| 8:45 AM | `tweet_predictions` | Post prediction tweets (one thread per tweet) |
| 9:30 AM | `tweet_results` | Post yesterday's result accuracy tweet |
| 11:00 AM | `lineup_sweep` | Re-fetch announced lineups, recompute handedness features |
| 6:30 PM | `lock_predictions` | Final check, lock all predictions before first pitch |
| Monday 8 AM | `weekly_recap` | Season W/L + biggest upset callout (logged, not yet posted) |

---

## Tweet Format (redesigned from old project)

### Morning predictions tweet(s)
Old format used odds (which we don't have a free source for). New format:

```
MLB Predictions — March 26, 2026

• NYY (78%) to defeat BOS
• LAD (65%) to defeat SF
• ATL (61%) to defeat NYM
• HOU (59%) to defeat SEA
[...]
```

- Use win probability % instead of odds (we compute this from model)
- Confidence tier emoji: 🔥 ≥65%, ⚡ 60-65%, — below 60%
- Multi-tweet thread if >6 games (same layout logic as old project)
- Only tweet games where model confidence ≥ 55% (exclude true coin flips)

### Morning results tweet (9:30 AM)
```
Yesterday's results: 8/12 (67%) ✅
Best pick: NYY (72%) def. BOS ✅
Biggest miss: LAD (71%) lost to SF ❌

Season record: 8/12 (67%)
```

### Data source changes vs old project
- **Odds removed** — we're not pulling odds (no free reliable source). Use win probability % instead.
- **Confidence tier** replaces odds as the "how sure are we" signal.
- All other tweet logic (multi-tweet splitting, result tracking) stays the same pattern.

---

## Tweet API
Use Tweepy v4 with OAuth 1.0a (same as old project). Credentials in `.env`:
```
TWITTER_API_KEY=...
TWITTER_API_SECRET=...
TWITTER_ACCESS_TOKEN=...
TWITTER_ACCESS_TOKEN_SECRET=...
```

---

## Predictions Storage
Switch from Excel (old project) to SQLite — already our canonical store.

New table: `daily_predictions`
```sql
CREATE TABLE daily_predictions (
    game_id INTEGER,
    game_date TEXT,
    home_team TEXT,
    away_team TEXT,
    home_team_id INTEGER,
    away_team_id INTEGER,
    predicted_winner TEXT,
    predicted_home_win_prob REAL,
    confidence_tier TEXT,  -- 'high'|'medium'|'low'
    run_diff_pred REAL,    -- from ensemble regressor
    ensemble_prob REAL,    -- blended probability
    actual_winner TEXT,
    did_predict_correct INTEGER,  -- 1/0/NULL
    home_score INTEGER,
    away_score INTEGER,
    tweeted INTEGER DEFAULT 0,
    result_tweeted INTEGER DEFAULT 0,
    created_at TEXT,
    updated_at TEXT,
    PRIMARY KEY (game_id)
);
```

---

## Inference Feature Computation

For each game on prediction day, compute features in real time using same logic as historical Layer 2 tables. Key sources:

| Feature group | Source | Notes |
|---|---|---|
| Team season stats | `team_pregame_stats` latest row per team | Recomputed nightly |
| Starter stats | `starter_pregame_stats` latest row per pitcher | Recomputed after each start |
| Career ERA | `player_career_pitching_stats` | Static, fetched once |
| Bullpen | `bullpen_pregame_stats` latest row | Recomputed nightly |
| Lineup | Fetched fresh from statsapi on prediction day | ~9 AM and again ~11 AM |
| Handedness matchup | `team_vs_hand_pregame_stats` + opposing starter hand | Recomputed nightly |
| Lineup top-5 OPS | `player_season_batting_stats` for current season | Updated as season progresses |
| Weather | Open-Meteo API forecast (~3h before first pitch) | Fetched on prediction day |

---

## Cold-Start Handling (April 1-10 approx.)
- Teams with < 15 games: use prior season win% and OPS as fallback
- Pitchers with 0 starts this season: use career ERA
- Flag low-confidence early-season predictions in tweet

---

## Script Structure
```
scripts/
  run_daily.py                    ← entry point, APScheduler setup
  jobs/
    ingest_yesterday.py           ← pull yesterday's games → raw DB
    update_layer2.py              ← recompute derived feature tables
    predict_today.py              ← compute features + score games
    lineup_sweep.py               ← midday lineup re-fetch
    lock_predictions.py           ← pre-game lock
    ingest_results.py             ← pull scores, update outcomes
  inference/
    feature_builder.py            ← compute feature row for one game
    scorer.py                     ← load model, generate predictions
  twitter/
    tweet_predictions.py          ← format + post prediction tweets
    tweet_results.py              ← format + post result tweets
    tweet_formatter.py            ← tweet line/thread formatting logic
```

---

## Build Priority (must be ready by March 26)
1. `inference/feature_builder.py` — live feature computation
2. `inference/scorer.py` — model loader + probability output
3. `jobs/predict_today.py` — end-to-end daily prediction job
4. `jobs/ingest_yesterday.py` — daily raw data ingestion
5. `jobs/update_layer2.py` — nightly derived feature refresh
6. `jobs/ingest_results.py` — results logging
7. `twitter/` — tweet formatting + posting
8. `run_daily.py` — APScheduler wrapper tying it all together
9. `jobs/lineup_sweep.py` + `jobs/lock_predictions.py` — secondary

## What's NOT in scope for launch
- Local web dashboard (post-season MVP)
- Ensemble regressor in production (freeze binary classifier for launch, add regressor later)
- Automated model retraining (manual for now)
