# AGENT.md — predict-mlb Operating Contract
Last updated: 2026-03-30

## 1. Mission
Autonomous daily MLB game prediction and tweet system for the 2026 season. The system ingests results, builds features, runs inference, and schedules tweets — fully unattended.

## 2. Current Phase: Active Season
Season started March 26, 2026. The pre-season refactor is complete; all Layer 2 tables, training, and inference pipeline are deployed and running daily.

## 3. Architecture
```
8:00 AM ET — morning chain:
  ingest_yesterday → update_layer2 → evaluate_yesterday →
  fetch_todays_games → fetch_odds → predict_today → register_tweet_jobs
Variable — per-game tweet jobs fire 1 hour before first pitch
Dashboard — http://localhost:8765
```

## 4. Key Files
| File | Purpose |
|------|---------|
| `run_daily.py` | APScheduler entry point — morning chain + tweet jobs |
| `start.sh` | Startup script (scheduler + FastAPI) |
| `scripts/jobs/` | Pipeline jobs: ingest, update_layer2, evaluate, fetch, predict, schedule_tweets |
| `scripts/inference/` | feature_builder, scorer, explainer |
| `server/api.py` | FastAPI dashboard backend |
| `server/tweet_generator_llm.py` | Tweet generation (deterministic default, LLM toggle) |
| `data/mlb_history.db` | Canonical DB (historical + 2026 accumulating) |
| `artifacts/model_registry/` | Trained model artifacts |
| `configs/training/` | Training configuration files |
| `docs/PIPELINE_SPEC.md` | Pipeline reference |
| `docs/STATUS.md` | Current project status |

## 5. Hard Guardrails
- **Do NOT modify raw tables:** `games`, `game_team_stats`, `game_pitcher_appearances`, `game_lineup_snapshots`, `game_weather_snapshots`, `labels`, `player_handedness_dim`, `venue_dim`
- **Do NOT push to git** without explicit Mako/Steven approval
- **Do NOT modify `data/mlb_history.db` raw data** — only add new derived tables
- Canonical DB: `data/mlb_history.db` — never disposable
- Use repo venv for all Python; do not install system packages
- Commit as `stevensautomations`

## 6. Verification
- Health check: `curl http://localhost:8765/api/status`
- Pipeline log: `SELECT * FROM pipeline_log ORDER BY id DESC LIMIT 20`
- Today's predictions: `SELECT * FROM daily_predictions WHERE game_date = date('now', 'localtime')`
- Season accuracy: `SELECT confidence_tier, COUNT(*), SUM(did_predict_correct) FROM daily_predictions WHERE did_predict_correct IS NOT NULL GROUP BY confidence_tier`

## 7. Handoff Format
```
Status: done | blocked | needs-review
What changed:
Evidence: (row counts, accuracy, logs)
Risks:
Next actions:
```
