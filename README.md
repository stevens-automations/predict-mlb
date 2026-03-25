# predict-mlb

> ML-powered MLB game prediction bot. Predicts daily game outcomes using LightGBM trained on historical MLB data (2020–2025). Tweets 2-3 highest-confidence predictions per day with win probabilities and market odds.

---

## 1. Overview

`predict-mlb` runs a daily pipeline that:
- Ingests yesterday's MLB game results into a local SQLite database
- Fetches today's game schedule and live betting odds
- Generates pre-game win probability predictions using a trained LightGBM model
- Posts 2-3 predictions per day (highest confidence games) to Twitter with win probability % and market odds in American ML format
- Weekly recap tweet every Monday with season W/L record

**Current model:** LightGBM v4 tuned (`matchup_lgbm_v4_tuned_final`)
**Accuracy:** 57.37% on 2025 holdout season
**Training data:** MLB 2020–2025 (6 seasons)

---

## 2. Architecture

### Data layers

```
Raw historical data (statsapi)
    └─► Layer 1: games, game_team_stats, game_pitcher_appearances,
                 game_lineup_snapshots, game_weather_snapshots
    └─► Layer 2 (engineered): team_pregame_stats, starter_pregame_stats,
                               bullpen_pregame_stats, lineup_pregame_context,
                               team_vs_hand_pregame_stats, player_career_pitching_stats
    └─► game_matchup_features (flat training/inference row)
    └─► LightGBM model → win probability → tweet
```

### Directory structure

```
predict-mlb/
├── run_daily.py              # APScheduler entry point
├── start.sh                  # startup script
├── scripts/
│   ├── inference/            # feature_builder, scorer, explainer
│   ├── jobs/                 # ingest_yesterday, update_layer2, predict_today, etc.
│   ├── training/             # train_matchup_lgbm.py, tune_lgbm.py, etc.
│   ├── build_layer2_*.py     # Layer 2 feature table builders
│   └── history_ingest.py     # historical data ingestion pipeline
├── server/
│   ├── api.py                # FastAPI dashboard backend
│   ├── static/index.html     # web dashboard
│   ├── tweet_generator_llm.py # Qwen tweet generation
│   └── tweet_scaffold.py     # deterministic fallback format
├── artifacts/model_registry/ # trained model artifacts
├── data/                     # SQLite database (not in repo)
├── docs/                     # project documentation
└── configs/training/         # training configuration files
```

The database (`data/mlb_history.db`) stores all layers. The model is pre-trained and stored in `artifacts/model_registry/`. No retraining is required to run daily predictions.

---

## 3. Setup

### Prerequisites
- Python 3.12+
- macOS or Linux (arm64 or x86_64)

### Clone and install

```bash
git clone https://github.com/stevens-automations/predict-mlb.git
cd predict-mlb
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Configure environment

```bash
cp .env.example .env
# Edit .env with your API keys
```

Required environment variables:

| Variable | Description |
|----------|-------------|
| `ODDS_API_KEY` | The Odds API key (for live betting lines) |
| `TWITTER_API_KEY` | Twitter/X API v2 key |
| `TWITTER_API_SECRET` | Twitter/X API v2 secret |
| `TWITTER_ACCESS_TOKEN` | Twitter/X OAuth access token |
| `TWITTER_ACCESS_TOKEN_SECRET` | Twitter/X OAuth access token secret |

### Database note

The historical database (2020–2025) is **not included** in this repo due to size. The pipeline will automatically build the 2026 season database from day 1 of the season — no manual setup required.

### Model artifact

The trained model artifact **is** included in the repo at:
```
artifacts/model_registry/matchup_lgbm_v4_tuned_final__20260319T122216Z/model.pkl
```
No retraining is needed to run daily predictions.

---

## 4. Running

```bash
./start.sh
```

This starts the APScheduler-based daily runner and the FastAPI dashboard. Dashboard is available at **http://localhost:8765**.

Daily behavior:
- **8:00 AM ET** — morning chain fires: ingest yesterday's results → update Layer 2 features → evaluate prior predictions → fetch today's schedule + odds → generate predictions → schedule tweet jobs
- **Variable** — tweet jobs fire 1 hour before each predicted game's first pitch
- **Every Monday** — weekly recap tweet with season W/L record generated alongside the morning chain

---

## 5. Daily Schedule

| Time (ET) | Job |
|-----------|-----|
| 8:00 AM | Morning chain: ingest → layer2 update → evaluate → fetch games → fetch odds → predict → schedule tweets |
| Variable | Per-game tweet jobs fire 1 hour before each game's first pitch |
| Monday 8 AM | Weekly recap tweet (season W/L record) generated as part of morning chain |

---

## 6. Dashboard

Visit **http://localhost:8765** for:
- Today's predictions with win probabilities, market odds, and confidence tiers
- Historical accuracy stats
- Recent log output
- Model metadata

---

## 7. Key Docs

| Doc | Description |
|-----|-------------|
| [docs/REFACTOR_SPEC.md](docs/REFACTOR_SPEC.md) | Canonical architecture reference — data layers, feature contracts, training spec |
| [docs/PIPELINE_SPEC.md](docs/PIPELINE_SPEC.md) | Daily pipeline spec — job sequence, error handling, retry logic |
| [docs/OPS_PLAN.md](docs/OPS_PLAN.md) | Operations plan — scheduling, monitoring, deployment |
| [docs/STATUS.md](docs/STATUS.md) | Current project status and model performance |
| [docs/TODO.md](docs/TODO.md) | Active backlog and future roadmap items |

---

## License

Private repository. All rights reserved.
