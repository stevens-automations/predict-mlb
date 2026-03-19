# predict-mlb

> ML-powered MLB game prediction bot. Predicts daily game outcomes using LightGBM trained on historical MLB data (2020–2025). Tweets predictions with win probabilities before each game.

---

## 1. Overview

`predict-mlb` runs a daily pipeline that:
- Ingests yesterday's MLB game results into a local SQLite database
- Fetches today's game schedule and live betting odds
- Generates pre-game win probability predictions using a trained LightGBM model
- Posts predictions to Twitter with confidence tiers and probabilities
- Evaluates prior-day predictions against final scores each evening

**Current model:** LightGBM v4 tuned (`matchup_lgbm_v4_tuned_final`)
**Accuracy:** ~57% on 2025 holdout season

---

## 2. Architecture

```
Raw historical data (statsapi)
    └─► Layer 1: games, game_team_stats, game_pitcher_appearances,
                 game_lineup_snapshots, game_weather_snapshots
    └─► Layer 2 (engineered): team_pregame_stats, starter_pregame_stats,
                               bullpen_pregame_stats, lineup_pregame_context,
                               team_vs_hand_pregame_stats
    └─► game_matchup_features (flat training/inference row)
    └─► LightGBM model → win probability → tweet
```

The database (`data/mlb_history.db`) stores all layers. The model is pre-trained and stored in `artifacts/model_registry/`. No retraining is required to run daily predictions.

---

## 3. Setup

### Prerequisites
- Python 3.12
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

The historical database (2020–2025) is **not included** due to size. The pipeline will build the 2026 season database automatically from day 1 of the season. To retrain on historical data, see [docs/REFACTOR_SPEC.md](docs/REFACTOR_SPEC.md).

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

---

## 5. Daily Schedule

| Time (ET) | Job |
|-----------|-----|
| 8:00 AM | Ingest yesterday's game results |
| 8:15 AM | Fetch today's schedule and odds |
| 8:30 AM | Generate predictions and post to Twitter |
| 11:00 PM | Evaluate today's predictions against final scores |

---

## 6. Dashboard

Visit **http://localhost:8765** for:
- Today's predictions with win probabilities and confidence tiers
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
| [docs/TODO.md](docs/TODO.md) | Active backlog and future roadmap items |

---

## License

Private repository. All rights reserved.
