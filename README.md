# predict-mlb

ML-powered MLB game prediction system. Predicts daily outcomes using LightGBM trained on 2020–2025 historical data, tweets highest-confidence picks with win probabilities and market odds.

**Model:** LightGBM v4 tuned — 57.37% on 2025 holdout
**Season:** 2026, live since March 26

## Quick Start

```bash
git clone https://github.com/stevens-automations/predict-mlb.git
cd predict-mlb
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # edit with your API keys
./start.sh
```

Dashboard: **http://localhost:8765**

## Daily Schedule

| Time (ET) | Job |
|-----------|-----|
| 8:00 AM | Morning chain: ingest → layer2 → evaluate → fetch games → fetch odds → predict → schedule tweets |
| Variable | Per-game tweet jobs fire 1 hour before first pitch |

## Configuration

Required `.env` variables:

| Variable | Description |
|----------|-------------|
| `ODDS_API_KEY` | The Odds API key (live betting lines) |
| `TWITTER_API_KEY` | Twitter/X API v2 key |
| `TWITTER_API_SECRET` | Twitter/X API v2 secret |
| `TWITTER_ACCESS_TOKEN` | Twitter/X OAuth access token |
| `TWITTER_ACCESS_TOKEN_SECRET` | Twitter/X OAuth access token secret |

## Project Structure

```
predict-mlb/
├── run_daily.py              # APScheduler entry point
├── start.sh                  # Startup script (scheduler + dashboard)
├── scripts/
│   ├── jobs/                 # Pipeline jobs (ingest, predict, evaluate, etc.)
│   ├── inference/            # feature_builder, scorer, explainer
│   └── training/             # Training scripts and utilities
├── server/
│   ├── api.py                # FastAPI dashboard backend
│   ├── static/               # Web dashboard
│   └── tweet_generator_llm.py # Tweet generation (deterministic + LLM toggle)
├── artifacts/model_registry/ # Trained model artifacts
├── configs/training/         # Training configuration files
├── data/                     # SQLite database (not in repo)
└── docs/                     # Documentation
```

## Key Docs

| Doc | Description |
|-----|-------------|
| [AGENT.md](AGENT.md) | Operating contract and guardrails |
| [docs/PIPELINE_SPEC.md](docs/PIPELINE_SPEC.md) | Pipeline architecture and job spec |
| [docs/STATUS.md](docs/STATUS.md) | Current project status |
| [docs/TWEET_STRATEGY.md](docs/TWEET_STRATEGY.md) | Tweet content strategy |
| [docs/TRAINING_SPEC.md](docs/TRAINING_SPEC.md) | Model training reference |

## License

Private repository. All rights reserved.
