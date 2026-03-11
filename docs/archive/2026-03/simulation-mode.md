# Simulation Mode (Deterministic Backtest Path)

Use simulation mode to run prediction generation + scheduling with realistic non-empty game data, without hitting live APIs.

## Env vars

- `PREDICT_SIM_MODE=true` enables simulation mode.
- `PREDICT_SIM_DATE=YYYY-MM-DD` overrides runtime game date.
- `PREDICT_SIM_FIXTURE_PATH=tests/fixtures/sim_games.json` points to fixture input.
- `PREDICT_SIM_SEED=123` (optional) applies deterministic seed-based game ordering for replay variants.
- `PREDICT_DRY_RUN=true` (or `PREDICT_DISABLE_POST=true`) suppresses tweet posting and scheduler start.

## What simulation mode does

When enabled:

1. `server.get_odds.get_todays_odds()` loads fixture games instead of calling Odds API.
2. `predict.generate_daily_predictions()` uses fixture `sim_game_id` + fixture prediction payloads.
3. If `PREDICT_SIM_SEED` is set, fixture game order is deterministically re-ordered by seed.
4. `check_and_predict()` skips unchecked-prediction live result refresh path.
5. Posting is disabled by default in sim mode (`send_tweet` is no-op + marks rows tweeted).

Production behavior is unchanged when `PREDICT_SIM_MODE` is not enabled.

## Fixture schema

`tests/fixtures/sim_games.json` contains:

- `games`: odds/schedule-like inputs (must include `date: "Today"` and `sim_game_id`).
- `predictions`: per-game deterministic prediction payload keyed by `game_id`.

## Quick run (non-empty, no-post)

```bash
PREDICT_SIM_MODE=true \
PREDICT_SIM_DATE=2026-07-04 \
PREDICT_SIM_FIXTURE_PATH=tests/fixtures/sim_games.json \
PREDICT_SIM_SEED=123 \
PREDICT_DRY_RUN=true \
SQLITE_DB_PATH=data/predictions-sim.db \
python3 predict.py
```

This generates fixture-backed predictions, writes them to SQLite, builds tweet lines, and performs scheduling prep without posting.
