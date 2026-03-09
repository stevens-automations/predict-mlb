# Simulation Mode Progress

## Implemented

- Added env-driven simulation config (`simulation.py`):
  - `PREDICT_SIM_MODE`
  - `PREDICT_SIM_DATE`
  - `PREDICT_SIM_FIXTURE_PATH`
  - `PREDICT_DRY_RUN` / `PREDICT_DISABLE_POST`
- Added deterministic fixture loader for simulated odds + predictions.
- Updated odds retrieval to short-circuit to fixtures in sim mode (no live Odds API call).
- Updated prediction generation to bypass live schedule/model prediction path in sim mode and use fixture-backed game/prediction records.
- Updated runtime validation to skip secret env requirements in sim mode.
- Updated pipeline run flow to skip unchecked live result refresh in sim mode.
- Added dry-run posting guard in `send_tweet` and scheduler-start skip for dry run.
- Added fixture: `tests/fixtures/sim_games.json`.
- Added tests in `tests/test_simulation_mode.py`:
  - odds retrieval short-circuit (no live request)
  - deterministic generation + scheduling with non-empty fixture lines
- Added docs: `docs/simulation-mode.md`.

## Notes

- Default/production behavior remains unchanged unless `PREDICT_SIM_MODE=true`.
- Simulation mode supports deterministic readiness runs even when there are no live MLB games.
