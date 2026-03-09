# Day-0 Launch Reliability Checklist (Preseason Dry Run)

## Required safe-mode flags
- `PREDICT_DRY_RUN=true` for rehearsal runs that should not post.
- `PREDICT_SIM_MODE=true` with `PREDICT_SIM_FIXTURE_PATH=...` for fully offline simulation.
- Keep one of `SQLITE_DB_PATH` or `PREDICTIONS_DB_PATH` set and writable.

## New resilience controls
### Tweet posting path (`predict.py`)
- `TWEET_RETRY_ATTEMPTS` (default `3`): retries for transient post failures.
- `TWEET_RETRY_BACKOFF_SEC` (default `2`): exponential base backoff.
- `TWEET_RATE_LIMIT_BACKOFF_SEC` (default `60`): cooldown when rate-limited.
- `TWEET_SUBPROCESS_TIMEOUT_SEC` (default `45`): avoid hung tweet subprocesses.
- `TWEET_CIRCUIT_FAILURE_THRESHOLD` (default `3`): open circuit after repeated failures.
- `TWEET_CIRCUIT_COOLDOWN_SEC` (default `300`): circuit open window.

### Odds retrieval path (`server/get_odds.py`)
- `ODDS_REQUEST_TIMEOUT_SEC` (default `10`): request timeout.
- `ODDS_REQUEST_RETRIES` (default `3`): retry count for 429/5xx/network errors.
- `ODDS_REQUEST_BACKOFF_SEC` (default `1.5`): exponential base backoff.
- `ODDS_CIRCUIT_FAILURE_THRESHOLD` (default `3`): open circuit after repeated API failures.
- `ODDS_CIRCUIT_COOLDOWN_SEC` (default `600`): circuit open window.
- On API outage, stale cached odds (`data/todays_odds.json`) are used as fallback.

## Pre-run checks
1. `python3 -m unittest tests/test_guardrails_and_scheduling.py tests/test_get_odds_resilience.py`
2. Verify `data/todays_odds.json` exists before expected API volatility windows.
3. Dry run first (`PREDICT_DRY_RUN=true`) and confirm:
   - no external post side effects,
   - scheduler dedupe still holds,
   - summary logs show zero sqlite write failures.
4. If switching to live posts, unset dry-run flags and monitor for:
   - `[tweet-circuit]` logs,
   - `[odds-circuit]` logs,
   - repeated rate-limit messages.

## Rollback/safety fallback
- Immediate safe fallback: set `PREDICT_DRY_RUN=true` and rerun.
- If odds provider unstable: keep dry run on and rely on simulation fixtures until provider recovers.
- If posting provider unstable: circuit breaker now prevents repeated hammering; keep dry run enabled for continued pipeline validation.
