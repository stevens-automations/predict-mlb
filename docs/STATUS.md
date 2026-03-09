# Project Status (Current)

Last updated: 2026-03-09
Branch intent: `staging/preseason-consolidated` is the integration branch; `main` remains unchanged until explicit promotion.

## Implemented

### Reliability / Ops
- Retry + backoff + timeout + circuit-breaker behavior for posting and odds retrieval.
- Stale odds cache fallback for transient API failures.
- Runtime guardrail warnings and structured logging coverage.

### Storage
- SQLite-first runtime storage path for prediction outputs.
- Transactional replace/upsert safety and rollback behavior covered by tests.
- SQLite healthcheck utility and operating runbook available.

### Historical Ingestion Foundation (new)
- Added historical ingestion CLI: `scripts/history_ingest.py` with subcommands:
  - `init-db`
  - `backfill` (bounded statsapi schedule ingest)
  - `incremental` (bounded one-day schedule ingest)
- `backfill-team-stats --season <2020-2025>` (season-scoped completed-game team boxscore backfill)
- `backfill-pitcher-context --season <2020-2025>` with legacy alias `backfill-pitcher-context-2020` (season-scoped parity-safe starter context backfill from prior completed games only)
- `materialize-feature-rows --season <2020-2025>` (season-scoped canonical `feature_rows(feature_version='v1')` materialization)
  - `dq`
- Added canonical historical schema SQL at `scripts/sql/history_schema.sql`.
- Added run/checkpoint ledger in DB (`ingestion_runs`, `ingestion_checkpoints`) with periodic/final checkpoint updates.
- Added idempotent upsert helpers for `games` + `labels` (`did_home_win`, `run_differential`, `total_runs` for final games).
- Added mocked tests for bounded backfill/incremental ingest behavior and idempotent upserts.
- `game_pitcher_context` no longer depends on leakage-prone `player_stat_data(type=yearByYear)` season aggregates for 2020 backfill. Starter season metrics are derived as-of each game from previously completed boxscores when available; otherwise the command preserves starter identity and rewrites season fields to explicit leakage-safe null fallback with `season_stats_scope='season_to_date_prior_completed_games'` and `season_stats_leakage_risk=0`.
- 2021 validator sanity-range blocker was reduced to two legitimate tiny-sample `game_pitcher_context` rows (4 field hits total), and validator pitcher ceilings now allow those edge cases while still flagging clearly broken decimal-innings-style outliers.
- `feature_rows(feature_version='v1')` can now be materialized for 2020 from existing support tables with one canonical row per `(game_id, feature_version)`, stable `as_of_ts`, stale-snapshot cleanup, and explicit degraded/null behavior.

### Training Readiness / Optimization (new)
- Canonical training configs now live under `configs/training/` for baseline, tuned candidate, experiment suite, and promotion gates.
- Readiness polling script added at `scripts/training/run_when_ready.py` to block training until required seasons are trainable.
- Walk-forward metrics now include calibration diagnostics (`expected_calibration_error`, `max_calibration_gap`) in addition to log loss, Brier, and accuracy.
- Canonical optimization roadmap and promotion template now live in `docs/runbooks/model-optimization-plan.md`.

## Newly Aligned Direction (encoded)

- Canonical historical store is SQLite at `data/mlb_history.db`.
- Backfill scope target is seasons `2020–2025`.
- Historical odds backfill is out-of-scope (odds are forward-only capture during season).
- Data policy is strict contracts with degraded fallback predictions (no silent game skipping).
- Ingestion reliability posture: bounded retries/backoff, request budget, checkpoint resume.
- Incremental cadence starts with **pre-game + post-game** only.
- Primary model metric starts with **log loss**.

## Known Constraints / Open Gaps

1. 2022-2025 ingestion/materialization is still finishing in the background, so the baseline training run has not been executed yet.
2. `feature_rows(v1)` remains the only approved immediate training contract; uplift tracks are intentionally gated for later.
3. Contract evaluators and DQ checks remain minimal placeholders beyond current validation coverage.
4. Odds snapshot table exists but should remain forward-only until explicit policy change.

## Non-Goals (current phase)

- No historical odds backfill.
- No rushed promotion to `main` before staging validation and acceptance gates are complete.
