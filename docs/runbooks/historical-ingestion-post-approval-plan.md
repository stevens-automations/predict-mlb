# Historical Ingestion Post-Approval Run/Verification Plan

Last updated: 2026-03-09

## Purpose

This runbook defines the exact execution plan to move from scaffold mode to approved bounded ingestion for MLB history.

## Guardrails (must remain true)

- Canonical store: `data/mlb_history.db`
- Historical backfill scope target: `2020-2025`
- No historical odds backfill; `odds_snapshot` remains forward-only
- Strict contracts + degraded fallback only (no silent game skipping)
- Minimize statsapi requests via bounded retries/backoff/jitter/timeouts + per-run request budget + checkpoint resume
- Incremental cadence v1: pre-game + post-game only
- Primary model metric v1: log loss

## Step 0: Preflight

From repo root:

```bash
python scripts/history_ingest.py init-db
pytest -q tests/test_history_ingest.py
```

Verify:
- CLI exits 0.
- DB file exists at `data/mlb_history.db`.
- Required tables exist.

## Step 1: Controlled activation slice (single partition)

Run one bounded partition only (example: 2024 season):

```bash
python scripts/history_ingest.py backfill \
  --season 2024 \
  --checkpoint-every 25 \
  --request-budget-per-run 800 \
  --max-attempts 5 \
  --timeout-seconds 25 \
  --initial-backoff-seconds 1 \
  --max-backoff-seconds 16 \
  --jitter-seconds 0.4
```

Verify in SQLite:

```bash
sqlite3 data/mlb_history.db "SELECT mode,status,partition_key,request_count,note FROM ingestion_runs ORDER BY started_at DESC LIMIT 3;"
sqlite3 data/mlb_history.db "SELECT job_name,partition_key,status,attempts,last_game_id FROM ingestion_checkpoints WHERE job_name='backfill' ORDER BY updated_at DESC LIMIT 5;"
```

Pass criteria:
- Run row created with non-failed terminal state.
- Checkpoint row created/updated for `season=2024`.
- No duplicate `games.game_id` keys.

## Step 2: Idempotency and resume proof

Re-run same partition command once.

Then verify:

```bash
sqlite3 data/mlb_history.db "SELECT COUNT(*) FROM games;"
sqlite3 data/mlb_history.db "SELECT game_id,COUNT(*) c FROM games GROUP BY game_id HAVING c>1 LIMIT 5;"
sqlite3 data/mlb_history.db "SELECT attempts,last_game_id FROM ingestion_checkpoints WHERE job_name='backfill' AND partition_key='season=2024';"
```

Pass criteria:
- No duplicate keys returned.
- Checkpoint `attempts` increments.
- Counts remain stable or change only for intended late corrections.

## Step 3: Full historical backfill (2020-2025)

Run season by season, one command at a time:

```bash
python scripts/history_ingest.py backfill --season 2020
python scripts/history_ingest.py backfill --season 2021
python scripts/history_ingest.py backfill --season 2022
python scripts/history_ingest.py backfill --season 2023
python scripts/history_ingest.py backfill --season 2024
python scripts/history_ingest.py backfill --season 2025
```

After each season:

```bash
python scripts/history_ingest.py dq --partition season=<YEAR>
```

Pass criteria per season:
- `ingestion_runs` has terminal run for that season.
- `dq_results` exists for the season run.
- Any degraded-contract incidents are logged and actionable.

## Step 4: Incremental cadence activation (pre-game + post-game)

Daily execution plan:

```bash
python scripts/history_ingest.py incremental --date <YYYY-MM-DD>
python scripts/history_ingest.py dq --partition date=<YYYY-MM-DD>
```

Pass criteria:
- Incremental checkpoint and run rows are written.
- No silent skips; degraded cases are explicit.

## Step 5: Model readiness check

After sufficient labels/features are present:
- Build training extract from `feature_rows` + `labels`.
- Evaluate baseline and challengers with **log loss** as primary metric.

## Explicit non-goals during post-approval rollout

- Do not add hourly intra-day cadence until pre/post-game stability is proven.
- Do not add historical odds backfill.
- Do not change canonical historical store from SQLite without separate approval.
