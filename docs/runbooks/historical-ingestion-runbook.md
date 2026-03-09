# Historical Ingestion Runbook (Scaffold Phase)

Last updated: 2026-03-09

## Purpose

This runbook covers the **approved-safe scaffold** for historical MLB ingestion.
Current scope is schema + control-plane metadata only. No long-running historical pull is executed by default.

## Canonical Decisions Encoded

- Canonical historical store: `data/mlb_history.db` (SQLite)
- Initial backfill target scope: seasons `2020-2025`
- Historical odds backfill: **disabled** (odds are forward-only during active season)
- Data contracts: strict with degraded fallback path (no silent game skipping)
- Incremental cadence v1: pre-game + post-game only
- Primary model metric v1: log loss
- Reliability defaults: bounded retry/backoff/jitter/timeouts/request budget + checkpoint resume

## Commands (safe in scaffold phase)

From repo root:

```bash
python scripts/history_ingest.py init-db
python scripts/history_ingest.py backfill --season 2024
python scripts/history_ingest.py incremental --date 2026-03-09
python scripts/history_ingest.py dq --partition season=2024
```

### Expected behavior now

- `init-db` creates/updates schema and run ledger row in `ingestion_runs`.
- `backfill` writes a **stubbed** run + checkpoint marker only (no statsapi historical pull).
- `incremental` writes a **stubbed** run + checkpoint marker only (no live fetch).
- `dq` writes a placeholder DQ result row to prove run plumbing.

## Verification Checklist

1. Schema tables exist:
   - `games`
   - `game_team_stats`
   - `game_pitcher_context`
   - `feature_rows`
   - `labels`
   - `ingestion_runs`
   - `ingestion_checkpoints`
   - `dq_results`
   - `odds_snapshot` (forward-only note)
2. `ingestion_runs` contains rows for each command invocation.
3. `ingestion_checkpoints` has entries for backfill/incremental partitions.
4. Re-running backfill/incremental updates checkpoint attempt count (idempotent key behavior).
5. Tests pass:

```bash
pytest -q tests/test_history_ingest.py
```

## Explicitly Not Run in This Phase

- No historical season-scale ingestion for 2020-2025
- No full statsapi crawl/backfill loops
- No historical odds ingestion

## Approval Gate Before Enabling Real Pulls

Before implementing non-stub backfill/incremental execution, require explicit approval for:
1. request budget ceilings by run type,
2. allowed runtime windows,
3. stop/abort thresholds for degraded API conditions,
4. DQ fail-open vs fail-closed behavior for each must-have field class.
