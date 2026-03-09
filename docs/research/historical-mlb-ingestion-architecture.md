# Historical MLB Ingestion & Storage Architecture (statsapi + local DB)

Date: 2026-03-09  
Scope: Replace Excel-centric historical data retrieval with idempotent, resumable local-database ingestion while preserving the existing prediction pipeline.

---

## 1) Current-state assessment (what exists + risks)

### What exists now
- `data_retriever.py`
  - Splits date ranges into half-month intervals.
  - Calls `LeagueStats.get_data(...)` / `TeamStats.get_data(...)`.
  - Writes each interval to Excel (`data/seasons/<year>/<month>_<index>.xlsx`).
  - If file exists, interval is skipped.
  - Retries forever on any exception (`while not success`) with no backoff.
- `data.py::get_data`
  - For each year in range, loads full-season schedule via `statsapi.schedule`.
  - Filters for final/eligible game types and date range.
  - For each game, builds one-row feature set using many additional API calls (`schedule`, standings, boxscore, team leaders, pitcher stats).
  - Concats per-game DataFrames in-memory and exports `.xlsx`.

### Primary failure/scale risks
1. **Excel as system of record**
   - Hard to enforce uniqueness/idempotency at row level.
   - Expensive reprocessing when one interval fails or changes.
   - Limited auditability/versioning for partial refreshes.
2. **Non-idempotent ingestion behavior**
   - Skip-if-file-exists prevents targeted correction of bad/partial files.
   - No row-level upsert key (e.g., `game_id + snapshot_ts + feature_version`).
3. **Unbounded retry loop**
   - Infinite retry on persistent errors can hang forever.
   - No backoff/jitter/rate-limit adaptation.
4. **High API amplification per game**
   - `make_game_df` invokes many endpoint calls per game; backfill over seasons is slow and failure-prone.
5. **No checkpoint ledger**
   - Progress state is implied by file existence only.
   - No resume from exact game/date cursor with attempt metadata.
6. **Data quality checks are implicit/manual**
   - No formal completeness/null/duplicate/freshness contract tied to ingestion.

---

## 2) Recommended target architecture

## 2.1 Storage choice/location
- **Primary DB:** SQLite (WAL mode) in repo-local data path.
  - Path: `data/mlb_history.db`
  - Rationale: already used in project, zero infra overhead, good enough for multi-season MLB volume.
- **Optional analytics mirror (later):** DuckDB file for heavy ad-hoc analysis/model experiments.
  - Path: `data/mlb_history.duckdb`
  - Populated from SQLite snapshots (not source-of-truth).

## 2.2 Data model (normalized + feature snapshot)

### Core entities
1. `games`
   - `game_id` (PK), season, game_date, game_type, status, home_team_id, away_team_id, venue, scheduled_datetime, final_scores, winning_team_id, source_updated_at
2. `team_game_stats`
   - PK: (`game_id`, `team_id`)
   - batting/pitching/fielding aggregates from boxscore (runs, hits, ops, strikeouts, obp, etc.)
3. `pitcher_game_context`
   - PK: (`game_id`, `side`) where side in {home, away}
   - probable starter ids/names and resolved season/career stats used by current features
4. `odds_snapshots`
   - PK: (`game_id`, `snapshot_ts`, `bookmaker`, `market_type`)
   - home/away odds + normalized implied probs + retrieval metadata
5. `feature_snapshots`
   - PK: (`game_id`, `feature_version`, `as_of_ts`)
   - materialized model feature vector compatible with current `order1/order2` names
   - includes `label_did_home_win` when game final
6. `ingestion_runs`
   - run metadata: run_id, mode(backfill/incremental), started_at, ended_at, status, config hash
7. `ingestion_checkpoints`
   - PK: (`job_name`, `partition_key`) e.g., `season=2022`
   - cursor fields: last_date, last_game_id, last_success_at, attempts, last_error
8. `dq_results`
   - per-run/per-table validation outputs and thresholds breached

## 2.3 Idempotent ingestion model
- Use deterministic natural keys + `INSERT ... ON CONFLICT DO UPDATE`.
- Separate layers:
  1) **Raw-ish canonical game facts** (`games`, `team_game_stats`, `pitcher_game_context`, `odds_snapshots`)
  2) **Derived features** (`feature_snapshots`) with explicit `feature_version`.
- Re-runs are safe:
  - same key updates existing row
  - no duplicate games/feature rows
  - schema supports targeted replay by season/date/game.

---

## 3) Backfill strategy (seasons + checkpoint/resume + retries)

## 3.1 Partitioning
- Backfill by **season** first, then by **month** for operational control.
- Partition key examples:
  - `season=2020`, `season=2021`, ...
  - optional subpartition `season=2024, month=06`

## 3.2 Execution order
1. Ingest `games` schedule skeleton for partition.
2. For final games in partition, ingest boxscore/team/pitcher context.
3. Build `feature_snapshots` (current feature version).
4. Run DQ checks.
5. Mark checkpoint complete.

## 3.3 Resume/checkpoint behavior
- After every N games (e.g., 25), persist checkpoint:
  - last processed `game_date`, `game_id`, row counts, attempt count.
- On restart:
  - read checkpoint and continue from next unprocessed game.
- Failed partition remains resumable without deleting prior good data.

## 3.4 Rate-limit/retry policy
- Max attempts per request: 5
- Backoff: exponential with jitter (e.g., 1s, 2s, 4s, 8s, 16s ± random)
- Timeout per request: 20–30s
- Circuit breaker (partition-local):
  - if error rate > X% over rolling window, pause 2–5 min and continue
- Hard stop conditions:
  - >Y consecutive hard failures on same endpoint -> mark partition `blocked` and continue other partitions

---

## 4) Data quality/validation framework

Run at end of each partition and daily incremental run.

## 4.1 Completeness
- Expected game count by date/season from `statsapi.schedule` baseline.
- Check: `count(games where status='Final')` vs expected.
- Check feature coverage: `% final games with feature_snapshots` >= threshold (e.g., 99.5%).

## 4.2 Null thresholds
- Must-have feature columns: null rate must be 0% (or strict near-zero if explicitly approved).
- Optional features: bounded null rate (e.g., <= 5%) with warnings.

## 4.3 Duplicate checks
- Assert PK uniqueness for each table.
- Explicit check query for accidental duplicates by `game_id` and (`game_id`,`feature_version`,`as_of_ts`).

## 4.4 Freshness checks
- Incremental run SLA:
  - today’s scheduled games present before prediction window.
  - final results/labels backfilled within X hours after game completion.

## 4.5 DQ artifacting
- Persist check results to `dq_results` + write JSON summary under `docs/archive/runtime-logs/`.
- Fail run (or downgrade) based on contract severity (must-have vs optional).

---

## 5) Incremental daily update strategy

Daily jobs (can be single orchestrator script with subcommands):
1. **Pre-game refresh (morning + pre-lock):**
   - update today’s schedule, probable pitchers, latest odds snapshots
   - rebuild today `feature_snapshots` as-of latest timestamp
2. **Intra-day refresh (optional hourly):**
   - odds/probables refresh for not-started games
3. **Post-game finalization (night):**
   - finalize game outcomes/team stats
   - populate labels for completed games
   - run DQ + checkpoint

Suggested command surface:
- `python scripts/history_ingest.py backfill --season 2021`
- `python scripts/history_ingest.py incremental --date 2026-03-09`
- `python scripts/history_ingest.py dq --partition season=2024`

---

## 6) Cost / performance / reliability tradeoffs

- **SQLite only (recommended now)**
  - Pros: simplest ops, no external service, easy backups.
  - Cons: less ideal for concurrent heavy analytical workloads.
- **SQLite + DuckDB mirror (later)**
  - Pros: faster offline analytics/retraining queries.
  - Cons: extra sync complexity.
- **Feature materialization upfront vs on-demand**
  - Upfront snapshots: faster training/reproducibility; more storage.
  - On-demand recompute: less storage; more runtime/API compute and drift risk.
- **Strict fail-closed for must-have features**
  - Pros: protects prediction integrity.
  - Cons: can reduce daily throughput on noisy API days.

---

## 7) Concrete phased implementation plan (safe migration)

## Phase A — Foundation (no behavior change)
- Build new DB + schema + ingestion metadata tables.
- Add ingestion script capable of writing `games` + `feature_snapshots` for a small sample window.
- Keep Excel pipeline untouched.

**Acceptance criteria**
- Can ingest one month into SQLite with resumable checkpoints.
- Re-running same month produces zero duplicates and stable row counts.

## Phase B — Historical backfill + DQ hardening
- Backfill target seasons in order.
- Implement DQ checks and run summaries.
- Validate internal consistency/reproducibility of newly ingested features (not legacy Excel parity as a hard objective).

**Acceptance criteria**
- Backfill completion per season with checkpoint logs.
- DQ pass rate meets thresholds; exceptions documented.
- Re-ingestion reproducibility checks pass within defined numeric tolerance.

## Phase C — Shadow read path
- Add read adapter so training/retrain can source from SQLite feature snapshots.
- Continue producing Excel outputs in parallel for validation period.

**Acceptance criteria**
- Training pipeline runs successfully from SQLite source.
- Metrics/report outputs match or improve vs Excel baseline.

## Phase D — Cutover
- Switch historical training source-of-truth to SQLite.
- Keep Excel export as optional compatibility artifact only.

**Acceptance criteria**
- No regression in scheduled prediction pipeline.
- Rollback plan tested (toggle back to legacy source if needed).

## Phase E — Optional enhancements
- Add DuckDB analytics mirror.
- Add richer features/market snapshots with feature version bumps.

---

## 8) Approvals / inputs needed from Steven (updated)

### Already decided
1. **Source-of-truth decision:** `data/mlb_history.db` (SQLite) as canonical historical store.
2. **Historical scope:** backfill 2020–2025 first, then extend if needed.
3. **Data contract policy:** strict contracts with degraded fallback predictions (no silent game skipping).
4. **Initial model metric:** log loss primary (weighted score can be added later).

### Still needed
5. **Operational cadence:** approve daily incremental schedule windows (pre-game, intra-day optional, post-game finalization).
6. **Storage policy:** approve retention policy for odds snapshots and feature versions (how long to keep high-frequency snapshots).
7. **Cutover gate:** approve objective go/no-go criteria for switching training source from legacy dataset assumptions to SQLite historical store.
8. **Optional analytics mirror:** decide whether to include DuckDB mirror in initial rollout or defer.

---

## Minimal example SQL checks

```sql
-- duplicate guard
SELECT game_id, COUNT(*)
FROM games
GROUP BY game_id
HAVING COUNT(*) > 1;

-- feature completeness for final games
SELECT
  COUNT(*) AS final_games,
  SUM(CASE WHEN fs.game_id IS NOT NULL THEN 1 ELSE 0 END) AS with_features
FROM games g
LEFT JOIN feature_snapshots fs
  ON fs.game_id = g.game_id
WHERE g.status = 'Final';
```

## Minimal runtime setup commands

```bash
# initialize schema
python scripts/history_ingest.py init-db --db data/mlb_history.db

# backfill one season with checkpoints
python scripts/history_ingest.py backfill --season 2023 --checkpoint-every 25

# daily incremental
python scripts/history_ingest.py incremental --date "$(date +%F)"
```
