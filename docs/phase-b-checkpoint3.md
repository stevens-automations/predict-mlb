# Phase B — Checkpoint 3 (Operator Controls + Rollout Safety)

## What shipped

### 1) Enrichment mode feature flag

New env: `TWEET_ENRICHMENT_MODE=off|shadow|on` (default: `on`)

- `off`
  - Baseline line rendering only (`over` phrasing)
  - No confidence tags (`[H]/[M]/[L]`)
  - No mismatch marker (`| value`)
- `shadow`
  - Baseline rendering for posted output
  - Enrichment output still computed for observability counters/logs
- `on`
  - Full enrichment rendering active

Parsing is defensive: unknown/invalid values safely fall back to `on`.

---

### 2) Regression guardrails before batching/scheduling

Added validation paths that fail-safe by skipping bad lines and continuing run:

- Reject empty/whitespace lines
- Reject lines above sanity cap (`MAX_TWEET_LINE_LENGTH`, pre-batch)
- Validate required fields before line generation (`home`, `away`, `predicted_winner`, `game_id`, `date`)

On skip, structured warning logs emit via `[guardrail-warning]` with reason and context.

---

### 3) Observability threshold warnings

New env thresholds:

- `ENRICHMENT_MISMATCH_RATE_WARN` (default `0.60`)
- `ENRICHMENT_LOW_CONFIDENCE_RATE_WARN` (default `0.70`)

Structured warnings are emitted when exceeded in summaries, including run summary stage.

---

### 4) Daily operator artifact (JSONL append)

Per-run report writer added (local file, append mode).

- Default path pattern:
  - `docs/reports/enrichment-YYYY-MM-DD.jsonl`
- Override path:
  - `ENRICHMENT_REPORT_PATH=/custom/path/report.jsonl`
- Disable writer:
  - set `ENRICHMENT_REPORT_PATH=off` (or `false`/`none`/`0`)

Each entry includes run_id, timestamp, mode, summary metrics, and threshold warnings.

## Recommended initial rollout sequence

1. **Day 1–2: `off`**
   - Validate no reliability regressions in core posting/scheduling.
2. **Day 3–5: `shadow`**
   - Confirm enrichment counters look sane without changing posted output.
   - Tune thresholds if warning noise is excessive.
3. **Day 6+: `on`**
   - Enable full enrichment in production rendering once shadow metrics are stable.

## Operator checklist

- Confirm env values in `.env`
- Run tests before deploy
- Start in `off` or `shadow`, not `on`
- Watch `[guardrail-warning]` and `[predict-summary]` logs
- Review daily JSONL artifact for drift/trend

## Reliability notes

- Bad records/lines are skipped, not fatal to run.
- Scheduler idempotency and dedupe behavior remain intact.
- All new env parsing includes safe defaults to avoid startup/runtime breakage.
