# Phase B — Checkpoint 2 (Operator Configurability + Observability)

## Scope Delivered

Checkpoint 2 extends the enrichment layer from checkpoint 1 with runtime knobs and per-run observability summaries, while keeping posting/scheduling flow unchanged.

## New Environment Knobs

### `TWEET_PHRASE_BANK`
- **Type:** comma-separated string
- **Default:** `over,to beat,vs`
- **Behavior:**
  - Parsed into trimmed phrases
  - Empty tokens are ignored
  - If env is empty/invalid (e.g. only commas/spaces), fallback to default bank

Example:
```bash
TWEET_PHRASE_BANK="edge past,outlast,handle"
```

### `TWEET_MISMATCH_LABEL`
- **Type:** string
- **Default:** `value`
- **Behavior:**
  - Used in mismatch marker suffix (`| <label>`)
  - If empty after trim, fallback to `value`

Example:
```bash
TWEET_MISMATCH_LABEL="market"
```

## Determinism/Stability

Phrase choice remains deterministic for the same game key:
- Seed uses: `game_id:winner:loser:date`
- Selection uses stable SHA1 modulo phrase bank length
- For the same seed and phrase bank order, output phrase is stable across runs

## Observability Counters

Per-run enrichment summary emitted at:
1. `generate_daily_predictions`
2. `schedule_tweets` (batching stage)

Counters include:
- `confidence_tier_distribution` (`H`/`M`/`L` counts)
- `mismatch_count`
- `mismatch_rate`
- `total_game_lines`
- (batching stage only) `batched_tweets`

## Sample Output

```text
[enrichment-summary] stage=generate_daily_predictions data={"confidence_tier_distribution": {"H": 4, "L": 2, "M": 3}, "mismatch_count": 3, "mismatch_rate": 0.3333, "total_game_lines": 9}
[enrichment-summary] stage=schedule_tweets data={"batched_tweets": 2, "confidence_tier_distribution": {"H": 4, "L": 2, "M": 3}, "mismatch_count": 3, "mismatch_rate": 0.3333, "total_game_lines": 9}
```

## Counter Interpretation

- **High mismatch rate** can indicate more model-vs-market divergence on a slate.
- **Tier skew** (e.g., mostly `L`) can signal weaker confidence day; useful for operator review.
- **`total_game_lines` vs `batched_tweets`** helps validate expected batching behavior and line throughput.

## Reliability Notes

- No posting API behavior changes.
- Existing dedupe and scheduler idempotency paths remain intact.
- New parsing paths include safe defaults to avoid runtime failures from malformed env values.
