# Phase B — Checkpoint 1 (Product Quality Upgrades)

## Scope Delivered

1. Confidence-aware tweet enrichment layer (no posting surface changes)
2. Odds mismatch signal for model-vs-market disagreement
3. Deterministic phrase variation to reduce repetitive line wording
4. Tests for confidence tiering, mismatch signaling, and tweet length safety

## What Changed

### 1) Confidence-aware enrichment

`server/tweet_generator.py`

- Added `derive_confidence_tier(prediction_value)`
  - Default thresholds:
    - `high >= 0.62`
    - `medium >= 0.55`
    - `low < 0.55`
- Added compact confidence tag in game lines:
  - `[H]`, `[M]`, `[L]`

### 2) Odds mismatch signal

- Added `has_market_mismatch(row, predicted_winner)`
- If predicted winner is different from market favorite, append compact signal:
  - `| value`

### 3) Deterministic phrase variation

- Added small phrase bank: `over`, `to beat`, `vs`
- Selection is deterministic via a stable hash seed (`game_id`, teams, date)
- Reduces repetitive output while preserving stable/reproducible tweet lines

### 4) Character safety with existing batching

- Kept existing tweet batching map/layout behavior in `create_tweets`
- Added per-line guardrail so each generated tweet remains within `TWITTER_MAX_CHAR_COUNT`

## Before / After Examples

### Before

- `NYY (-120) to defeat BOS (+110)`
- `LAD (-140) to defeat SF (+125)`

### After

- `NYY (-120) over BOS (+110) [H] | value`
- `LAD (-140) to beat SF (+125) [M]`

## Tuning Knobs

### Confidence thresholds

Environment variable:

- `PREDICTION_CONFIDENCE_THRESHOLDS="<high>,<medium>"`
- Example: `PREDICTION_CONFIDENCE_THRESHOLDS="0.75,0.60"`

Validation rules:

- both values must parse as floats
- `0 <= medium <= high <= 1`
- if invalid/missing, defaults are used

## Reliability Notes

- No change to posting surface (`send_tweet`, scheduler, posting flow unchanged)
- Existing dedupe + batching path preserved
- Guardrail added to prevent oversized tweets in enriched output mode
