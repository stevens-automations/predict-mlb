# Detection Tuning Guide (Non-Live / Test-Data Ready)

This pipeline currently uses enrichment observability as its anomaly signal surface (not model-drift stats). Alerts are produced by `_emit_enrichment_threshold_warnings` in `predict.py`.

## What is detected

Two warning classes:

- `enrichment_mismatch_rate_high`
  - High share of lines marked with market mismatch (`| value`)
- `enrichment_low_confidence_rate_high`
  - High share of low-confidence lines (`[L]`)

## False-positive controls (high impact)

To reduce noisy alerts on small slates / sparse runs, warnings now require:

1. **Rate threshold breached**
2. **Minimum sample size reached**
3. **Minimum absolute count reached**

This combination avoids triggering on tiny denominators (e.g., 1/1 or 2/2).

## Default tuning values

These defaults are conservative for MLB daily run sizes:

- `ENRICHMENT_MISMATCH_RATE_WARN=0.60`
- `ENRICHMENT_LOW_CONFIDENCE_RATE_WARN=0.70`
- `ENRICHMENT_MIN_SAMPLE_WARN=5`
- `ENRICHMENT_MIN_MISMATCH_COUNT_WARN=3`
- `ENRICHMENT_MIN_LOW_CONFIDENCE_COUNT_WARN=3`

Rationale:
- Rate thresholds preserve sensitivity to broad degradation.
- Count + sample gates reduce false positives when only a few games are generated.

## Suggested tuning workflow (offline)

1. Run simulation / fixture-backed mode and collect `docs/reports/enrichment-*.jsonl`.
2. Compute distribution of:
   - `total_game_lines`
   - `mismatch_rate`, `mismatch_count`
   - low-confidence rate/count (`L` bucket)
3. Set thresholds so normal historical runs produce near-zero warnings, and known degraded fixtures still trigger.
4. Re-run tests and a dry-run pipeline.

## Quick profile suggestions

- **Low-noise profile** (production default):
  - keep current defaults
- **Early-warning profile** (higher sensitivity):
  - lower rates by ~0.05 to 0.10
  - keep minimum counts/samples to protect against denominator noise

## Notes

- These warnings are readiness/health signals, not hard failure conditions.
- For true anomaly escalation, consume the JSONL reports and apply run-over-run trend logic externally.
