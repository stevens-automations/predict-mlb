# Phase A Anomalies

Date: 2026-03-08

## Summary
Two readiness anomalies were found during safe validation and fixed locally.

---

## Anomaly 1: Missing columns crash in prediction generation

- **Where:** `predict.generate_daily_predictions`
- **Symptom:** `KeyError: "['date', 'summary'] not in index"` when constructing `df_new = df_new[COLUMN_ORDER]`.
- **Cause:** Generated prediction payload can omit optional fields expected by `COLUMN_ORDER`.
- **Fix:** Backfill missing `COLUMN_ORDER` fields with `None` before column selection.
- **Validation:** Controlled run now reports `generated_count= 1` and no crash.

## Anomaly 2: Odds type crash in result update

- **Where:** `predict.update_row`
- **Symptom:** `TypeError: bad operand type for abs(): 'str'` during upset diff computation.
- **Cause:** Odds are stored as strings in SQLite path but upset logic assumed numeric odds.
- **Fix:** Added local odds coercion helper to safely parse odds to integers and guard when parsing fails.
- **Validation:** Controlled run now reports `accuracy_after_update= 1.0` and no crash.

---

## Post-fix status

- Unit tests: `17/17` passing.
- Safe readiness harness: all required Phase A paths passing.
