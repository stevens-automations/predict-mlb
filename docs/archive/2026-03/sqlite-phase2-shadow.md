# SQLite Phase 2 Shadow Integration (No Cutover)

## Goal
Keep Excel as source of truth while optionally mirroring prediction writes/updates to SQLite.

## Feature flag
- Env var: `SQLITE_SHADOW_WRITE`
- Enabled values: `1`, `true`, `yes`, `on` (case-insensitive)
- Default/unset: disabled (no behavior change)

## Optional DB path
- Env var: `SQLITE_DB_PATH`
- Default: `data/predictions.db`

## Behavior
- Excel read/write remains primary and authoritative.
- When shadow flag is enabled:
  - New prediction rows are mirrored to SQLite.
  - Updated historical rows (`prediction_accuracy` and related result fields) are mirrored to SQLite.
- Shadow write failures are fail-open:
  - warning is logged
  - Excel flow continues without interruption

## Enable
1. Set in `.env`:
   - `SQLITE_SHADOW_WRITE=true`
   - Optional: `SQLITE_DB_PATH=data/predictions.db`
2. Run normal pipeline (`python3 main.py` or direct `predict.py` path).
3. Validate by checking SQLite row growth/parity.

## Disable / rollback
- Set `SQLITE_SHADOW_WRITE=false` (or unset it).
- No code rollback required.
- Pipeline immediately returns to Excel-only behavior.

## Safety notes
- This phase does **not** switch read-paths to SQLite.
- No production cutover; SQLite is mirror-only in this slice.
