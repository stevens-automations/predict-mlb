# SQLite Migration Plan (Wave1 Planning Artifact)

## Scope
Plan migration of prediction persistence from `data/predictions.xlsx` to a local SQLite database with no runtime switch in this wave.

## Target Schema

### `predictions`
- `id` INTEGER PRIMARY KEY AUTOINCREMENT
- `game_id` INTEGER NOT NULL
- `date` TEXT NOT NULL (ISO8601)
- `time` TEXT
- `datetime` TEXT
- `home` TEXT NOT NULL
- `away` TEXT NOT NULL
- `home_probable` TEXT
- `away_probable` TEXT
- `predicted_winner` TEXT
- `predicted_winner_location` TEXT
- `model` TEXT
- `favorite` TEXT
- `prediction_value` REAL
- `prediction_accuracy` REAL
- `home_odds` TEXT
- `home_odds_bookmaker` TEXT
- `away_odds` TEXT
- `away_odds_bookmaker` TEXT
- `odds_retrieval_time` TEXT
- `prediction_generation_time` TEXT
- `home_score` INTEGER
- `away_score` INTEGER
- `winning_pitcher` TEXT
- `losing_pitcher` TEXT
- `venue` TEXT
- `series_status` TEXT
- `national_broadcasts` TEXT
- `summary` TEXT
- `tweet` TEXT
- `time_to_tweet` TEXT
- `tweeted` INTEGER NOT NULL DEFAULT 0
- `created_at` TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
- `updated_at` TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP

### Indexes
- `idx_predictions_game_id` on `(game_id)`
- `idx_predictions_date` on `(date)`
- `idx_predictions_tweeted_date` on `(tweeted, date)`
- `uq_predictions_game_date_model` UNIQUE on `(game_id, date, model)`

## Data Mapping from Current Excel Columns
- `tweeted?` (Excel) -> `tweeted` (SQLite integer 0/1)
- Timestamp-like columns (`date`, `datetime`, `odds_retrieval_time`, `prediction_generation_time`, `time_to_tweet`) -> ISO8601 TEXT
- Numeric scores (`home_score`, `away_score`) -> INTEGER nullable
- `prediction_value`, `prediction_accuracy` -> REAL nullable
- All team/metadata/tweet columns -> TEXT nullable unless listed NOT NULL

## Phased Migration Strategy (with Rollback)

### Phase 0: Readiness
- Freeze schema draft and example seed from current Excel snapshot.
- Build one-way importer script (Excel -> SQLite).
- Keep Excel as source of truth.

### Phase 1: Shadow Writes (optional after validation)
- Runtime continues reading/writing Excel.
- Add optional non-blocking mirror write to SQLite behind explicit feature flag.
- Log row counts and write errors only; do not fail pipeline on shadow-write error.

### Phase 2: Dual Read Validation
- Continue prod read path from Excel.
- Periodically compare query outputs between Excel-derived views and SQLite.
- Validate equivalence on key slices (today games, un-tweeted rows, game_id lookups).

### Phase 3: Controlled Cutover
- Switch read path to SQLite via config flag.
- Keep Excel write-back available for temporary rollback window.
- Run with enhanced monitoring for at least several daily cycles.

### Rollback
- Single flag revert to Excel read/write path.
- Rebuild SQLite from latest Excel snapshot if corruption/drift detected.
- Preserve failed SQLite file for forensic analysis before replacement.

## Code Adaptation Points / Modules
- `predict.py`
  - `load_unchecked_predictions_from_excel`
  - `generate_daily_predictions` (read existing + append new)
  - `mark_as_tweeted`
- `server/prep_tweet.py`
  - row lookup and in-place odds/tweet updates
- `paths.py`
  - add SQLite DB path env resolution (`PREDICTIONS_DB_PATH`)
- new storage abstraction module
  - introduce backend interface and Excel/SQLite adapters

## Validation Plan
- Unit tests for storage adapter parity:
  - read/write round-trip
  - missing column guardrails
  - game_id lookup behavior (missing row, duplicate keys)
- Golden dataset comparison:
  - Import fixed Excel fixture to SQLite
  - Compare deterministic query outputs against expected JSON snapshot
- Operational validation:
  - Daily run parity metrics (rows predicted, rows tweeted, rows updated)
  - Log and alert on mismatch > 0

## Cutover Checklist
- [ ] SQLite schema + migration SQL committed
- [ ] Importer script tested on latest real workbook backup
- [ ] Dual-read parity checks green for multiple consecutive runs
- [ ] Recovery/rollback command documented and tested
- [ ] Feature flag for read-source switch verified
- [ ] Backups of Excel + SQLite taken immediately before cutover
- [ ] Post-cutover monitoring enabled for scheduler + tweet state transitions
