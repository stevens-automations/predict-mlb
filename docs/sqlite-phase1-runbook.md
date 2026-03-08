# SQLite Phase 1 Runbook (Behavior-Preserving)

Excel remains the runtime source of truth in this phase.
SQLite is initialized/imported/checked offline for migration readiness.

## 0) Preconditions
- Python venv active
- Existing Excel sheet present (`data/predictions.xlsx` by default)

## 1) Initialize SQLite schema
```bash
python scripts/sqlite_init.py
```
Optional override:
```bash
PREDICTIONS_DB_PATH=data/predictions.db python scripts/sqlite_init.py
```

## 2) Import Excel -> SQLite (one-way)
Replace table contents (default):
```bash
python scripts/import_excel_to_sqlite.py
```
Append mode:
```bash
python scripts/import_excel_to_sqlite.py --append
```
Explicit paths:
```bash
python scripts/import_excel_to_sqlite.py \
  --excel data/predictions.xlsx \
  --db data/predictions.db
```

## 3) Run parity checker (read-only)
```bash
python scripts/check_excel_sqlite_parity.py --strict
```
Custom key fields:
```bash
python scripts/check_excel_sqlite_parity.py \
  --key-fields game_id,date,home,away,model \
  --strict
```

## Rollback / Recovery Notes
Since runtime is still Excel-backed, rollback is simple:
1. Stop using SQLite tooling for the run.
2. Remove/recreate the SQLite file if drift/corruption is suspected:
   ```bash
   mv data/predictions.db data/predictions.db.bak.$(date +%Y%m%d-%H%M%S)
   python scripts/sqlite_init.py
   python scripts/import_excel_to_sqlite.py
   ```
3. Re-run parity:
   ```bash
   python scripts/check_excel_sqlite_parity.py --strict
   ```

No runtime switch or production write-path changes are performed in this phase.
