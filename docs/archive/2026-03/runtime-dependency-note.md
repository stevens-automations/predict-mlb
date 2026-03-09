# Runtime Dependency Note — LightGBM/OpenMP on Mac mini

Date: 2026-03-08

## Current blocker
End-to-end runtime dry-run is blocked because LightGBM cannot load:

`OSError: ... lib_lightgbm.dylib ... Library not loaded: @rpath/libomp.dylib`

This occurs during import path:
- `predict.py` -> `data.py` -> `lightgbm`

## Why this matters
Without `libomp`, we cannot run a true pipeline dry-run (preflight + prediction generation + scheduling simulation) on this machine.

## Non-janky long-term flow (recommended)
1. Install `libomp` once from an admin account on the host.
2. Keep assistant account **without Homebrew write privileges**.
3. Assistant only consumes the installed runtime library; no package-manager rights needed.

### Admin-side command (one-time)
```bash
brew install libomp
```

## Alternative paths
- **Conda/micromamba env with OpenMP runtime**: workable but introduces a second package-manager stack and added ops complexity.
- **Rebuilding LightGBM differently**: possible but higher maintenance and unnecessary for this project.

Recommendation: keep single-host dependency baseline and install `libomp` once via admin.

## Verification after install
Run from repo:
```bash
source .venv/bin/activate
python -c "import lightgbm; print('lightgbm import ok')"
python - <<'PY'
from apscheduler.schedulers.background import BlockingScheduler
import predict
predict.validate_runtime()
lines = predict.generate_daily_predictions()
predict.daily_scheduler = BlockingScheduler(job_defaults={"coalesce": False}, timezone=predict.eastern)
predict.schedule_tweets(lines or [])
print('dry-run scheduling jobs:', len(predict.daily_scheduler.get_jobs()))
PY
```

## Policy alignment
- No remote writes/pushes without explicit Telegram approval.
- No temporary/unsafe dependency hacks.
