# Remaining Work Before First Serious Integrated Model Run

Last updated: 2026-03-11

## Current State

- Pitcher appearances and bullpen support are backfilled.
- Lineup / platoon support is implemented; completed-game coverage is effectively complete and only postponed 2020 games remain without lineup snapshots.
- Weather / venue support is largely fixed, and weather historical support is effectively complete enough for downstream work.
- `feature_rows(v1)` remains the stable baseline.
- `feature_rows(feature_version='v2_phase1')` already exists as a materializer in code/tests but is not yet backfilled in the canonical DB.
- Retraining is intentionally deferred.

## What Is Done

- Historical ingestion foundation is in place.
- The richer support-table direction is implemented across pitcher appearances, bullpen, lineup / platoon, and weather / venue.
- The weather contract has been simplified enough to stop being the main blocker.
- The remaining work is no longer schema invention; it is final validation plus integrated materialization.

## What Is In Progress

- Final validation across bullpen, lineup / platoon, and weather support
- First canonical `v2_phase1` materialization and degraded-path review

## What Remains Before Training

### Checklist

- [ ] Keep the known residual support gaps explicit in audit/report output.
- [ ] Run one compact coverage / sanity pass over bullpen, lineup / platoon, and weather / venue support.
- [ ] Materialize the integrated feature rows and verify contract quality.
- [ ] Confirm training readiness gates: DQ checks are sufficient, degraded behavior is explicit, and review outputs are readable.
- [ ] Run the first serious integrated model training / evaluation pass.
- [ ] Decide whether the integrated contract is good enough to become the new baseline, or whether another data/validation pass is required before retraining continues.

## Optional / High-Value Later Work

- Add deeper lineup-quality and matchup interaction terms
- Expand weather realism beyond the first practical cutoff if later evidence says it matters
- Revisit secondary run-margin modeling after the main side model improves
