# Post-Promotion Stabilization Plan

Last updated: 2026-03-11

## Current State

- The canonical historical DB has been recovered and promoted.
- Training is intentionally deferred.
- The active phase is stabilization of the canonical DB workflow and preparation of a durable rebuild path.

## What Is Done

- Historical ingestion foundation is in place.
- The canonical DB exists locally at `data/mlb_history.db`.
- The historical support-table and baseline feature-row path already exist.

## What Is In Progress

- Canonical DB workflow protection
- Durable rebuild-path / CLI scoping
- Top-level doc consolidation
- Legacy surface isolation where coupling is low and confidence is high

## What Remains Before Training

### Checklist

- [ ] Protect and lock the canonical DB workflow.
- [ ] Finish the durable rebuild path / CLI with minimal durable script surface area.
- [ ] Perform comprehensive repo cleanup and consolidate one-off artifacts into canonical homes or archive.
- [ ] Consolidate and update canonical docs so one file owns each concern.
- [ ] Cut a clean checkpoint commit before starting any training work.

## Optional / High-Value Later Work

- Add deeper lineup-quality and matchup interaction terms
- Expand weather realism beyond the first practical cutoff if later evidence says it matters
- Revisit secondary run-margin modeling after the main side model improves
