# Predict-MLB TODO

Last updated: 2026-03-11

## Current State

- The canonical DB is promoted.
- This queue is now for stabilization and cleanup sequencing, not training.

## What Is Done

- Canonical historical DB recovered and promoted
- Historical ingestion / support-table foundation present
- `feature_rows(v1)` retained as the stable baseline

## What Is In Progress

- Canonical DB workflow hardening
- Durable rebuild-path / CLI scoping
- Canonical training-manifest consolidation from outside research

## What Remains Before Training

- [ ] Document and enforce the protected-canonical-DB boundary in the main ingestion workflow.
- [ ] Collapse rebuild guidance toward one durable CLI / path and identify remaining one-off scripts to retire later.
- [ ] Perform the broader repo cleanup pass after the rebuild path is settled.
- [ ] Consolidate canonical docs and archive superseded notes.
- [ ] Cut a clean checkpoint commit before any training execution resumes.

## Optional / High-Value Later Work

- [ ] Add deeper lineup-quality features after the first integrated run.
- [ ] Add richer park / weather interaction features after the first integrated run.
- [ ] Revisit secondary run-margin modeling later.
