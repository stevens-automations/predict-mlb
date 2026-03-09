# Open Decisions

Track unresolved decisions here for fast agent handoff.

## Template
- **Decision:**
- **Options:**
- **Recommendation:**
- **Owner:**
- **Due:** today / this week / later
- **Status:** open / decided
- **Notes:**

---

## Decided

1) **Decision:** Canonical historical database location
- **Decision:** `data/mlb_history.db` (repo-local SQLite)
- **Owner:** Steven
- **Status:** decided

2) **Decision:** Initial historical backfill scope
- **Decision:** 2020–2025 first, then extend if needed
- **Owner:** Steven
- **Status:** decided

3) **Decision:** Historical odds ingestion policy
- **Decision:** No historical odds backfill; odds are non-core for model and forward-only capture during season
- **Owner:** Steven
- **Status:** decided

4) **Decision:** Data reliability policy
- **Decision:** Strict contracts with degraded fallback predictions when must-have data is missing
- **Owner:** Steven / Mako
- **Status:** decided
- **Notes:** No silent game skipping. Degraded runs require explicit reason codes.

5) **Decision:** Incremental cadence (v1)
- **Decision:** pre-game + post-game only (no hourly intra-day in v1)
- **Owner:** Steven
- **Status:** decided

6) **Decision:** Model selection primary metric (initial)
- **Decision:** Start with log loss as primary metric
- **Owner:** Steven / Mako
- **Status:** decided

7) **Decision:** Ingestion reliability controls
- **Decision:** Bounded retries/backoff/jitter + timeout + request budget + checkpoint resume
- **Owner:** Steven / Mako
- **Status:** decided

---

## Open

1) **Decision:** Retention policy for forward-only odds snapshots
- **Options:** keep full in-season vs compact after N days
- **Recommendation:** keep full current season, compact after season close
- **Owner:** Steven / Mako
- **Due:** this week
- **Status:** open

2) **Decision:** Cutover timing from `staging/preseason-consolidated` to `main`
- **Options:**
  - promote after ingestion + model gates pass
  - promote earlier and continue hardening on main
- **Recommendation:** promote only after gates pass
- **Owner:** Steven
- **Due:** this week
- **Status:** open
