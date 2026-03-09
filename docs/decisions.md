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
- **Notes:** Workspace constraints and likely single-project usage make repo-local DB the best fit now.

2) **Decision:** Initial historical backfill scope
- **Decision:** 2020–2025 first, then extend if needed
- **Owner:** Steven
- **Status:** decided

3) **Decision:** Data reliability policy
- **Decision:** Strict contracts with degraded fallback predictions when must-have data is missing
- **Owner:** Steven / Mako
- **Status:** decided
- **Notes:** No silent game skipping. Every degraded run must record reason codes + incident logs for root-cause follow-up.

4) **Decision:** Model selection primary metric (initial)
- **Decision:** Start with log loss as primary metric; weighted score may be introduced later
- **Owner:** Steven / Mako
- **Status:** decided

---

## Open

1) **Decision:** Daily incremental cadence windows
- **Options:**
  - pre-game + post-game only
  - pre-game + hourly intra-day + post-game
- **Recommendation:** pre-game + post-game first; add intra-day after baseline stability
- **Owner:** Steven
- **Due:** this week
- **Status:** open

2) **Decision:** Retention policy for odds snapshots / feature versions
- **Options:** 30/90/unlimited days for high-frequency snapshots
- **Recommendation:** keep full season in-year; compact old high-frequency snapshots after season close
- **Owner:** Steven / Mako
- **Due:** this week
- **Status:** open

3) **Decision:** Promotion timing from `staging/preseason-consolidated` to `main`
- **Options:**
  - Promote after ingestion + model gates pass
  - Promote earlier and continue hardening on main
- **Recommendation:** promote only after gates pass
- **Owner:** Steven
- **Due:** this week
- **Status:** open
