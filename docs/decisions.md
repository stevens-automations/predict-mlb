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

## Current

1) **Decision:** Promotion timing from `staging/preseason-consolidated` to `main`
- **Options:**
  - Promote after retrain + replay validation gates
  - Promote now and iterate on `main`
- **Recommendation:** Promote only after gates pass
- **Owner:** Steven
- **Due:** this week
- **Status:** open
- **Notes:** Reliability and data-contract gates should be explicit before promotion.

2) **Decision:** Data reliability contract strictness
- **Options:**
  - Strict fail-closed for all required features
  - Partial-degrade with confidence penalty
- **Recommendation:** Strict fail-closed for must-have; degrade only for optional features
- **Owner:** Steven / Mako
- **Due:** today
- **Status:** open
- **Notes:** Needed before retrain promotion and production scheduling.
