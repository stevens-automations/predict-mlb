# AGENT.md

Purpose: project operating contract for any agent working in this repository.

## 1) Mission
- **Goal:** Run the daily MLB prediction pipeline and publish prediction/result tweets reliably.
- **Primary outcome:** Correct predictions are generated, persisted, and posted once per intended schedule.
- **Out of scope (non-goals):** Major architecture rewrites, new product features, or provider migrations unless explicitly requested.

## 2) Success Criteria
- [ ] Functional requirements met
- [ ] Tests pass
- [ ] No critical regressions introduced
- [ ] Documentation updated for behavior changes

## 3) Hard Guardrails
- Security first. If a request is risky/unclear, stop and ask.
- Never expose secrets in code, logs, commits, or docs.
- Do not perform destructive actions without explicit approval.
- **Do not perform external write actions** (e.g., `git push`, PR/issue creation/edits, remote mutations) without explicit Steven approval via Telegram.

## 4) Repo Ground Truth
- **Stack / framework:** Python, pandas, statsapi, APScheduler, tweepy integration scripts.
- **Entry points:** `main.py`, `predict.py`, `data_retriever.py`.
- **Key directories:** `server/`, `models/`, `data/`, `tests/`, `docs/`.
- **Critical files:** `predict.py`, `data.py`, `server/get_odds.py`, `server/prep_tweet.py`, `main.py`.

## 5) Standard Commands
- Install: `pip install -r requirements.txt` (if requirements file is present)
- Dev/run: `python3 main.py`
- Test: `python3 -m unittest discover -s tests -p 'test*.py' -v`
- Lint/format: not standardized in-repo
- Build: n/a

## 6) Working Style
- Make the smallest correct change first.
- Prefer explicit, readable code over clever code.
- Keep changes scoped; avoid unrelated refactors.
- If assumptions are required, state them before proceeding.

## 7) Delivery Workflow
1. Understand task + constraints
2. Propose short plan
3. Implement in small, reviewable increments
4. Run validation (tests/lint/build as relevant)
5. Summarize: what changed, why, risks, next steps

## 8) Definition of Done (DoD)
- [ ] Acceptance criteria satisfied
- [ ] Validation commands executed successfully
- [ ] Edge cases considered for touched logic
- [ ] Notes/changelog/docs updated if needed

## 9) Open Decisions
Track unresolved decisions in `docs/decisions.md` (or project equivalent) with:
- Decision needed
- Options
- Recommendation
- Owner
- Due timing (today / this week / later)

## 10) Fast Handoff Format
When handing off work, include:
- **Status:** done / blocked / needs-review
- **What changed:**
- **Evidence:** tests/commands run
- **Risks:**
- **Next actions:** owner + due timing
