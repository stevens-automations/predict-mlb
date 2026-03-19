# AGENT.md — predict-mlb Operating Contract
Last updated: 2026-03-18

## 1. Mission
Build and maintain a clean MLB game prediction system. Primary current phase: **full feature layer refactor** to achieve >66% accuracy on 2025 holdout (up from ~52% baseline).

**Read `docs/REFACTOR_SPEC.md` before doing any development work in this project.** It is the canonical reference for all architectural decisions.

## 2. Current Phase: Feature Layer Refactor
We are rebuilding the engineered feature layer from scratch. The raw historical tables are solid and stay untouched. See `docs/REFACTOR_SPEC.md` for full spec.

**Phase 1 (active):** Build 6 new Layer 2 tables:
- `player_career_pitching_stats` — one-time batch fetch from statsapi
- `team_pregame_stats` — cumulative team batting/record stats going into each game
- `starter_pregame_stats` — starter ERA/WHIP/K% computed from game_pitcher_appearances
- `bullpen_pregame_stats` — season bullpen quality + fatigue metrics
- `lineup_pregame_context` — lineup handedness vs opposing starter
- `team_vs_hand_pregame_stats` — team OPS vs LHP/RHP starters this season

**Phase 2:** Assemble `game_matchup_features` flat training/inference row from all Layer 2 tables.

**Phase 3:** Train LightGBM baseline + logistic regression benchmark. Target >66% holdout.

**Phase 4:** Cleanup — drop old v1/v2 tables and code.

## 3. Hard Guardrails
- **Do NOT modify raw tables:** `games`, `game_team_stats`, `game_pitcher_appearances`, `game_lineup_snapshots`, `game_weather_snapshots`, `labels`, `player_handedness_dim`, `venue_dim`
- **Do NOT push to git** without explicit Mako/Steven approval
- **Do NOT touch main branch** — all work on `staging/preseason-consolidated`
- **Do NOT modify `data/mlb_history.db` raw data** — only add new derived tables
- Canonical DB: `data/mlb_history.db` — never disposable
- Use repo venv for all Python; do not install system packages
- Commit as `stevensautomations`

## 4. Verification Standard
Always verify new tables using **game_id=661199 (Aug 15, 2022)** — a mid-season game with full data. Never use opening day games for verification (cold-start NULLs expected there).

Expected values for game 661199:
- Dodgers (away, team_id=119): ~112 games, ~0.699 win_pct, strong OPS
- Julio Urías (pitcher_id=628711): ~22 starts, ERA ~2.50, strike_pct ~0.693

## 5. Key Files
| File | Purpose |
|------|---------|
| `docs/REFACTOR_SPEC.md` | **Primary spec** — all architectural decisions |
| `docs/STATUS.md` | Current project status |
| `data/mlb_history.db` | Canonical DB |
| `scripts/history_ingest.py` | Historical data pipeline (large — read targeted sections) |
| `scripts/sql/history_schema.sql` | DB schema |
| `scripts/training/train_lgbm.py` | LightGBM training |
| `configs/training/` | Training configs |
| `artifacts/model_registry/` | Model artifacts |

## 6. Tooling
- **Primary:** Codex CLI — `codex -m gpt-5.4` for all code execution
- **Agent reasoning layer:** Claude Sonnet 4.6 (the agent itself — not for code execution)
- Repo venv: `source .venv/bin/activate`
- Tests: `python3 -m unittest discover -s tests -p 'test*.py' -v`

## 7. Handoff Format (always use when reporting to Mako)
```
Status: done | blocked | needs-review
What changed:
Evidence: (row counts, verification results for game 661199, metrics)
Risks:
Next actions:
```
