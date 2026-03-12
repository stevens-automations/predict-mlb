# Schema + Feature Map

Last updated: 2026-03-11

This is the practical map of what a user gets locally after running the historical ingestion / rebuild flow against `data/mlb_history.db`.

## Mental model

**Training unit = one game = one row in `feature_rows`, joined to one row in `labels`.**

- `game_id` identifies the game
- `labels.did_home_win` is the training target
- `feature_rows.feature_payload_json` is the model input payload
- all support tables exist to make that one-row-per-game payload point-in-time safe

If you want to understand the DB quickly, think in this order:

1. `games` = schedule spine
2. `labels` = outcomes
3. support tables = pregame context built only from prior completed data or aligned pregame snapshots
4. `feature_rows` = final model-ready row per game/version

---

## Canonical local database

- **DB path:** `data/mlb_history.db`
- **Canonical training source:** `feature_rows + labels`
- **Current feature versions:**
  - `v1` = stable baseline team/starter feature set
  - `v2_phase1` = `v1` plus bullpen, lineup/platoon, and venue/weather blocks
- **Odds policy:** `odds_snapshot` exists, but historical backfill is intentionally disabled; odds are not part of the rebuild-derived training set

---

## Major table groups

## 1) Schedule / label spine

### `games`
One row per MLB game.

Contains:
- season/date/game type/status
- scheduled first pitch
- home/away team IDs
- venue ID
- final score fields when known

Use it for:
- the canonical game list
- ordering by date / season
- joining every other table

### `labels`
One row per completed game target.

Contains:
- `did_home_win`
- `home_score`, `away_score`
- `run_differential`, `total_runs`

Use it for:
- the supervised label for win prediction
- downstream run-margin / totals experimentation later

---

## 2) Raw/support tables used to derive pregame context

### `game_team_stats`
Two rows per game, one per side (`home` / `away`).

Contains completed-game team boxscore stats such as:
- runs, hits, errors
- batting average / OBP / SLG / OPS
- strikeouts, walks

Why it matters:
- drives rolling team form features
- lets the rebuild derive history locally instead of re-querying old boxscores repeatedly

### `game_pitcher_context`
Two rows per game, one per side.

Contains the probable starter identity plus parity-safe starter metrics:
- probable pitcher known flag / IDs / names
- season ERA / WHIP / runs per 9 / strike % / win %
- career ERA
- provenance fields such as `season_stats_scope` and `season_stats_leakage_risk`

Why it matters:
- this is the canonical starter-context layer used by `feature_rows`
- it replaces leakage-prone season endpoints with locally derived season-to-date stats

### `game_pitcher_appearances`
One row per pitcher appearance in a completed game.

Contains:
- starter vs reliever role
- outs / innings / batters faced / pitches / strikes
- hits / walks / strikeouts / runs / earned runs / HR
- holds / saves / inherited runner fields

Why it matters:
- it is the raw history behind bullpen support
- it is also the audit trail for pitcher-context derivation

---

## 3) Bullpen support tables

### `team_bullpen_game_state`
Two rows per game, one per side, as-of the game timestamp.

Contains compact bullpen summaries:
- sample size (`season_games_in_sample`, `bullpen_appearances_season`)
- quality (`bullpen_era_season`, `bullpen_whip_season`, `bullpen_k_minus_bb_rate_season`, `bullpen_hr_rate_season`)
- freshness / workload (`bullpen_outs_last3d`, `bullpen_pitches_last3d`, `relievers_back_to_back_count`, `high_usage_relievers_last3d_count`, `freshness_score`)

### `team_bullpen_top_relievers`
Two rows per game per `top_n` choice.

Current rebuild flow materializes `top_n=3` and `top_n=5`; `v2_phase1` uses **top 3**.

Contains:
- number available
- top-N quality/freshness summaries
- `selected_pitcher_ids_json`
- `quality_dropoff_vs_team`

Why both bullpen tables matter:
- `team_bullpen_game_state` captures the full pen
- `team_bullpen_top_relievers` captures late-inning / leverage talent and whether the top arms are actually available

---

## 4) Lineup / handedness / platoon tables

### `player_handedness_dim`
One row per known player.

Contains:
- bat side
- pitch hand
- basic identity / position metadata

### `game_lineup_snapshots`
One row per lineup slot / player snapshot.

Contains:
- side, snapshot time, snapshot type
- lineup status
- batting order
- player handedness / position

Historical backfill currently stores completed-game lineup support as **fallback snapshots**, not guaranteed true announced pregame snapshots.

### `team_lineup_game_state`
Two rows per game, one per side.

Contains lineup-level summaries such as:
- lineup known flag / announced count / lineup status
- left/right/switch counts for full lineup and top of order
- handedness share proxies and balance score
- lineup quality summary fields
- `lineup_vs_rhp_quality` / `lineup_vs_lhp_quality`

### `team_platoon_splits`
Two rows per game per side per opposing pitch hand.

Contains team offense vs LHP/RHP, computed from prior completed local history:
- games in sample
- plate appearances
- AVG / OBP / SLG / OPS
- runs per game
- strikeout rate / walk rate

Why these lineup tables matter:
- `team_lineup_game_state` tells you what lineup is known and what shape it has
- `team_platoon_splits` provides the stable fallback offense-vs-hand block even when lineup details are weak or missing

---

## 5) Venue / weather tables

### `venue_dim`
One row per venue.

Contains:
- venue identity / coordinates / timezone
- roof type
- `weather_exposure_default`

### `game_weather_snapshots`
One row per game per aligned weather snapshot.

Contains:
- `snapshot_type` (`observed_archive` historically, `forecast` for live pregame refresh)
- temperature / humidity / pressure / precipitation / wind / cloud cover
- day/night and exposure flags
- snapshot timing relative to first pitch

Why this matters:
- venue is durable static context
- weather is a game-level context block and is optional/degradable, not row-failing

---

## 6) Final materialized model rows

### `feature_rows`
**This is the table that matters most for training.**

One row per game per feature version.

Columns:
- `game_id`
- `feature_version`
- `as_of_ts`
- `feature_payload_json`
- `source_contract_status`
- `source_contract_issues_json`

Use it for:
- training set creation
- inference parity
- degraded-row auditing

### What `v1` contains

`v1` is the stable baseline and currently materializes a compact **56-key** payload centered on:
- game identity / season / date
- home-field flag
- team season strength
- team rolling last-10 form
- days rest / doubleheader flags
- probable starter known/stats block
- availability flags for team-strength / rolling / starter support

In plain English: `v1` is the safe backbone.

### What `v2_phase1` adds

`v2_phase1` keeps all of `v1` and extends it to a **187-key** payload with four practical additions:

1. **Bullpen quality + freshness**
   - home/away bullpen ERA, WHIP, K-BB, HR rate
   - recent bullpen outs/pitches
   - fatigue / low-sample / availability flags
   - top-3 reliever support and deltas

2. **Lineup / platoon support**
   - lineup known / partial / missing flags
   - lineup handedness shares and top-3 handedness counts
   - lineup quality summaries when available
   - offense-vs-opposing-starter-hand platoon metrics
   - deltas between home and away matchup quality

3. **Venue / weather context**
   - roof type / roof-closed-or-fixed flag
   - temperature / wind / gust / direction
   - humidity / pressure / precipitation / cloud cover / day flag
   - weather source / availability flags

4. **Integrated matchup deltas**
   - bullpen deltas
   - lineup balance / handedness deltas
   - lineup-vs-hand deltas
   - top-3 quality / freshness deltas

In plain English: `v2_phase1` is the first full integrated feature store row, not just a baseline row with a few extras.

---

## Acceptable degraded / missingness behavior

The rebuild contract is **do not silently skip games just because a secondary support block is missing**.

## Valid row behavior

A row is still acceptable when the core `v1` spine exists and one or more richer support blocks are partially missing.

Expected behavior by block:

### Starter context
- if probable starter is unknown, starter stat fields may be `NULL`
- `*_starter_known` and `*_starter_stats_available` stay explicit
- this is acceptable; it should not trigger unsafe backfilling from future-aware endpoints

### Team rolling / season form
- early-season rows can have thin samples
- counts / availability flags must show that explicitly
- thin history is acceptable; hidden imputation is not

### Bullpen support
- bullpen support is expected, but missing bullpen rows should degrade rather than drop the game
- quality metrics may be `NULL`
- availability / low-sample / fatigue flags should explain what happened

### Lineup / platoon support
- lineup snapshots are valuable but degradable
- if lineup is unknown, lineup structure metrics can be `NULL` and `lineup_known_flag=0`
- platoon fallback can still keep the row useful
- missing lineup quality alone is acceptable if structure/platoon support still exists

### Weather / venue support
- venue fields can still exist even if numeric weather is missing
- `weather_available_flag=0` is acceptable
- weather is secondary; it should not invalidate an otherwise good row

## Invalid row behavior

A row should only be treated as truly broken when the required `v1` backbone is broken, for example:
- no canonical game identity
- no joinable game/label structure
- corrupted or missing required baseline fields
- leakage-risk starter context being treated as safe

---

## What a user should expect after a full rebuild

If the rebuild runs successfully, the user should expect:

1. a canonical SQLite DB at `data/mlb_history.db`
2. one durable schedule row per game in `games`
3. one outcome row per completed game in `labels`
4. durable raw/support tables for:
   - team boxscore stats
   - pitcher appearances
   - pitcher context
   - bullpen state
   - lineup/platoon state
   - venue/weather state
5. one materialized feature row per game per feature version in `feature_rows`
6. the ability to train from `feature_rows + labels` without touching notebooks or ad hoc API pulls

---

## Minimal query path for most users

If you only care about model training inputs, these are the only tables you usually need:

- `feature_rows`
- `labels`
- `games`

Everything else is there to make those rows reproducible and auditable.

For richer debugging, inspect:
- `game_pitcher_context`
- `team_bullpen_game_state`
- `team_lineup_game_state`
- `team_platoon_splits`
- `game_weather_snapshots`

---

## Rebuild sequence that produces this state

From repo root:

```bash
python scripts/history_ingest.py init-db
python scripts/history_ingest.py backfill --season 2021
python scripts/history_ingest.py backfill-team-stats --season 2021
python scripts/history_ingest.py backfill-pitcher-appearances --season 2021
python scripts/history_ingest.py backfill-bullpen-support --season 2021 --top-n-values 3,5
python scripts/history_ingest.py backfill-lineup-support --season 2021
python scripts/history_ingest.py backfill-pitcher-context --season 2021 --repair-mode
python scripts/history_ingest.py materialize-feature-rows --season 2021 --feature-version v1
python scripts/history_ingest.py materialize-feature-rows --season 2021 --feature-version v2_phase1
```

For command details and operational caveats, see `docs/runbooks/historical-ingestion-runbook.md`.
