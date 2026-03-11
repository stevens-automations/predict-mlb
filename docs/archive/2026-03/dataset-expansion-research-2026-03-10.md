# Predict-MLB dataset expansion research memo

Date: 2026-03-10
Repo: `projects/predict-mlb`

## Executive summary

The current pipeline is still missing several of the most baseball-specific pregame signals that matter for MLB winner prediction. The good news is that the core raw data is more available than it looked at first.

Most importantly:

1. **Bullpen state is realistically reconstructable historically from MLB Stats API boxscores/live feeds.** MLB Stats API does not expose a first-class “bullpen” object, but it does expose enough pitcher-appearance data to rebuild bullpen usage, role, and fatigue proxies game by game.
2. **Handedness/platoon context is also realistically available.** Starter handedness, hitter handedness, active rosters, batting order slots, and even team split stats vs left/right pitching are available from Stats API endpoints / game feeds.
3. **Weather and park context are obtainable with low friction.** Stats API game feed includes game weather text and wind text for played games; Open-Meteo gives clean historical hourly weather and same-day forecast weather by stadium coordinates.
4. **The best immediate expansion is not “fancier models”; it is a richer pregame state layer.** Specifically: bullpen fatigue/quality, starter handedness + team-vs-handedness splits, and park/weather context.
5. **Inference-time realism has to drive design.** Historical training can use observed postgame boxscores to reconstruct *prior* state, but target-game features must only depend on information known before first pitch (or at the chosen forecast cutoff).

Bottom line: we should add new raw/support tables for pitcher appearances, lineup snapshots, roster snapshots, venue dimension, and weather snapshots, then materialize a tighter set of robust pregame features from those tables.

---

## What I inspected

### Repo state

Relevant current repo artifacts:

- `scripts/history_ingest.py`
- `scripts/sql/history_schema.sql`
- `docs/research/feature-contract-v1.md`
- `docs/research/model-data-feasibility-audit-2026-03-09.md`
- `docs/research/next-gen-modeling-plan-2026-03-10.md`

Current SQLite state in `data/mlb_history.db`:

- `games`: populated for 2020-2025
- `labels`: populated
- `game_team_stats`: populated (`26614` rows)
- `game_pitcher_context`: populated (`26618` rows)
- `feature_rows`: populated (`13309` rows)
- Tables present: `games`, `labels`, `game_team_stats`, `game_pitcher_context`, `feature_rows`, `odds_snapshot`, `ingestion_runs`, `ingestion_checkpoints`, `dq_results`

Season counts currently loaded in `games`:

- 2020: 953 games (951 final)
- 2021: 2466 (2466 final)
- 2022: 2470 (2470 final)
- 2023: 2471 (2471 final)
- 2024: 2472 (2472 final)
- 2025: 2477 (2477 final)

### Current pipeline limitations

The repo already improved leakage safety for starters by deriving season-to-date stats from prior completed games. But it still does **not** have a dedicated raw layer for:

- every pitcher appearance in every game
- bullpen-only reconstructed state
- lineup snapshots / confirmed batting orders
- hitter handedness aggregates by lineup
- venue dimension / park metadata layer
- weather snapshots tied to pregame timestamps

That is the main gap.

---

## Outside research findings

## 1) MLB Stats API / related baseball data availability

### A. Schedule data: good for daily inference indexing, starter identity, venue id, day/night

Observed and documented from `statsapi.schedule(...)` and direct Stats API schedule payloads:

Available fields include:

- `game_id` / `gamePk`
- `game_datetime`, `game_date`
- `status`
- home/away team ids
- probable pitchers
- `venue_id`, `venue_name`
- `dayNight`
- doubleheader / game number

This is already enough to anchor pregame joins for:

- starter identity (when posted)
- venue / park id
- rest / travel / doubleheader context
- game-time weather lookup by scheduled start time and venue coordinates

### B. Boxscore/live feed data: enough to reconstruct bullpen and lineups

Concrete observations from direct Stats API responses:

- `/api/v1/game/{gamePk}/boxscore` exposes:
  - per-team `teamStats`
  - per-player `stats.pitching`, `stats.batting`, `stats.fielding`
  - pitcher appearance stats including `inningsPitched`, `numberOfPitches`, `strikes`, `holds`, `saves`, `blownSaves`, `inheritedRunners`, `inheritedRunnersScored`, `gamesStarted`
  - player `battingOrder`
  - player `allPositions`
  - player `gameStatus.isSubstitute`
- `/api/v1.1/game/{gamePk}/feed/live` exposes:
  - rich `gameData.players` objects with `batSide` and `pitchHand`
  - `gameData.datetime.dayNight`
  - `gameData.weather.condition`
  - `gameData.weather.wind`
  - `gameData.probablePitchers`
  - venue/team/player metadata

This matters because it means:

- **Bullpen appearances are recoverable historically** from target-game-minus-1 boxscores.
- **Starter vs reliever separation is recoverable** via `gamesStarted` / starter identity / first pitcher used.
- **Historical batting orders are recoverable** from boxscore/live feed snapshots for played games.
- **Player handedness is recoverable** from feed player metadata.

### C. Bullpen is not explicit, but reconstructable enough

MLB Stats API does not appear to have a dedicated bullpen entity like “team bullpen status.” But that is not a blocker.

Bullpen can be reconstructed using:

1. active roster / pitcher roster
2. probable starter identity
3. actual pitcher appearance lines by game
4. appearance order / starter flag / innings / pitches / leverage-ish outcomes
5. rolling prior-game windows

At minimum, we can derive for each team and game date:

- which pitchers are likely in the bullpen pool
- which relievers threw in the last 1/2/3/5/7 days
- who threw back-to-back days
- who had heavy pitch-count outings recently
- bullpen innings over recent windows
- bullpen quality from prior appearances only

That is enough to create useful, realistic pregame bullpen features.

### D. Starter handedness: clearly available

Starter handedness is straightforward:

- probable starter from schedule/live feed
- pitcher id -> `pitchHand`

This should be treated as foundational because it unlocks:

- team hitting split vs starter hand
- likely lineup handedness balance
- platoon exposure proxies

### E. Hitter handedness: clearly available

The live feed `gameData.players` payload includes `batSide` and `pitchHand` for players. That means hitter handedness can be captured historically for players in actual lineups and also for active roster snapshots if combined with roster + people lookups.

This supports:

- lineup left/right/switch composition
- aggregate lineup strength vs starter hand
- bench flexibility proxies later

### F. Historical lineups: available, but treat confirmation timing carefully

Historical played-game lineups are available from boxscore/live feed via player-level `battingOrder` and game participation metadata.

That means we can reconstruct:

- actual starting lineup by slot
- actual lineup handedness mix
- actual lineup strength ex post

But there is a design choice:

- If the production system will only have **confirmed lineups close to first pitch**, then training rows should only use confirmed-lineup features if we can timestamp them appropriately.
- If we cannot recover historical confirmation timestamps reliably, the safe fallback is to use **team-level or active-roster-based platoon proxies** rather than actual target-game lineups for all training rows.

Recommendation: use actual historical lineups initially for **research / ablation**, but do **not** make them core production features until the inference-time acquisition path and cutoff policy are fully specified.

### G. Team-vs-handedness split features: realistically available now

Direct team stats endpoint test:

- `/api/v1/teams/{teamId}/stats?stats=statSplits&group=hitting&sitCodes=vr,vl&season=2024`

This returns team hitting splits vs right-handed and left-handed pitching.

Important caveat: this looks like **season aggregate** output for the full requested season, so by itself it is leakage-prone for historical training if queried retrospectively.

However, this still proves the split family is conceptually supported. The correct implementation is:

- either materialize daily/as-of snapshots during the season
- or derive as-of-game team split stats locally from prior batter-game data / lineup history
- or start with rolling team hitting performance split by opponent starter hand using only prior games

### H. Park / venue ids: clearly available; park factors can be joined later

Schedule payloads provide `venue_id` and `venue_name`. That is enough to build a local venue dimension and later join:

- park factors
- stadium coordinates
- timezone
- retractable roof flag / indoor flag
- altitude / elevation

Even if park factors come from a secondary source later, the Stats API venue id solves the join key problem.

### I. Retrosheet: strong fallback / enrichment source, especially for historical lineups and play-by-play

Retrosheet remains a strong optional historical supplement.

Useful properties:

- very broad historical play-by-play coverage
- explicit start/sub records and rich event data
- published park codes and game metadata
- permissive use notice for downstream data products with attribution requirements

Retrosheet is probably not needed for the first next-gen upgrade if Stats API coverage is sufficient for 2020+ and current goals. But it is a good fallback if:

- Stats API historical lineup extraction proves brittle
- we want richer appearance order / substitution logic
- we want deeper event-derived bullpen or lineup quality features later

My view: **do not add Retrosheet in phase 1** unless Stats API extraction materially fails. Keep it as a phase-2 fallback.

---

## 2) Weather / environment data availability

### A. Stats API weather fields exist, but are not enough as the primary source

Observed in live feed:

- `weather.condition`
- `weather.wind` (string text)

Useful, but limited:

- likely only for games with feed coverage
- wind is stringy / messy
- may not be reliable enough as the canonical historical weather source
- unclear how consistently it is available pregame versus in-game versus final

Recommendation: use Stats API weather text only as a **secondary sanity field**, not as the canonical weather pipeline.

### B. Open-Meteo is a very practical primary weather source

Directly confirmed:

- Forecast endpoint can return hourly `temperature_2m`, `relative_humidity_2m`, `precipitation`, `wind_speed_10m`, `wind_direction_10m`, `wind_gusts_10m`, `pressure_msl`
- Archive endpoint can return the same style of hourly historical weather for past dates
- Timezone-aware hourly output is easy to align with local game time

This is a strong fit for the project because it gives:

- free / low-friction access
- both historical and forward-looking weather from the same provider family
- structured hourly fields instead of scraped text

### C. What weather fields are most worth trying for MLB?

For side/winner prediction, the highest-value weather/environment fields are probably not as large as bullpen or starter context, but they are still meaningful, especially via run environment and volatility.

Best candidates:

1. **Temperature**
2. **Wind speed**
3. **Wind direction** relative to park geometry if we later support that; otherwise use coarse speed only at first
4. **Wind gusts**
5. **Precipitation / rain probability proxy**
6. **Humidity**
7. **Pressure**
8. **Day/night**
9. **Roof-open / indoor / retractable-roof-open proxy**

For an initial build, I would not overcomplicate this. Start with:

- temperature
- wind speed
- wind direction
- precipitation
- humidity
- pressure
- day/night
- indoor/retractable roof coarse flags

### D. Roof / park enclosure matters, but data may need a manual dimension table

The biggest practical issue with weather is not the API itself; it is **whether outside weather actually matters in the stadium**.

For domes / closed-roof games, outside wind should have near-zero effect.

So we need a local venue dimension with at least:

- venue_id
- venue_name
- latitude
- longitude
- timezone
- roof_type (`open`, `retractable`, `fixed_dome`)
- weather_exposure_default (`1/0`)

If we cannot reliably source same-day roof-open status, the conservative first pass is:

- fixed domes: suppress outdoor weather effect
- open-air parks: use weather normally
- retractable roofs: use outdoor weather but add a retractable-roof flag and accept some noise

Later, if roof status can be inferred from feed/weather text or another source, we can improve it.

---

## 3) What additional data is actually available historically?

## Historically available now

### Bullpen

Available historically from Stats API / local derivation:

- actual pitcher appearances by game
- innings / outs / pitches / strikes
- runs / earned runs / hits / walks
- holds / saves / blown saves
- inherited runners / inherited runners scored
- whether a pitcher started that game
- probable starter identity for the game
- active roster snapshots by date

This is enough to build historical team bullpen state before each game.

### Handedness / lineup / platoon

Available historically:

- probable starter identity
- starter `pitchHand`
- player `batSide`
- player `pitchHand`
- actual batting order via boxscore/live feed for played games
- active rosters by date
- venue id / dayNight

Potentially available but more fragile:

- exact confirmed lineup timing
- exact projected lineup before first pitch

### Weather / park

Available historically:

- venue id from schedule
- day/night from schedule or feed
- feed weather text/wind text for played games
- historical hourly weather from Open-Meteo using stadium coordinates and game time

Needs local/manual enrichment:

- venue coordinates master
- roof type / indoor exposure
- optional park factors

---

## 4) What additional data is available at daily inference time?

## Available at daily inference time now or with low-friction buildout

### Bullpen

- schedule / game ids
- probable starters (usually available, though not always early)
- active roster by date
- all prior completed games in local DB
- therefore all locally derived bullpen usage/fatigue/quality features up to that morning / cutoff

This is very realistic and should be a priority.

### Handedness / platoon

- starter handedness once probable starter is known
- active roster handedness
- rolling team hitting splits vs starter hand derived locally from prior games
- optionally team-level Stats API split endpoint for same-day operational convenience, but not for retrospective training without snapshots

### Lineups

At inference time this depends on the operational cutoff:

- **Morning model**: usually no confirmed lineups yet; use team-vs-hand and roster-based proxies.
- **Closer-to-first-pitch model**: confirmed lineups often become available; then lineup-strength features become viable.

So lineup features are available at inference time, but only if we explicitly define:

- when predictions are generated
- whether a refresh model runs after lineups post

### Weather / park

- park/venue id immediately available
- forecast weather from Open-Meteo available same day
- day/night known from schedule
- retractable/open-air flag from venue dimension

This is operationally easy.

---

## 5) Highest-value data families for MLB winner prediction

My ranking, considering both expected predictive value and feasibility:

### 1. Bullpen state / quality / fatigue

**Why it matters:** MLB games are heavily decided by the innings after the starter exits. Weak models often ignore this or reduce it to vague team quality.

**Why it is valuable:**

- captures hidden team strength not present in basic standings
- matters more when starters are short-leash / weak / returning from injury
- interacts with recent usage, doubleheaders, extra innings, and rotation instability
- likely improves both mean accuracy and calibration

**Why it is feasible:** reconstructable from historical boxscores using prior completed games only.

### 2. Starter handedness + team-vs-handedness split context

**Why it matters:** matchup quality changes materially when a team that mashes LHP faces a lefty, or vice versa.

**Why it is feasible:** starter hand is easy; team split features can be derived locally.

### 3. Park + weather environment

**Why it matters:** affects scoring environment, HR environment, and game volatility. For side prediction the effect is often secondary, but still useful, especially with extreme wind/heat/park combinations.

**Why it is feasible:** easy from venue id + weather API.

### 4. Confirmed lineup quality / handedness

**Why it matters:** can be very valuable when stars sit, lineups are bench-heavy, or platoon-optimized.

**Why it ranks fourth:** the signal is good, but the **operational and leakage risks are higher** unless we define a lineup-aware inference workflow.

### 5. Park-factor joins / richer venue physics

Useful, but lower immediate value than the four above.

---

## 6) Recommended schema / dataset representation

The main principle: separate **raw support tables** from **materialized pregame feature tables**.

## New raw/support tables to add

### A. `game_pitcher_appearances`

Purpose: canonical per-pitcher, per-game appearance rows derived from final boxscores/live feed.

Suggested columns:

- `game_id`
- `team_id`
- `side`
- `pitcher_id`
- `pitcher_name`
- `appearance_order` (if derivable)
- `is_starter`
- `is_reliever`
- `entered_inning`
- `outs_recorded`
- `innings_pitched`
- `batters_faced`
- `pitches`
- `strikes`
- `hits`
- `walks`
- `runs`
- `earned_runs`
- `home_runs`
- `strikeouts`
- `holds`
- `save`
- `blown_save`
- `inherited_runners`
- `inherited_runners_scored`
- `decision_win`
- `decision_loss`
- `source_updated_at`

This is the foundation for bullpen features.

### B. `team_bullpen_daily`

Purpose: precomputed team bullpen state as of each game date or daily cutoff.

Suggested keys:

- `team_id`
- `as_of_date`
- optional `as_of_ts`

Suggested fields:

- active relievers count
- bullpen innings last 1/3/5/7 days
- bullpen pitches last 1/3/5 days
- relievers used yesterday count
- relievers used back-to-back count
- relievers with 25+ pitches yesterday count
- relievers with 2+ of last 3 days used
- closer_used_yesterday flag
- top_3 reliever outs last 3 days
- bullpen ERA/FIP-like proxy season-to-date prior games
- bullpen WHIP prior games
- bullpen K-BB proxy prior games
- bullpen inherited-runner strand proxy prior games

If storage is a concern, this can be materialized per game instead of daily.

### C. `game_lineup_snapshot`

Purpose: player-level lineup rows for a game.

Suggested columns:

- `game_id`
- `team_id`
- `side`
- `player_id`
- `player_name`
- `batting_order`
- `position_code`
- `is_starting_lineup`
- `is_substitute`
- `bat_side`
- `pitch_hand`
- `snapshot_source`
- `snapshot_ts` if available
- `confirmed_flag`

This enables both historical research and future lineup-aware production.

### D. `team_roster_snapshot`

Purpose: active roster by team/date.

Suggested columns:

- `team_id`
- `as_of_date`
- `player_id`
- `player_name`
- `primary_position`
- `roster_status`
- `bat_side`
- `pitch_hand`
- `is_pitcher`
- `is_hitter`

This supports roster-based lineup/bullpen pools at inference time.

### E. `venue_dim`

Purpose: stable venue metadata.

Suggested columns:

- `venue_id`
- `venue_name`
- `latitude`
- `longitude`
- `timezone`
- `roof_type`
- `weather_exposure_default`
- `park_factor_run` (optional later)
- `park_factor_hr` (optional later)
- `altitude_m` (optional)

### F. `game_weather_snapshot`

Purpose: weather aligned to pregame time or forecast cutoff.

Suggested columns:

- `game_id`
- `source` (`open_meteo_forecast`, `open_meteo_archive`, `statsapi_feed_weather_text`)
- `snapshot_type` (`forecast`, `historical_observed`, `feed_text`)
- `as_of_ts`
- `game_local_start_ts`
- `temperature_c`
- `humidity_pct`
- `precipitation_mm`
- `wind_speed_kph`
- `wind_direction_deg`
- `wind_gusts_kph`
- `pressure_hpa`
- `is_day`
- `weather_code`
- optional raw text fields from Stats API

## Materialized pregame feature families to add to `feature_rows`

### Bullpen feature family

Recommended first-pass features:

- `home_bullpen_ip_last3`
- `away_bullpen_ip_last3`
- `bullpen_ip_last3_delta`
- `home_bullpen_pitches_last3`
- `away_bullpen_pitches_last3`
- `home_bullpen_used_yesterday_count`
- `away_bullpen_used_yesterday_count`
- `home_bullpen_b2b_count`
- `away_bullpen_b2b_count`
- `home_high_leverage_rp_used_yesterday`
- `away_high_leverage_rp_used_yesterday`
- `home_bullpen_season_era_prior`
- `away_bullpen_season_era_prior`
- `home_bullpen_whip_prior`
- `away_bullpen_whip_prior`
- `home_bullpen_k_minus_bb_rate_prior`
- `away_bullpen_k_minus_bb_rate_prior`

Strong opinion: start with **team-level bullpen aggregates**, not individual reliever embeddings or role graphs.

### Lineup / handedness / platoon feature family

Recommended first-pass features:

- `home_starter_pitch_hand`
- `away_starter_pitch_hand`
- `home_team_ops_vs_rhp_prior`
- `home_team_ops_vs_lhp_prior`
- `away_team_ops_vs_rhp_prior`
- `away_team_ops_vs_lhp_prior`
- chosen matchup feature like `home_expected_offense_vs_opp_starter_hand`
- same for away
- `home_active_roster_lhb_count`
- `away_active_roster_lhb_count`
- `home_active_roster_shb_count`
- `away_active_roster_shb_count`

If confirmed lineups become operationally supported:

- `home_confirmed_lineup_avg_ops_vs_hand_prior`
- `away_confirmed_lineup_avg_ops_vs_hand_prior`
- `home_confirmed_lineup_lhb_count`
- `away_confirmed_lineup_lhb_count`
- `home_confirmed_lineup_missing_core_count`
- `away_confirmed_lineup_missing_core_count`

### Weather / park feature family

Recommended first-pass features:

- `venue_id`
- `day_night_flag`
- `roof_type`
- `weather_exposure_flag`
- `temperature_c`
- `humidity_pct`
- `wind_speed_kph`
- `wind_direction_deg`
- `wind_gusts_kph`
- `precipitation_mm`
- `pressure_hpa`
- optional coarse bucketed features:
  - `hot_weather_flag`
  - `high_wind_flag`
  - `precip_risk_flag`

For winner prediction, bucketed/coarse variants may outperform overfit continuous weather terms unless sample size is large.

---

## 7) Explicit recommendations by requested family

## Bullpen feature family: recommendation

### Recommendation

**Prioritize first.** Build a bullpen state layer immediately.

### What to build now

1. Raw `game_pitcher_appearances`
2. Daily or per-game derived `team_bullpen_daily`
3. Add compact bullpen usage + quality features to `feature_rows`

### Best feature philosophy

Prefer robust proxies over fancy role modeling:

- recent bullpen workload
- number of taxed relievers
- recent use of best relievers
- season-to-date bullpen quality from prior games

### What not to do yet

- do not try to infer exact manager leverage roles with a complex classifier
- do not wait for a perfect “bullpen roster” definition
- do not use target-game reliever outcomes or same-game boxscore info

### Why this should be first

This is the biggest missing baseball signal with the best value-to-feasibility ratio.

## Lineup / handedness / platoon feature family: recommendation

### Recommendation

**Prioritize second, but split it into two phases.**

### Phase 1: build now

- starter handedness
- team offense vs starter hand from prior games
- active-roster handedness composition

### Phase 2: only after inference workflow is defined

- confirmed lineup strength
- confirmed lineup handedness mix
- core-bat-missing features

### Why

Handedness/platoon is real and actionable, but full lineup features are only safe if we know whether production predictions happen:

- before lineups
- after lineups
- or both

Without that, training on actual lineups risks teaching the model to use information we will not reliably have at prediction time.

## Weather / park feature family: recommendation

### Recommendation

**Prioritize third, but implement early because it is operationally easy.**

### What to build now

- `venue_dim`
- stadium coordinates + roof type
- Open-Meteo hourly historical + forecast snapshots
- coarse weather feature block in `feature_rows`

### Modeling stance

Treat weather/park as a contextual adjustment layer, not the backbone of the model.

### Why

- easy to build
- low leakage risk if forecast/observation semantics are handled correctly
- likely helps calibration and volatility understanding
- complements totals work later too

---

## 8) Leakage / inference-time realism risks

This is the most important operational section.

### A. Historical target-game lineup leakage

If you train on the **actual target-game lineup** but predict in production before lineups are confirmed, that is leakage in practical terms even if not classical label leakage.

Safe rule:

- only use target-game lineup features in production if the model runs after lineup publication, and training rows simulate that same cutoff.

### B. Historical observed weather vs forecast weather mismatch

If production uses same-day forecast weather, training should ideally use forecast snapshots from a comparable lead time, not perfectly observed final weather.

If forecast-history is not available yet, acceptable fallback:

- train on hourly observed weather as a proxy
- document that this is slightly optimistic
- keep weather weight modest until validated

### C. Team split endpoint leakage

Retrospective calls like `teams/{id}/stats?...season=2024` can easily return full-season aggregates.

Do **not** use those directly for historical training unless you snapshot them during season or rebuild them locally from prior games.

### D. Bullpen leakage via target-game boxscore

Bullpen features must be computed from **prior completed games only**. Never use target-game pitcher usage to characterize pregame bullpen state.

### E. Probable starter uncertainty

Production predictions sometimes happen before probable starters are known or before they are reliable. The dataset contract needs explicit starter-availability flags and null-safe fallbacks.

### F. Roof status ambiguity

Observed outdoor weather may be noisy for retractable-roof parks when roof-open status is unknown. Handle with explicit retractable-roof flags and avoid pretending the effect is precise.

---

## 9) What should be prioritized first?

## Priority order

### Priority 1 — Bullpen raw layer + features

Why:

- biggest missing baseball-specific signal
- historically reconstructable now
- inference-time realistic now
- high expected value

### Priority 2 — Starter handedness + team-vs-hand split features

Why:

- strong matchup signal
- operationally simple
- pairs naturally with current starter context

### Priority 3 — Venue/weather layer

Why:

- easy engineering lift
- clean schema addition
- useful contextual signal

### Priority 4 — Confirmed lineup features

Why:

- likely useful
- but only after cutoff policy / production refresh strategy is defined

### Priority 5 — Retrosheet or deeper park-factor enrichment

Why:

- worthwhile later
- not required for the first meaningful next-gen dataset jump

---

## 10) Recommended immediate dataset-expansion step

**Immediate step:** add a canonical `game_pitcher_appearances` raw table and derive a first-pass `team_bullpen_daily` / per-game bullpen state feature block from prior completed games only.

If I had to choose exactly one next action, it would be:

> Build the bullpen raw/support layer first, because it unlocks the highest-value pregame feature family without requiring fragile external dependencies or ambiguous inference-time assumptions.

A practical phase-1 sequence:

1. Add `game_pitcher_appearances`
2. Backfill it from historical boxscores for all completed games
3. Materialize bullpen usage/fatigue/quality features into `feature_rows`
4. Add starter handedness + team-vs-handedness split features
5. Add `venue_dim` + `game_weather_snapshot` and a coarse weather block
6. Decide whether there will be a lineup-refresh inference mode before building confirmed-lineup production features

---

## Final answers to the required questions

### 1. What additional data is actually available historically?

Historically available with realistic effort:

- bullpen appearances / usage / quality proxies from boxscores/live feeds
- probable starter identity and handedness
- hitter handedness
- actual batting orders / starting lineups for played games
- team-vs-handedness split style features if rebuilt locally from prior games
- venue ids / day-night context
- historical hourly weather from Open-Meteo

### 2. What additional data is available at daily inference time?

Available same day:

- schedule, venue, day/night, probable starters
- active rosters
- locally derived bullpen state from prior completed games
- starter-handedness matchup context
- same-day forecast weather
- confirmed lineups only if running close enough to first pitch

### 3. Which data families are highest-value for MLB winner prediction?

In order:

1. bullpen state/quality/fatigue
2. starter-handedness matchup + team-vs-hand offense
3. park/weather context
4. confirmed lineup quality/handedness
5. deeper park-factor / event-level enrichments

### 4. How should we represent them in the dataset/schema?

Use raw support tables for appearances, lineups, rosters, weather, and venue metadata; then materialize compact pregame features into `feature_rows`.

### 5. Which should be prioritized first?

**Bullpen first**, then starter-handedness/platoon, then weather/park, then confirmed lineups once inference-time policy is explicit.

---

## Short opinionated close

The right path here is not to chase exotic models. The right path is to make the pregame state smarter.

If we add:

- bullpen fatigue + quality
- starter hand / team-vs-hand context
- venue/weather context

we will be much closer to an actually baseball-aware model instead of another thin tabular baseline pretending team record and starter ERA are enough.
