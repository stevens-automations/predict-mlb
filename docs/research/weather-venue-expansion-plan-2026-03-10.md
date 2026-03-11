# Predict-MLB weather / venue expansion plan

_Date: 2026-03-10_

## Purpose

This memo defines the next implementation-ready data expansion for **weather + venue / stadium support**.

Context for this plan:

- Bullpen raw/support tables are already materially planned and present in the current schema direction.
- Retraining is intentionally deferred until the agreed new data families are in place and validated.
- Weather / venue should be built now as the next additive support layer, in parallel with lineup / platoon planning.
- **Train / inference parity is mandatory.**

The goal here is not to design final model features in detail. The goal is to define the raw tables, support tables, ingestion path, and parity rules so implementation can proceed with minimal schema churn.

---

## Recommended design in one sentence

Add a small, explicit **venue dimension** plus a **pregame weather snapshot table**, keep any derived modeling logic outside the raw layer, and make live inference use the same weather class and timing policy as training.

---

## Current repo context that matters

The current schema already includes:

- `games`
- `game_team_stats`
- `game_pitcher_context`
- `game_pitcher_appearances`
- `team_bullpen_game_state`
- `team_bullpen_top_relievers`
- `feature_rows`
- `labels`

Important implication:

- This should be an **additive phase**.
- Do **not** redesign `feature_rows`.
- Do **not** overload `games` with lots of weather columns.
- Keep venue identity stable in one place and keep weather snapshot-time data in its own table.

---

## 1) Exact new raw / support tables needed

## A. New support dimension: `venue_dim`

Purpose:
Stable stadium / venue identity and metadata used for weather lookups and later park-context joins.

Recommended columns:

- `venue_id INTEGER PRIMARY KEY`
- `venue_name TEXT NOT NULL`
- `city TEXT`
- `state TEXT`
- `country TEXT DEFAULT 'USA'`
- `timezone TEXT NOT NULL`
- `latitude REAL NOT NULL`
- `longitude REAL NOT NULL`
- `roof_type TEXT NOT NULL`  
  Allowed practical values: `open`, `retractable`, `fixed_dome`, `unknown`
- `weather_exposure_default INTEGER NOT NULL DEFAULT 1 CHECK(weather_exposure_default IN (0,1))`
- `statsapi_venue_name TEXT`
- `source_updated_at TEXT`
- `ingested_at TEXT NOT NULL DEFAULT (datetime('now'))`

Recommended notes:

- `weather_exposure_default = 0` for fixed domes.
- `weather_exposure_default = 1` for open-air parks.
- For retractable roofs, keep `weather_exposure_default = 1` in first pass and accept some noise.
- Keep park-factor columns out of the first pass unless they are already locally maintained. If desired later, add nullable `park_factor_run` / `park_factor_hr` fields without changing the weather design.

Why this table should exist:

- Venue identity is stable and reusable.
- Weather joins should use coordinates from a local canonical table, not ad hoc string matching.
- Future park-factor or park-physics work can reuse the same key.

## B. New raw snapshot table: `game_weather_snapshots`

Purpose:
Store the weather state aligned to the model prediction cutoff for a specific game.

Recommended columns:

- `game_id INTEGER NOT NULL`
- `venue_id INTEGER NOT NULL`
- `as_of_ts TEXT NOT NULL`
- `target_game_ts TEXT NOT NULL`
- `snapshot_type TEXT NOT NULL`  
  Allowed first-pass values: `forecast`, `observed_archive`, `statsapi_text`
- `source TEXT NOT NULL`  
  Suggested values: `open_meteo_forecast`, `open_meteo_archive`, `statsapi_feed`
- `source_priority INTEGER NOT NULL DEFAULT 1`
- `hour_offset_from_first_pitch REAL`
- `temperature_f REAL`
- `humidity_pct REAL`
- `pressure_hpa REAL`
- `precipitation_mm REAL`
- `precipitation_probability REAL`
- `wind_speed_mph REAL`
- `wind_gust_mph REAL`
- `wind_direction_deg REAL`
- `weather_code INTEGER`
- `cloud_cover_pct REAL`
- `is_day INTEGER CHECK(is_day IN (0,1))`
- `day_night_source TEXT`
- `weather_exposure_flag INTEGER CHECK(weather_exposure_flag IN (0,1))`
- `statsapi_weather_condition_text TEXT`
- `statsapi_wind_text TEXT`
- `source_updated_at TEXT`
- `ingested_at TEXT NOT NULL DEFAULT (datetime('now'))`

Recommended key:

- `PRIMARY KEY (game_id, as_of_ts, snapshot_type, source)`

Recommended relationships:

- `FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE`
- `FOREIGN KEY (venue_id) REFERENCES venue_dim(venue_id)`

Why this table should exist:

- Weather is a snapshot, not a permanent game attribute.
- The same game may have multiple forecast refreshes before first pitch.
- The table cleanly distinguishes forecast-time rows from observed historical rows.
- It preserves parity auditing later because `snapshot_type` and `source` are explicit.

## C. Optional but recommended small support table: `game_venue_context`

Recommendation:
**Do not make this mandatory in first pass.**

If the team wants a compact derived row later, this table can be added after the raw pipeline is working.

Possible columns later:

- `game_id`
- `venue_id`
- `day_night_flag`
- `roof_type`
- `weather_exposure_flag`
- `local_start_time`
- `weather_snapshot_selected_source`
- `weather_snapshot_selected_as_of_ts`

Why optional:

- The feature materializer can join `games -> venue_dim -> game_weather_snapshots` directly.
- First pass should keep the schema as small as possible.

## D. Minimal extension to an existing table: `games`

Recommended tiny extension:

- add `venue_id INTEGER`
- optionally add `day_night TEXT`

This is the one existing-table change I do recommend.

Why:

- `venue_id` belongs to the schedule/game identity layer.
- Storing it directly in `games` avoids awkward joins and repeated schedule parsing.
- `day_night` is also a schedule/feed attribute and is cheap to keep there.

If the team wants zero changes to existing tables, weather can still work by deriving venue from raw schedule payload each time. But that is worse operationally and not worth the purity.

---

## 2) Source / API choice for historical weather and inference-time weather

## Primary recommendation: Open-Meteo for both historical and live weather

### Historical weather source

Use:

- **Open-Meteo Archive API** as the canonical historical weather source

Why:

- Same provider family as live forecast path
- Structured hourly fields
- Easy timezone alignment
- No need to parse messy free-text wind strings
- Better long-run schema stability than scraping or relying on feed text

### Inference-time weather source

Use:

- **Open-Meteo Forecast API** as the canonical live inference-time weather source

Why:

- Same field family as archive
- Easy to query for the expected first-pitch hour
- Cleanest path to train/inference parity

### Secondary / fallback source

Use:

- **MLB Stats API weather text** only as a secondary sanity / audit field

Specifically:

- keep `statsapi_weather_condition_text`
- keep `statsapi_wind_text`
- do not make Stats API weather text the canonical numeric source

Why not use Stats API as primary weather source:

- wind is stringy and inconsistent
- coverage/timing is less clean for a full historical training pipeline
- it is better as an audit breadcrumb than the main weather feed

## Parity policy

Canonical production rule:

- **Training should ultimately use forecast-like weather snapshots, not final observed weather, whenever feasible.**

Practical first-pass compromise:

- historical backfill may start from `observed_archive` rows because they are easiest to reconstruct reliably
- inference will use `forecast` rows
- this mismatch must be treated as an explicit temporary risk, not silently ignored

---

## 3) How venue / stadium identity should be stored

Recommended approach:

1. `games.venue_id` is the game-level foreign key
2. `venue_dim` is the stable source of truth for venue metadata
3. `game_weather_snapshots.venue_id` is stored redundantly for join convenience and auditability

Do **not** use only venue name strings as the durable key.

Why:

- name changes happen
- formatting differs across sources
- later park-factor and geometry joins should hang off a stable integer key

Recommended venue identity rules:

- one row per MLB venue in `venue_dim`
- if a stadium is renamed, keep the same `venue_id` and update `venue_name`
- if a team temporarily relocates, that should still resolve to the actual physical venue row
- coordinates and timezone live only in `venue_dim`

---

## 4) Which weather fields belong in first pass

Keep the first pass practical and compact.

## First-pass numeric fields

Recommended first-pass modeled weather fields:

- `temperature_f`
- `wind_speed_mph`
- `wind_direction_deg`
- `precipitation_mm`
- `precipitation_probability`
- `humidity_pct`
- `pressure_hpa`
- `wind_gust_mph`
- `cloud_cover_pct`

## First-pass contextual fields

Recommended first-pass context fields:

- `venue_id`
- `day_night`
- `roof_type`
- `weather_exposure_flag`
- `is_day`

## Fields to defer unless easy

Defer for now:

- park geometry / wind-out-to-center transformations
- air density composite score
- handedness-sensitive park factors
- manually engineered weather run-environment score
- exact roof-open status for retractable roofs

Why defer:

- they are useful later, but not required to get the weather layer operational
- first pass should prioritize reliable storage and parity, not fancy transformations

---

## 5) How day / night and related context should be handled

Recommendation:

Use a **two-source approach** with one canonical modeled field.

### Canonical modeled field

- `day_night` at the game level

### Where it should come from

Priority order:

1. `games.day_night` from schedule/feed if available
2. fallback from weather row `is_day`
3. if both missing, derive from local scheduled time only as a last resort

### Why this approach is best

- MLB schedule feeds usually already classify day/night
- weather APIs can also expose day/night at the hourly level
- storing both source paths lets the project debug discrepancies later

### Related context to store now

Also carry these as support context, even if not immediate model features:

- `target_game_ts`
- `hour_offset_from_first_pitch`
- `timezone`
- `weather_exposure_flag`
- `roof_type`

These matter because weather effects are contextual, and they are useful for later auditing.

---

## 6) Historical backfill path

Recommended historical path:

## Step 1: add / populate venue dimension

- Build `venue_dim` once from Stats API venue IDs already attached to historical schedules
- Manually patch coordinates, timezone, and roof type where needed
- This is low-volume and should be treated as a durable dimension table

## Step 2: extend historical game spine with venue id

- Backfill `games.venue_id`
- Backfill `games.day_night` where available

## Step 3: weather snapshot backfill for completed games

For each historical game:

1. read `game_id`, `venue_id`, `scheduled_datetime`
2. resolve venue coordinates and local timezone from `venue_dim`
3. choose the canonical target hour nearest first pitch
4. query Open-Meteo Archive for that local-hour weather
5. write one `observed_archive` row into `game_weather_snapshots`
6. optionally also store Stats API free-text weather/wind if present

## Step 4: define historical training snapshot policy

First-pass acceptable policy:

- use one weather row per game aligned to the first-pitch hour
- clearly label it `snapshot_type = observed_archive`

Better later policy:

- if the project later captures historical forecast snapshots or can simulate a consistent pregame forecast lead, add those as separate rows and promote them for model training

## Step 5: validation / DQ checks

Minimum checks:

- weather row coverage by season
- missing venue coordinates count = 0
- dome parks have `weather_exposure_flag = 0`
- non-dome parks mostly have non-null wind / temperature
- weather timestamp is near scheduled first pitch

---

## 7) Daily inference-time update path

Recommended live path:

## Morning schedule refresh

1. refresh today’s schedule into `games`
2. populate `venue_id` and `day_night`
3. ensure every scheduled venue exists in `venue_dim`

## Pregame weather refresh at model cutoff

At the same prediction cutoff used for lineup/starter assembly:

1. read each scheduled game’s `venue_id` and `scheduled_datetime`
2. resolve coordinates from `venue_dim`
3. query Open-Meteo Forecast for the forecast hour nearest target first pitch
4. write a `forecast` row into `game_weather_snapshots`
5. derive `weather_exposure_flag` from venue roof metadata
6. keep any Stats API weather text only as optional extra fields

## Feature materialization rule

When building the feature row for inference:

- select the latest eligible `forecast` weather snapshot at or before the configured as-of cutoff
- do not use a weather snapshot newer than the feature row `as_of_ts`
- if forecast is missing, either:
  - fall back to null weather features and degrade the row, or
  - use a documented lower-priority source only if that same fallback is allowed historically

Recommendation:

- prefer **degraded/null** over mixing incompatible live fallbacks silently

---

## 8) Forecast-vs-observed parity risks

This is the biggest weather-specific modeling risk.

## Core problem

If training uses `observed_archive` weather but production uses `forecast`, the model is trained on cleaner information than it will have live.

That can create:

- optimistic offline evaluation
- overweighting of weather fields
- instability when live forecasts miss wind or rain timing

## Specific risks

### A. Observed weather is too perfect

Observed archive weather reflects what actually happened at game time.
Live predictions only know what was forecast before first pitch.

### B. Retractable-roof ambiguity

Outside weather may not matter much if the roof was closed.
Without same-day roof-open state, retractable parks add noise.

### C. Timestamp mismatch

Using the game-start hour in history but a much earlier forecast hour live can create hidden as-of mismatch.

### D. Overfitting to weak signal

Weather probably helps, but it is not the backbone of an MLB side model. If parity is imperfect, weather should stay a modest feature block until proven.

## Mitigations

Recommended mitigations:

1. label every row with `snapshot_type` and `source`
2. keep first-pass weather feature set modest
3. include `weather_exposure_flag` and `roof_type`
4. explicitly monitor model uplift with and without weather
5. if historical forecast-like snapshots are later available, prefer them over observed archive for final training datasets
6. do not create complex weather interaction features until the raw parity path is proven

---

## 9) Recommended implementation order

This is the implementation order I recommend for the weather / venue track itself.

### Order 1 — add stable venue support

1. add `venue_dim`
2. add `games.venue_id`
3. optionally add `games.day_night`

Reason:

- every later weather step depends on a stable venue key and coordinates

### Order 2 — add raw weather snapshot layer

4. add `game_weather_snapshots`
5. wire historical archive ingestion for completed games
6. wire live forecast ingestion for scheduled games

Reason:

- this gets the parity-critical raw layer in place first

### Order 3 — validate coverage and timestamp realism

7. validate per-season weather coverage
8. validate dome / retractable / open-air exposure logic
9. validate snapshot selection rules around first pitch

Reason:

- better to prove the support layer before pushing it into modeling

### Order 4 — only then add model-facing weather features

10. materialize a compact weather / venue feature block into a new feature version
11. keep it coarse at first
12. compare uplift vs no-weather baseline before expanding feature breadth

Reason:

- minimizes churn
- prevents building too many downstream features on a shaky support layer

---

## Clear separation: raw tables vs support tables vs later model features

## Raw / snapshot layer

- `game_weather_snapshots`

## Support / dimension layer

- `venue_dim`
- `games.venue_id`
- `games.day_night` (small extension)

## Later model feature layer

Examples of later feature payload fields only after the raw/support layer is validated:

- `home_temperature_f`
- `home_wind_speed_mph`
- `home_precip_probability`
- `weather_exposure_flag`
- `roof_type`
- `day_night`
- coarse weather buckets such as hot/cold, windy/calm, wet/dry

Do not collapse raw weather storage and model features into one step.

---

## Final recommendation

The cleanest practical plan is:

- store stadium identity in a local `venue_dim`
- put `venue_id` on `games`
- store prediction-time weather in `game_weather_snapshots`
- use Open-Meteo archive for historical weather and Open-Meteo forecast for live inference
- treat Stats API weather text as audit-only support
- keep the first-pass weather block modest and parity-aware

That gives the project a durable venue/weather foundation without forcing a redesign of the existing schema or feature contract.
