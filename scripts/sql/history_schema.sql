PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS games (
  game_id INTEGER PRIMARY KEY,
  season INTEGER NOT NULL,
  game_date TEXT NOT NULL,
  game_type TEXT,
  status TEXT,
  scheduled_datetime TEXT,
  home_team_id INTEGER,
  away_team_id INTEGER,
  home_score INTEGER,
  away_score INTEGER,
  winning_team_id INTEGER,
  source_updated_at TEXT,
  ingested_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS game_team_stats (
  game_id INTEGER NOT NULL,
  team_id INTEGER NOT NULL,
  side TEXT CHECK(side IN ('home', 'away')),
  runs INTEGER,
  hits INTEGER,
  errors INTEGER,
  batting_avg REAL,
  obp REAL,
  slg REAL,
  ops REAL,
  strikeouts INTEGER,
  walks INTEGER,
  source_updated_at TEXT,
  ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (game_id, team_id),
  FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS game_pitcher_context (
  game_id INTEGER NOT NULL,
  side TEXT NOT NULL CHECK(side IN ('home', 'away')),
  pitcher_id INTEGER,
  pitcher_name TEXT,
  probable_pitcher_id INTEGER,
  probable_pitcher_name TEXT,
  probable_pitcher_known INTEGER NOT NULL DEFAULT 0 CHECK(probable_pitcher_known IN (0, 1)),
  season_era REAL,
  season_whip REAL,
  season_avg_allowed REAL,
  season_runs_per_9 REAL,
  season_strike_pct REAL,
  season_win_pct REAL,
  career_era REAL,
  stats_source TEXT,
  stats_as_of_date TEXT,
  season_stats_scope TEXT,
  season_stats_leakage_risk INTEGER NOT NULL DEFAULT 1 CHECK(season_stats_leakage_risk IN (0, 1)),
  source_updated_at TEXT,
  ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (game_id, side),
  FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS feature_rows (
  game_id INTEGER NOT NULL,
  feature_version TEXT NOT NULL,
  as_of_ts TEXT NOT NULL,
  feature_payload_json TEXT NOT NULL,
  source_contract_status TEXT NOT NULL DEFAULT 'valid',
  source_contract_issues_json TEXT,
  ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (game_id, feature_version, as_of_ts),
  FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS labels (
  game_id INTEGER PRIMARY KEY,
  did_home_win INTEGER CHECK (did_home_win IN (0, 1)),
  home_score INTEGER,
  away_score INTEGER,
  run_differential INTEGER,
  total_runs INTEGER,
  label_source TEXT DEFAULT 'statsapi',
  settled_at TEXT,
  ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS ingestion_runs (
  run_id TEXT PRIMARY KEY,
  mode TEXT NOT NULL CHECK(mode IN ('init-db', 'backfill', 'incremental', 'dq')),
  status TEXT NOT NULL CHECK(status IN ('running', 'success', 'failed', 'blocked', 'stubbed')),
  partition_key TEXT,
  started_at TEXT NOT NULL,
  ended_at TEXT,
  config_json TEXT,
  request_count INTEGER NOT NULL DEFAULT 0,
  note TEXT
);

CREATE TABLE IF NOT EXISTS ingestion_checkpoints (
  job_name TEXT NOT NULL,
  partition_key TEXT NOT NULL,
  cursor_json TEXT,
  last_game_id INTEGER,
  attempts INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'running',
  updated_at TEXT NOT NULL,
  last_error TEXT,
  PRIMARY KEY (job_name, partition_key)
);

CREATE TABLE IF NOT EXISTS dq_results (
  run_id TEXT NOT NULL,
  check_name TEXT NOT NULL,
  severity TEXT NOT NULL CHECK(severity IN ('info', 'warn', 'error')),
  passed INTEGER NOT NULL CHECK(passed IN (0, 1)),
  expected_value REAL,
  observed_value REAL,
  details_json TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (run_id, check_name),
  FOREIGN KEY (run_id) REFERENCES ingestion_runs(run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS odds_snapshot (
  game_id INTEGER NOT NULL,
  snapshot_ts TEXT NOT NULL,
  bookmaker TEXT NOT NULL,
  market_type TEXT NOT NULL DEFAULT 'moneyline',
  home_price INTEGER,
  away_price INTEGER,
  implied_home_prob REAL,
  implied_away_prob REAL,
  source TEXT,
  note TEXT NOT NULL DEFAULT 'forward_only',
  ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (game_id, snapshot_ts, bookmaker, market_type),
  FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
);
