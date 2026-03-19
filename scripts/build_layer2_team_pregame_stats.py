#!/usr/bin/env python3
"""Build the Layer 2 team_pregame_stats table."""

from __future__ import annotations

import sqlite3
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "mlb_history.db"
TABLE_NAME = "team_pregame_stats"
SEASONS = tuple(range(2020, 2026))
INSERT_SQL = f"""
    INSERT OR REPLACE INTO {TABLE_NAME} (
      game_id,
      team_id,
      side,
      season,
      season_games,
      season_wins,
      season_win_pct,
      season_run_diff_per_game,
      season_runs_scored_per_game,
      season_runs_allowed_per_game,
      season_batting_avg,
      season_obp,
      season_slg,
      season_ops,
      season_strikeouts_per_game,
      season_walks_per_game,
      rolling_last10_win_pct,
      rolling_last10_runs_scored_per_game,
      rolling_last10_runs_allowed_per_game,
      rolling_last10_ops,
      rolling_last10_obp,
      rolling_last10_batting_avg,
      days_rest,
      doubleheader_flag,
      computed_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


@dataclass(slots=True)
class GameResult:
    game_date: str
    win: int
    runs_scored: int
    runs_allowed: int
    batting_avg: float | None
    obp: float | None
    slg: float | None
    ops: float | None
    strikeouts: int | None
    walks: int | None


@dataclass(slots=True)
class TeamSeasonState:
    games: int = 0
    wins: int = 0
    runs_scored: int = 0
    runs_allowed: int = 0
    sum_batting_avg: float = 0.0
    sum_obp: float = 0.0
    sum_slg: float = 0.0
    sum_ops: float = 0.0
    sum_strikeouts: float = 0.0
    sum_walks: float = 0.0
    last10: deque[GameResult] = field(default_factory=lambda: deque(maxlen=10))

    def apply_result(self, result: GameResult) -> None:
        self.games += 1
        self.wins += result.win
        self.runs_scored += result.runs_scored
        self.runs_allowed += result.runs_allowed
        self.sum_batting_avg += result.batting_avg or 0.0
        self.sum_obp += result.obp or 0.0
        self.sum_slg += result.slg or 0.0
        self.sum_ops += result.ops or 0.0
        self.sum_strikeouts += float(result.strikeouts or 0)
        self.sum_walks += float(result.walks or 0)
        self.last10.append(result)


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=60.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 60000")
    return conn


def create_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
          game_id INTEGER NOT NULL,
          team_id INTEGER NOT NULL,
          side TEXT NOT NULL CHECK(side IN ('home', 'away')),
          season INTEGER NOT NULL,
          season_games INTEGER,
          season_wins INTEGER,
          season_win_pct REAL,
          season_run_diff_per_game REAL,
          season_runs_scored_per_game REAL,
          season_runs_allowed_per_game REAL,
          season_batting_avg REAL,
          season_obp REAL,
          season_slg REAL,
          season_ops REAL,
          season_strikeouts_per_game REAL,
          season_walks_per_game REAL,
          rolling_last10_win_pct REAL,
          rolling_last10_runs_scored_per_game REAL,
          rolling_last10_runs_allowed_per_game REAL,
          rolling_last10_ops REAL,
          rolling_last10_obp REAL,
          rolling_last10_batting_avg REAL,
          days_rest INTEGER,
          doubleheader_flag INTEGER DEFAULT 0,
          computed_at TEXT,
          PRIMARY KEY (game_id, team_id),
          FOREIGN KEY (game_id) REFERENCES games(game_id) ON DELETE CASCADE
        )
        """
    )
    conn.commit()


def fetch_games(conn: sqlite3.Connection, season: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT game_id, season, game_date, scheduled_datetime, status, home_team_id, away_team_id
        FROM games
        WHERE season = ?
        ORDER BY game_date, COALESCE(scheduled_datetime, ''), game_id
        """,
        (season,),
    ).fetchall()


def fetch_final_results(conn: sqlite3.Connection, season: int) -> dict[tuple[int, str], GameResult]:
    rows = conn.execute(
        """
        SELECT
          g.game_id,
          g.game_date,
          gts.team_id,
          gts.side,
          gts.runs,
          gts.batting_avg,
          gts.obp,
          gts.slg,
          gts.ops,
          gts.strikeouts,
          gts.walks,
          l.did_home_win,
          l.home_score,
          l.away_score
        FROM games g
        INNER JOIN labels l
          ON l.game_id = g.game_id
        INNER JOIN game_team_stats gts
          ON gts.game_id = g.game_id
        WHERE g.season = ?
          AND g.status = 'Final'
        """,
        (season,),
    ).fetchall()

    results: dict[tuple[int, str], GameResult] = {}
    for row in rows:
        side = str(row["side"])
        did_home_win = int(row["did_home_win"])
        win = 1 if (side == "home" and did_home_win == 1) or (side == "away" and did_home_win == 0) else 0
        if side == "home":
            runs_allowed = int(row["away_score"] or 0)
        else:
            runs_allowed = int(row["home_score"] or 0)
        results[(int(row["game_id"]), side)] = GameResult(
            game_date=str(row["game_date"]),
            win=win,
            runs_scored=int(row["runs"] or 0),
            runs_allowed=runs_allowed,
            batting_avg=to_float(row["batting_avg"]),
            obp=to_float(row["obp"]),
            slg=to_float(row["slg"]),
            ops=to_float(row["ops"]),
            strikeouts=to_int(row["strikeouts"]),
            walks=to_int(row["walks"]),
        )
    return results


def to_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def mean_or_none(total: float, count: int) -> float | None:
    if count <= 0:
        return None
    return total / count


def rolling_mean(results: deque[GameResult], field_name: str) -> float | None:
    count = len(results)
    if count == 0:
        return None
    total = 0.0
    for result in results:
        value = getattr(result, field_name)
        total += float(value or 0.0)
    return total / count


def build_row(
    *,
    game: sqlite3.Row,
    side: str,
    team_id: int,
    state: TeamSeasonState | None,
    last_completed_date: str | None,
    computed_at: str,
) -> tuple[Any, ...]:
    game_date = str(game["game_date"])
    season = int(game["season"])
    season_games = state.games if state and state.games > 0 else None
    season_wins = state.wins if state and state.games > 0 else None

    if state and state.games > 0:
        season_win_pct = state.wins / state.games
        season_run_diff_per_game = (state.runs_scored - state.runs_allowed) / state.games
        season_runs_scored_per_game = state.runs_scored / state.games
        season_runs_allowed_per_game = state.runs_allowed / state.games
        season_batting_avg = mean_or_none(state.sum_batting_avg, state.games)
        season_obp = mean_or_none(state.sum_obp, state.games)
        season_slg = mean_or_none(state.sum_slg, state.games)
        season_ops = mean_or_none(state.sum_ops, state.games)
        season_strikeouts_per_game = mean_or_none(state.sum_strikeouts, state.games)
        season_walks_per_game = mean_or_none(state.sum_walks, state.games)
        rolling_last10_win_pct = rolling_mean(state.last10, "win")
        rolling_last10_runs_scored_per_game = rolling_mean(state.last10, "runs_scored")
        rolling_last10_runs_allowed_per_game = rolling_mean(state.last10, "runs_allowed")
        rolling_last10_ops = rolling_mean(state.last10, "ops")
        rolling_last10_obp = rolling_mean(state.last10, "obp")
        rolling_last10_batting_avg = rolling_mean(state.last10, "batting_avg")
    else:
        season_win_pct = None
        season_run_diff_per_game = None
        season_runs_scored_per_game = None
        season_runs_allowed_per_game = None
        season_batting_avg = None
        season_obp = None
        season_slg = None
        season_ops = None
        season_strikeouts_per_game = None
        season_walks_per_game = None
        rolling_last10_win_pct = None
        rolling_last10_runs_scored_per_game = None
        rolling_last10_runs_allowed_per_game = None
        rolling_last10_ops = None
        rolling_last10_obp = None
        rolling_last10_batting_avg = None

    days_rest = None
    doubleheader_flag = 0
    if last_completed_date is not None:
        days_rest = (datetime.fromisoformat(game_date) - datetime.fromisoformat(last_completed_date)).days
        doubleheader_flag = 1 if days_rest == 0 else 0

    return (
        int(game["game_id"]),
        team_id,
        side,
        season,
        season_games,
        season_wins,
        season_win_pct,
        season_run_diff_per_game,
        season_runs_scored_per_game,
        season_runs_allowed_per_game,
        season_batting_avg,
        season_obp,
        season_slg,
        season_ops,
        season_strikeouts_per_game,
        season_walks_per_game,
        rolling_last10_win_pct,
        rolling_last10_runs_scored_per_game,
        rolling_last10_runs_allowed_per_game,
        rolling_last10_ops,
        rolling_last10_obp,
        rolling_last10_batting_avg,
        days_rest,
        doubleheader_flag,
        computed_at,
    )


def process_season(
    conn: sqlite3.Connection,
    season: int,
    computed_at: str,
) -> int:
    games = fetch_games(conn, season)
    final_results = fetch_final_results(conn, season)
    season_states: dict[int, TeamSeasonState] = {}
    last_completed_date_by_team: dict[int, str] = {}
    rows_to_upsert: list[tuple[Any, ...]] = []

    index = 0
    while index < len(games):
        current_date = str(games[index]["game_date"])
        day_games: list[sqlite3.Row] = []
        while index < len(games) and str(games[index]["game_date"]) == current_date:
            day_games.append(games[index])
            index += 1

        pending_updates: list[tuple[int, GameResult]] = []
        for game in day_games:
            for side, team_key in (("home", "home_team_id"), ("away", "away_team_id")):
                team_id = to_int(game[team_key])
                if team_id is None:
                    continue
                rows_to_upsert.append(
                    build_row(
                        game=game,
                        side=side,
                        team_id=team_id,
                        state=season_states.get(team_id),
                        last_completed_date=last_completed_date_by_team.get(team_id),
                        computed_at=computed_at,
                    )
                )

            if str(game["status"]) != "Final":
                continue

            game_id = int(game["game_id"])
            for side, team_key in (("home", "home_team_id"), ("away", "away_team_id")):
                team_id = to_int(game[team_key])
                result = final_results.get((game_id, side))
                if team_id is None or result is None:
                    continue
                pending_updates.append((team_id, result))
                last_completed_date_by_team[team_id] = result.game_date

        for team_id, result in pending_updates:
            season_states.setdefault(team_id, TeamSeasonState()).apply_result(result)

    conn.executemany(INSERT_SQL, rows_to_upsert)
    conn.commit()
    return len(rows_to_upsert)


def print_verification(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        f"""
        SELECT *
        FROM {TABLE_NAME}
        WHERE game_id = 661199
          AND team_id = 119
        """
    ).fetchone()
    print("Verification row for game_id=661199, team_id=119:")
    if row is None:
        print("  <missing>")
        return
    for key in row.keys():
        print(f"  {key}: {row[key]}")


def build_table() -> int:
    computed_at = datetime.now(timezone.utc).isoformat()
    with connect_db() as conn:
        create_table(conn)
        total_rows = 0
        for season in SEASONS:
            print(f"Processing season {season}...")
            rows = process_season(conn, season, computed_at)
            total_rows += rows
            print(f"Season {season}: upserted {rows} rows.")
        total_count = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
        print(f"Finished {TABLE_NAME}: processed {total_rows} season rows, table count={total_count}.")
        print_verification(conn)
    return 0


if __name__ == "__main__":
    raise SystemExit(build_table())
