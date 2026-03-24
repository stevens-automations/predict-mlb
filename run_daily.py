#!/usr/bin/env python3
"""
run_daily.py — Daily MLB prediction pipeline scheduler.

Runs APScheduler BlockingScheduler in foreground (nohup-able).

Schedule:
  8:00 AM ET  — morning_chain: ingest → layer2 → evaluate → fetch → odds → predict
  11:00 PM ET — evening_evaluate: catch late game results

Usage:
  cd ~/.openclaw/projects/predict-mlb
  .venv/bin/python run_daily.py
  # or via start.sh
"""

from __future__ import annotations

import logging
import sqlite3
import traceback
from datetime import datetime, timedelta
from pathlib import Path

import pytz
import requests
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.schedulers.blocking import BlockingScheduler

# --- Job imports ---
from scripts.jobs.evaluate_yesterday import evaluate_yesterday, generate_weekly_recap
from scripts.jobs.fetch_odds import fetch_odds
from scripts.jobs.fetch_todays_games import fetch_todays_games
from scripts.jobs.ingest_yesterday import ingest_yesterday
from scripts.jobs.predict_today import predict_today
from scripts.jobs.update_layer2 import update_layer2

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent
DB_PATH = str(ROOT / "data" / "mlb_history.db")
ET_TZ = pytz.timezone("America/New_York")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("run_daily")

# ---------------------------------------------------------------------------
# DDL — pipeline tables (idempotent)
# ---------------------------------------------------------------------------

_ENSURE_TABLES_SQL = [
    """
    CREATE TABLE IF NOT EXISTS pipeline_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        ts          TEXT DEFAULT (datetime('now')),
        job         TEXT,
        status      TEXT,
        message     TEXT,
        duration_s  REAL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS today_schedule (
        game_id         INTEGER PRIMARY KEY,
        game_date       TEXT,
        home_team       TEXT,
        away_team       TEXT,
        home_team_id    INTEGER,
        away_team_id    INTEGER,
        first_pitch_et  TEXT,
        home_odds       TEXT,
        away_odds       TEXT,
        odds_bookmaker  TEXT,
        fetched_at      TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS daily_predictions (
        game_id             INTEGER PRIMARY KEY,
        game_date           TEXT NOT NULL,
        home_team           TEXT,
        away_team           TEXT,
        home_team_id        INTEGER,
        away_team_id        INTEGER,
        first_pitch_et      TEXT,
        predicted_winner    TEXT,
        home_win_prob       REAL,
        confidence_tier     TEXT,
        home_odds           TEXT,
        away_odds           TEXT,
        best_odds_bookmaker TEXT,
        tweet_scheduled_at  TEXT,
        tweeted             INTEGER DEFAULT 0,
        actual_winner       TEXT,
        home_score          INTEGER,
        away_score          INTEGER,
        did_predict_correct INTEGER,
        result_tweeted      INTEGER DEFAULT 0,
        created_at          TEXT DEFAULT (datetime('now')),
        updated_at          TEXT DEFAULT (datetime('now'))
    )
    """,
]


def ensure_tables() -> None:
    """Create pipeline tables if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    try:
        for sql in _ENSURE_TABLES_SQL:
            conn.execute(sql)
        conn.commit()
        logger.info("Pipeline tables verified/created.")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# pipeline_log helper (standalone, used from listener without an open conn)
# ---------------------------------------------------------------------------


def _pipeline_log(job: str, status: str, message: str) -> None:
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "INSERT INTO pipeline_log (job, status, message) VALUES (?, ?, ?)",
            (job, status, message),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        logger.warning(f"pipeline_log write failed: {exc}")


# ---------------------------------------------------------------------------
# Job functions
# ---------------------------------------------------------------------------


def _open_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def check_ollama() -> bool:
    """Check if Ollama is running and Qwen models are loaded. Logs warning if not."""
    try:
        r = requests.get("http://127.0.0.1:11434/api/tags", timeout=3)
        models = [m["name"] for m in r.json().get("models", [])]
        if "qwen3.5:9b" not in models and "qwen3.5:4b" not in models:
            logger.warning(
                "Ollama running but Qwen models not loaded — tweet generation will use deterministic fallback"
            )
            return False
        logger.info(f"Ollama OK — models: {models}")
        return True
    except Exception as e:
        logger.warning(f"Ollama not reachable: {e} — tweet generation will use deterministic fallback")
        return False


def register_tweet_jobs(scheduler: BlockingScheduler, conn: sqlite3.Connection) -> None:
    """Register one APScheduler job per tweet-eligible game, firing 1 hour before first pitch."""
    ET = pytz.timezone("America/New_York")

    rows = conn.execute(
        """
        SELECT game_id, home_team, away_team, tweet_text, first_pitch_et, tweet_scheduled_at
        FROM daily_predictions
        WHERE tweet_eligible = 1 AND tweeted = 0 AND tweet_text IS NOT NULL
          AND game_date = date('now', 'localtime')
        """
    ).fetchall()

    registered = 0
    for row in rows:
        fp_et_str = row["first_pitch_et"] if hasattr(row, "__getitem__") else row[4]
        if not fp_et_str:
            continue

        try:
            # first_pitch_et is stored as naive ET ISO string e.g. "2026-03-26T13:10:00"
            naive_dt = datetime.fromisoformat(fp_et_str)
            first_pitch_et_aware = ET.localize(naive_dt)
        except Exception as exc:
            logger.warning(f"register_tweet_jobs: could not parse first_pitch_et={fp_et_str!r}: {exc}")
            continue

        tweet_at_et = first_pitch_et_aware - timedelta(hours=1)

        # Skip if tweet time is in the past
        if tweet_at_et < datetime.now(ET):
            _pipeline_log(
                "register_tweet_jobs",
                "skipped",
                f"game {row['game_id']} tweet time already passed ({tweet_at_et.strftime('%I:%M %p ET')})",
            )
            continue

        game_id = row["game_id"]
        tweet_text = row["tweet_text"]
        home_team = row["home_team"]
        away_team = row["away_team"]

        def make_tweet_job(gid: int, text: str, db_path: str):
            def tweet_job():
                import sqlite3 as _sqlite3
                c = _sqlite3.connect(db_path)
                c.row_factory = _sqlite3.Row
                try:
                    c.execute(
                        "INSERT INTO pipeline_log (job, status, message) VALUES (?, ?, ?)",
                        (
                            f"tweet_game_{gid}",
                            "completed",
                            f"TWEET (not yet posted — no Twitter creds): {text[:100]}",
                        ),
                    )
                    c.execute(
                        "UPDATE daily_predictions SET tweeted=1, tweet_scheduled_at=? WHERE game_id=?",
                        (datetime.now().isoformat(), gid),
                    )
                    c.commit()
                finally:
                    c.close()
                print(f"\n[TWEET] {text}\n")

            return tweet_job

        scheduler.add_job(
            make_tweet_job(game_id, tweet_text, DB_PATH),
            trigger="date",
            run_date=tweet_at_et,
            id=f"tweet_{game_id}",
            replace_existing=True,
            name=f"Tweet: {away_team} @ {home_team}",
        )
        _pipeline_log(
            "register_tweet_jobs",
            "completed",
            f"Scheduled tweet for game {game_id} at {tweet_at_et.strftime('%I:%M %p ET')}",
        )
        print(f"  Tweet scheduled: {away_team} @ {home_team} → {tweet_at_et.strftime('%I:%M %p ET')}")
        registered += 1

    logger.info(f"register_tweet_jobs: {registered} tweet job(s) scheduled")


def morning_chain(scheduler: BlockingScheduler) -> None:
    """Full morning pipeline chain.

    Runs sequentially:
      ingest_yesterday → update_layer2 → evaluate_yesterday →
      fetch_todays_games → fetch_odds → predict_today → register_tweet_jobs
    """
    logger.info("=== morning_chain starting ===")
    conn = _open_conn()
    try:
        ingest_yesterday(conn)
        update_layer2(conn)
        evaluate_yesterday(conn)
        if datetime.now(ET_TZ).weekday() == 0:  # Monday
            recap = generate_weekly_recap(conn)
            _pipeline_log('weekly_recap', 'completed', recap)
            print(f'[WEEKLY RECAP] {recap}')
        fetch_todays_games(conn)
        fetch_odds(conn)
        predict_today(conn)
        register_tweet_jobs(scheduler, conn)
    finally:
        conn.close()
    logger.info("=== morning_chain complete ===")


# ---------------------------------------------------------------------------
# Scheduler setup
# ---------------------------------------------------------------------------


def _make_listener(scheduler: BlockingScheduler):
    """Return a job event listener that logs and prints next-job info."""

    def listener(event) -> None:
        job_id = event.job_id

        if event.exception:
            logger.error(f"Job '{job_id}' FAILED: {event.exception}")
            traceback.print_tb(event.traceback)
            _pipeline_log(job_id, "failed", str(event.exception))
        else:
            logger.info(f"Job '{job_id}' executed successfully")
            _pipeline_log(job_id, "executed", "completed successfully")

        # Print upcoming jobs
        jobs = scheduler.get_jobs()
        upcoming = [
            (j.name, getattr(j, "next_run_time", None))
            for j in jobs
            if getattr(j, "next_run_time", None) is not None
        ]
        upcoming.sort(key=lambda x: x[1])
        if upcoming:
            name, nxt = upcoming[0]
            print(f"  Next job: {name} at {nxt.strftime('%Y-%m-%d %H:%M %Z')}")

    return listener


def _print_banner(scheduler: BlockingScheduler) -> None:
    now_et = datetime.now(ET_TZ)
    jobs = scheduler.get_jobs()

    print("=" * 60)
    print("  predict-mlb Daily Pipeline Scheduler")
    print(f"  Started : {now_et.strftime('%Y-%m-%d %H:%M %Z')}")
    print(f"  DB      : {DB_PATH}")
    print("  Scheduled jobs:")
    for job in jobs:
        next_run = getattr(job, "next_run_time", None)
        next_str = next_run.strftime("%Y-%m-%d %H:%M %Z") if next_run else "pending start"
        print(f"    [{job.id}] {job.name} → next: {next_str}")
    print("  Press Ctrl+C to stop.")
    print("=" * 60)


def main() -> None:
    # 1. Ensure tables exist before anything else
    ensure_tables()

    # 2. Check Ollama availability (non-blocking — just logs)
    check_ollama()

    # 3. Build scheduler
    scheduler = BlockingScheduler(timezone=ET_TZ)

    scheduler.add_job(
        lambda: morning_chain(scheduler),
        trigger="cron",
        hour=8,
        minute=0,
        id="morning_chain",
        name="Morning Pipeline Chain (8 AM ET)",
        misfire_grace_time=3600,
        coalesce=True,
        replace_existing=True,
    )

    # Note: 11 PM evaluate job removed — morning_chain already calls evaluate_yesterday
    # which runs after midnight and catches all completed games from the prior day.

    # 4. Attach listener
    scheduler.add_listener(
        _make_listener(scheduler),
        EVENT_JOB_EXECUTED | EVENT_JOB_ERROR,
    )

    # 5. Print startup banner
    _print_banner(scheduler)

    # 6. Start (blocks until Ctrl+C)
    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user (KeyboardInterrupt).")
        scheduler.shutdown(wait=False)


if __name__ == "__main__":
    main()
