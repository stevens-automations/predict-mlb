"""
FastAPI server for the predict-mlb dashboard.

Endpoints:
  GET /api/predictions/today   - today's predictions (all columns incl tweet fields)
  GET /api/predictions/{date}  - predictions for any YYYY-MM-DD date
  GET /api/log                 - last 50 pipeline_log entries
  GET /api/accuracy            - overall + per-tier prediction accuracy stats
  GET /api/status              - latest pipeline job run status
"""

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import sqlite3, os
from datetime import datetime, timedelta
import pytz

app = FastAPI()
DB_PATH = os.path.join(os.path.dirname(__file__), "../data/mlb_history.db")
ET = pytz.timezone("America/New_York")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def rows_to_list(cursor):
    return [dict(row) for row in cursor.fetchall()]


@app.get("/api/predictions/today")
def get_today_predictions():
    today = datetime.now(ET).strftime("%Y-%m-%d")
    conn = None
    try:
        conn = get_db()
        cursor = conn.execute(
            "SELECT * FROM daily_predictions WHERE game_date = ?", (today,)
        )
        return rows_to_list(cursor)
    except sqlite3.Error:
        return []
    finally:
        if conn is not None:
            conn.close()


@app.get("/api/predictions/{date}")
def get_predictions_by_date(date: str):
    conn = None
    try:
        conn = get_db()
        cursor = conn.execute(
            "SELECT * FROM daily_predictions WHERE game_date = ?", (date,)
        )
        return rows_to_list(cursor)
    except sqlite3.Error:
        return []
    finally:
        if conn is not None:
            conn.close()


@app.get("/api/log")
def get_pipeline_log():
    conn = None
    try:
        conn = get_db()
        cursor = conn.execute(
            "SELECT * FROM pipeline_log ORDER BY ts DESC LIMIT 50"
        )
        return rows_to_list(cursor)
    except sqlite3.Error:
        return []
    finally:
        if conn is not None:
            conn.close()


@app.get("/api/accuracy")
def get_accuracy():
    conn = None
    try:
        conn = get_db()
        cursor = conn.execute(
            "SELECT * FROM daily_predictions WHERE did_predict_correct IS NOT NULL"
        )
        rows = rows_to_list(cursor)
    except sqlite3.Error:
        return {}
    finally:
        if conn is not None:
            conn.close()

    def summarize(records):
        total = len(records)
        correct = sum(int(record["did_predict_correct"]) for record in records)
        pct = correct / total if total > 0 else 0
        return {"total": total, "correct": correct, "pct": pct}

    by_tier = {}
    for tier in ["high", "medium", "low"]:
        tier_rows = [row for row in rows if row.get("confidence_tier") == tier]
        by_tier[tier] = summarize(tier_rows)

    return {"overall": summarize(rows), "by_tier": by_tier}


@app.get("/api/status")
def get_status():
    conn = None
    try:
        conn = get_db()
        cursor = conn.execute(
            "SELECT job, ts, status FROM pipeline_log ORDER BY ts DESC"
        )
        rows = rows_to_list(cursor)
    except sqlite3.Error:
        return {}
    finally:
        if conn is not None:
            conn.close()

    latest_by_job = {}
    for row in rows:
        job = row["job"]
        if job not in latest_by_job:
            latest_by_job[job] = {
                "last_run": row["ts"],
                "status": row["status"],
            }

    return latest_by_job


app.mount("/", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static"), html=True), name="static")
