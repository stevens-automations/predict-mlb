from server.tweet_generator import (
    gen_result_tweet,
    gen_game_line,
    gen_game_line_with_observability,
    create_tweets,
    summarize_enrichment_observability,
    get_enrichment_mode,
)
from apscheduler.schedulers.background import BlockingScheduler  # type: ignore
from apscheduler.events import (
    EVENT_SCHEDULER_STARTED,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MISSED,
)
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple
from uuid import uuid4
import json
from server.get_odds import get_todays_odds
from server.prep_tweet import prepare
from data import LeagueStats
from paths import load_env
from runtime import validate_runtime
from reliability_utils import get_predicted_winner_location
from storage import WriteStats, get_primary_storage
from simulation import (
    get_simulated_prediction,
    resolve_sim_date,
    simulation_enabled,
    posting_disabled,
)
import pandas as pd  # type: ignore
import subprocess
import threading
import statsapi  # type: ignore
import pytz  # type: ignore
import time
import os
import hashlib

selected_model = "mlb4year"
cwd = os.path.dirname(os.path.abspath(__file__))
load_env()
ret = os.getenv("SELECTED_MODEL")
selected_model = ret if ret is not None else selected_model

global_correct: int = 0
global_wrong: int = 0
global_biggest_upset: Optional[List] = None
global_upset_diff: int = 0
global_results: Optional[Tuple[str, str]] = None

mlb = LeagueStats()
lock = threading.Lock()
eastern = pytz.timezone("America/New_York")
daily_scheduler = None
current_run_id: Optional[str] = None


MAX_TWEET_LINE_LENGTH = 180
DEFAULT_ENRICHMENT_MISMATCH_RATE_WARN = 0.60
DEFAULT_ENRICHMENT_LOW_CONF_RATE_WARN = 0.70


COLUMN_ORDER = [
    "prediction_accuracy",
    "date",
    "time",
    "home",
    "home_probable",
    "away",
    "away_probable",
    "predicted_winner",
    "model",
    "favorite",
    "home_odds",
    "home_odds_bookmaker",
    "away_odds",
    "away_odds_bookmaker",
    "home_score",
    "away_score",
    "winning_pitcher",
    "losing_pitcher",
    "prediction_value",
    "venue",
    "series_status",
    "national_broadcasts",
    "odds_retrieval_time",
    "prediction_generation_time",
    "datetime",
    "game_id",
    "summary",
    "tweet",
    "time_to_tweet",
    "tweeted?",
]


def log_event(stage: str, result: str, game_id: Optional[str] = None, run_id: Optional[str] = None) -> None:
    payload = {
        "run_id": run_id or current_run_id or "unknown",
        "stage": stage,
        "game_id": game_id,
        "result": result,
    }
    print(f"[predict-log] {json.dumps(payload)}")


def print_next_job(event) -> None:
    time.sleep(1)
    ret = daily_scheduler.get_jobs()
    if daily_scheduler.running and len(ret) == 0:
        time.sleep(5)
        daily_scheduler.shutdown(wait=False)
        return
    next_job = ret[0] if (ret != []) else None
    if next_job is not None:
        print(f"{datetime.now(eastern).strftime('%D - %I:%M:%S %p')}... Next Scheduled Job")
        print(f"Job Name: {next_job.name}")
        et_time = next_job.next_run_time.astimezone(eastern)
        print(f"Next Execution Time: {et_time.strftime('%I:%M %p')} ET")
        time.sleep(1)


def update_row(row: pd.Series) -> pd.Series:
    global global_correct, global_wrong, global_biggest_upset, global_upset_diff
    predicted_winner = row["predicted_winner"]
    id = row["game_id"]
    games = statsapi.schedule(game_id=id)
    if not games:
        return row
    game = games[-1]
    if game["status"] != "Final":
        return row
    actual_winner = game.get("winning_team")
    prediction_accuracy = 1.0 if (actual_winner == predicted_winner) else (0.0 if actual_winner is not None else None)
    losing_team = row["home"] if actual_winner == row["away"] else row["away"]
    if actual_winner == row["home"]:
        winner_odds, loser_odds = row["home_odds"], row["away_odds"]
    else:
        winner_odds, loser_odds = row["away_odds"], row["home_odds"]
    def _coerce_odds(value):
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return int(value)
        s = str(value).strip()
        if not s:
            return None
        try:
            return int(s)
        except ValueError:
            try:
                return int(float(s))
            except Exception:
                return None

    winner_odds_i = _coerce_odds(winner_odds)
    loser_odds_i = _coerce_odds(loser_odds)

    if prediction_accuracy == 1.0:
        global_correct += 1
        if winner_odds_i is not None and loser_odds_i is not None:
            odds_diff = int((abs(winner_odds_i) - 100) + (abs(loser_odds_i) - 100))
            if odds_diff > global_upset_diff and winner_odds_i > 100:
                global_upset_diff = odds_diff
                global_biggest_upset = [actual_winner, winner_odds_i, losing_team, loser_odds_i]
    else:
        global_wrong += 1

    updated_row = row.copy()
    updated_row["prediction_accuracy"] = prediction_accuracy
    updated_row["home_score"] = game["home_score"]
    updated_row["away_score"] = game["away_score"]
    updated_row["winning_pitcher"] = game["winning_pitcher"]
    updated_row["losing_pitcher"] = game["losing_pitcher"]
    updated_row["datetime"] = game.get("datetime")
    updated_row["game_id"] = game["game_id"]
    updated_row["summary"] = game["summary"]
    return updated_row


def load_unchecked_predictions(storage, write_stats: Optional[WriteStats] = None) -> Optional[pd.DataFrame]:
    global global_results, global_correct, global_wrong
    global global_biggest_upset, global_upset_diff
    global_correct = 0
    global_wrong = 0
    global_biggest_upset = None
    global_upset_diff = 0
    global_results = None

    df = storage.read_predictions()
    if df.empty or "prediction_accuracy" not in df.columns:
        return df

    df_missing_accuracy = df[df["prediction_accuracy"].isnull()]
    df_missing_accuracy = df_missing_accuracy.apply(update_row, axis=1)
    if (global_correct + global_wrong) > 0:
        percentage = str(int(100 * round((global_correct / (global_correct + global_wrong)), 2))) + "%"
        correct_wrong = f"{str(global_correct)}/{str(global_wrong + global_correct)}"
        global_results = correct_wrong, percentage
        if global_biggest_upset is not None:
            is_upset = True
            upset_winner, upset_w_odds, upset_loser, upset_l_odds = global_biggest_upset
            res = gen_result_tweet(correct_wrong, percentage, is_upset, upset_winner, upset_loser, upset_w_odds, upset_l_odds)
        else:
            res = f"I was {percentage} ({correct_wrong}) accurate in predicting yesterday's MLB games. "
        if res:
            send_tweet(res)

    df.update(df_missing_accuracy)
    success, failure = storage.replace_predictions(df)
    if write_stats is not None:
        write_stats.add(success, failure)
    return df


def safely_prepare(row: pd.Series) -> str:
    try:
        lock.acquire()
        tweet_line = prepare(row)
    finally:
        lock.release()
    return tweet_line


def are_within_30_minutes(dt1, dt2):
    dt1 = datetime.fromisoformat(dt1.rstrip('Z'))
    dt2 = datetime.fromisoformat(dt2.rstrip('Z'))
    return abs(dt1 - dt2) <= timedelta(minutes=30)


def normalize_tweet_line(line: str) -> str:
    return line.replace("•", "").strip()


def unique_tweet_lines(tweet_lines: List[str]) -> List[str]:
    seen: Set[str] = set()
    unique_lines: List[str] = []
    for line in tweet_lines:
        normalized = normalize_tweet_line(line)
        if normalized in seen:
            continue
        seen.add(normalized)
        unique_lines.append(normalized)
    return unique_lines


def get_tweet_job_id(tweet: str) -> str:
    digest = hashlib.sha1(tweet.encode("utf-8")).hexdigest()[:12]
    return f"tweet_{digest}"


def _parse_float_env(name: str, default: float) -> float:
    raw = os.getenv(name, "")
    try:
        value = float(str(raw).strip())
        if 0.0 <= value <= 1.0:
            return value
    except (TypeError, ValueError):
        pass
    return default


def _required_line_fields_present(info: Dict) -> tuple[bool, List[str]]:
    required_fields = ["home", "away", "predicted_winner", "game_id", "date"]
    missing = [field for field in required_fields if info.get(field) in (None, "")]
    return len(missing) == 0, missing


def _validate_tweet_line(line: str) -> tuple[bool, Optional[str], str]:
    normalized = normalize_tweet_line(line)
    if not normalized:
        return False, "empty_line", normalized
    if len(normalized) > MAX_TWEET_LINE_LENGTH:
        return False, "line_too_long", normalized
    return True, None, normalized


def _guardrail_warn(reason: str, stage: str, run_id: Optional[str], details: Optional[Dict[str, object]] = None) -> None:
    payload: Dict[str, object] = {
        "run_id": run_id or current_run_id or "unknown",
        "stage": stage,
        "warning": reason,
    }
    if details:
        payload.update(details)
    print(f"[guardrail-warning] {json.dumps(payload, sort_keys=True)}")


def _emit_enrichment_threshold_warnings(summary: Dict[str, object], run_id: Optional[str], stage: str) -> List[str]:
    mismatch_warn = _parse_float_env("ENRICHMENT_MISMATCH_RATE_WARN", DEFAULT_ENRICHMENT_MISMATCH_RATE_WARN)
    low_conf_warn = _parse_float_env("ENRICHMENT_LOW_CONFIDENCE_RATE_WARN", DEFAULT_ENRICHMENT_LOW_CONF_RATE_WARN)
    warnings: List[str] = []

    mismatch_rate = float(summary.get("mismatch_rate", 0.0) or 0.0)
    low_count = int((summary.get("confidence_tier_distribution") or {}).get("L", 0))
    total = int(summary.get("total_game_lines", 0) or 0)
    low_conf_rate = (low_count / total) if total else 0.0

    if mismatch_rate >= mismatch_warn:
        warnings.append("enrichment_mismatch_rate_high")
    if low_conf_rate >= low_conf_warn:
        warnings.append("enrichment_low_confidence_rate_high")

    for warning in warnings:
        _guardrail_warn(
            reason=warning,
            stage=stage,
            run_id=run_id,
            details={
                "summary": summary,
                "mismatch_rate_warn": mismatch_warn,
                "low_confidence_rate_warn": low_conf_warn,
                "low_confidence_rate": round(low_conf_rate, 4),
            },
        )
    return warnings


def _write_enrichment_report(run_id: Optional[str], report: Dict[str, object]) -> Optional[str]:
    report_path = os.getenv("ENRICHMENT_REPORT_PATH", "").strip()
    if not report_path:
        report_path = f"docs/reports/enrichment-{datetime.now(eastern).date().isoformat()}.jsonl"
    if report_path.lower() in {"off", "false", "none", "0"}:
        return None

    resolved_path = os.path.join(cwd, report_path) if not os.path.isabs(report_path) else report_path
    report_dir = os.path.dirname(resolved_path)
    if report_dir:
        os.makedirs(report_dir, exist_ok=True)
    payload = dict(report)
    payload["run_id"] = run_id or current_run_id or "unknown"
    payload["timestamp_et"] = datetime.now(eastern).isoformat()
    with open(resolved_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, sort_keys=True) + "\n")
    return resolved_path


def generate_daily_predictions(storage, model: str = selected_model, date=datetime.now(), run_id: Optional[str] = None, write_stats: Optional[WriteStats] = None) -> tuple[List[str], List[str], int]:
    date = resolve_sim_date(date)

    scheduled_ids = []
    predicted_ids = []
    model = selected_model
    tweet_lines: List[str] = []
    observability_lines: List[str] = []
    enrichment_mode = get_enrichment_mode()
    log_event(stage="generate_daily_predictions", result=f"started mode={enrichment_mode}", run_id=run_id)

    df = storage.read_predictions()
    required_cols = {"date", "tweeted?", "game_id"}
    if required_cols.issubset(df.columns):
        dates = pd.to_datetime(df["date"], errors="coerce", utc=True).dropna()
        existing_dates = dates.dt.tz_convert(eastern).dt.date.unique()
        check_date = str(date.date())
        if check_date in [str(v) for v in existing_dates]:
            d = pd.to_datetime(df["date"], errors="coerce", utc=True).dt.tz_convert(eastern).dt.date
            mask = (d == date.date()) & (df["tweeted?"] == False)
            to_tweet_today = df[mask]
        else:
            to_tweet_today = pd.DataFrame()
    else:
        to_tweet_today = pd.DataFrame()

    if not to_tweet_today.empty:
        for _, row in to_tweet_today.iterrows():
            scheduled_ids.append(row["game_id"])
            predicted_ids.append(row["game_id"])
            prepared_line = safely_prepare(row)
            is_valid, reason, normalized = _validate_tweet_line(prepared_line)
            if not is_valid:
                _guardrail_warn(reason=reason or "invalid_line", stage="generate_daily_predictions", run_id=run_id, details={"game_id": row.get("game_id"), "line_length": len(normalized)})
                continue
            tweet_lines.append(normalized)
            _, obs_line = gen_game_line_with_observability(row, mode=enrichment_mode)
            observability_lines.append(obs_line)

    all_games, odds_time = get_todays_odds()
    game_predictions: List[Dict] = []

    if simulation_enabled():
        for game in all_games:
            if game.get("date") != "Today":
                continue
            if not game.get("sim_game_id"):
                continue
            scheduled_ids.append((game.get("sim_game_id"), game))
    else:
        scheduled_doubleheaders = []
        for game in all_games:
            if game.get("date") != "Today":
                continue
            today = date.strftime("%m/%d/%Y")
            teams_games = mlb.get_days_games(game.get("home_team"), today)
            if not teams_games:
                continue
            if len(teams_games) == 2:
                first, second = teams_games[0], teams_games[1]
                if first.get("game_id") and second.get("game_id") in scheduled_doubleheaders:
                    continue
                for game in all_games:
                    if game['home_team'] != first['home_name']:
                        continue
                    scheduled_ids.append((first.get("game_id"), game, first.get("game_num")))
                    scheduled_doubleheaders.append(first.get("game_id"))
                    first_ct = game.get("commence_time")
                    break
                for game in all_games:
                    if game['home_team'] != second['home_name']:
                        continue
                    if game.get("commence_time") == first_ct:
                        continue
                    scheduled_ids.append((second.get("game_id"), game, second.get("game_num")))
                    scheduled_doubleheaders.append(second.get("game_id"))
                    break
                continue
            elif len(teams_games) == 1:
                day_game = teams_games[0]
                if not are_within_30_minutes(day_game['game_datetime'], game['commence_time']):
                    continue
                if (day_game.get("game_id") not in scheduled_ids) and (day_game.get("game_date") == date.date().strftime("%Y-%m-%d")):
                    scheduled_ids.append((day_game.get("game_id"), game))

    for gameObj in scheduled_ids:
        try:
            gamePk = gameObj[0]
            if gamePk in predicted_ids:
                continue
            game = gameObj[1]
            if simulation_enabled():
                sim_prediction = get_simulated_prediction(gamePk)
                if sim_prediction is None:
                    log_event(stage="predict_game", game_id=str(gamePk), result="missing_sim_prediction", run_id=run_id)
                    continue
                info = {
                    "datetime": sim_prediction.get("datetime", game.get("commence_time")),
                    "date": sim_prediction.get("date", date.date().isoformat()),
                    "away": sim_prediction.get("away") or game.get("away_team"),
                    "home": sim_prediction.get("home") or game.get("home_team"),
                    "home_probable": sim_prediction.get("home_probable"),
                    "away_probable": sim_prediction.get("away_probable"),
                    "venue": sim_prediction.get("venue"),
                    "national_broadcasts": sim_prediction.get("national_broadcasts"),
                    "series_status": sim_prediction.get("series_status"),
                    "summary": sim_prediction.get("summary", f"{game.get('away_team')} @ {game.get('home_team')}"),
                    "game_id": gamePk,
                }
                winner = sim_prediction.get("predicted_winner")
                prediction = float(sim_prediction.get("prediction_value", 0.5))
            else:
                ret = mlb.predict_game(gamePk)
                if ret is None or ret[0] is None:
                    continue
                winner, prediction, info = ret[0], ret[1], ret[2]
        except Exception as e:
            log_event(stage="predict_game", game_id=str(gameObj[0]) if len(gameObj) > 0 else None, result=f"error: {e}", run_id=run_id)
            continue

        if not winner:
            continue
        home, away = info["home"], info["away"]
        info["predicted_winner"] = winner
        info["model"] = model
        info["predicted_winner_location"] = get_predicted_winner_location(winner, home)
        info["prediction_value"] = prediction
        info["time"] = game["time"]
        info["favorite"] = game.get("favorite")
        info["home_odds"] = str(game.get(f"{home}_odds"))
        info["away_odds"] = str(game.get(f"{away}_odds"))
        info["home_odds_bookmaker"] = game.get(f"{home}_bookmaker")
        info["away_odds_bookmaker"] = game.get(f"{away}_bookmaker")
        info["odds_retrieval_time"] = odds_time
        info["prediction_generation_time"] = datetime.now()
        info["prediction_accuracy"] = None
        info["home_score"] = None
        info["away_score"] = None
        info["winning_pitcher"] = None
        info["losing_pitcher"] = None
        info["tweeted?"] = False

        fields_present, missing_fields = _required_line_fields_present(info)
        if not fields_present:
            _guardrail_warn(
                reason="missing_required_fields",
                stage="generate_daily_predictions",
                run_id=run_id,
                details={"game_id": gamePk, "missing_fields": missing_fields},
            )
            continue

        tweet, observability_tweet = gen_game_line_with_observability(pd.Series(info), mode=enrichment_mode)
        if len(gameObj) == 3:
            tweet = f"{tweet} ({game['time']})"
            observability_tweet = f"{observability_tweet} ({game['time']})"

        is_valid, reason, normalized = _validate_tweet_line(tweet)
        if not is_valid:
            _guardrail_warn(reason=reason or "invalid_line", stage="generate_daily_predictions", run_id=run_id, details={"game_id": gamePk, "line_length": len(normalized)})
            continue

        info["tweet"] = normalized
        info["time_to_tweet"] = date.replace(hour=9, minute=45, second=0, microsecond=0).replace(tzinfo=None)

        tweet_lines.append(normalized)
        observability_lines.append(observability_tweet)
        game_predictions.append(info)
        log_event(stage="predict_game", game_id=str(gamePk), result="queued_for_tweet", run_id=run_id)

    df_new = pd.DataFrame(game_predictions)
    if len(df_new) > 0:
        for col in COLUMN_ORDER:
            if col not in df_new.columns:
                df_new[col] = None
        df_new = df_new[COLUMN_ORDER]
        success, failure = storage.upsert_predictions(df_new)
        if write_stats is not None:
            write_stats.add(success, failure)
    else:
        print(f"{datetime.now(eastern).strftime('%D - %I:%M:%S %p')}... \nNo new predictions made for games\n")

    enrichment_summary = summarize_enrichment_observability(observability_lines if observability_lines else tweet_lines)
    enrichment_summary_payload = json.dumps(enrichment_summary, sort_keys=True)
    print(f"[enrichment-summary] stage=generate_daily_predictions data={enrichment_summary_payload}")
    _emit_enrichment_threshold_warnings(enrichment_summary, run_id=run_id, stage="generate_daily_predictions")
    log_event(
        stage="generate_daily_predictions",
        result=f"completed: {len(tweet_lines)} tweet lines | enrichment_summary={enrichment_summary_payload}",
        run_id=run_id,
    )
    return tweet_lines, (observability_lines if observability_lines else tweet_lines), len(game_predictions)


def mark_as_tweeted(tweet: str) -> None:
    storage = get_primary_storage()
    lines = [normalize_tweet_line(line) for line in tweet.split('\n')]
    lines = [line for line in lines if line]
    storage.mark_tweeted(lines)


def send_tweet(tweet: str) -> bool:
    if posting_disabled():
        print(f"[dry-run] tweet suppressed: {tweet}")
        mark_as_tweeted(tweet)
        return True
    try:
        tweet_script = os.path.join(cwd, "server/tweet.py")
        process = subprocess.Popen(["python3", tweet_script, tweet], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        process.wait()
        stdout, stderr = process.communicate()
        print(stdout.strip())
        print(stderr.strip())
        return_code = process.poll()
        if return_code != 0:
            print(f"Error calling tweet.py: return code={return_code}")
            return False
        mark_as_tweeted(tweet)
        return True
    except subprocess.CalledProcessError as e:
        print(f"{datetime.now(eastern).strftime('%D - %I:%M:%S %p')}... \nError tweeting results{e}\n")
        return False


def schedule_tweets(tweet_lines: List[str], run_id: Optional[str] = None, observability_lines: Optional[List[str]] = None) -> tuple[int, Dict[str, object]]:
    global daily_scheduler
    if not tweet_lines or daily_scheduler is None:
        return 0, summarize_enrichment_observability([])

    filtered_lines: List[str] = []
    for line in unique_tweet_lines(tweet_lines):
        is_valid, reason, normalized = _validate_tweet_line(line)
        if not is_valid:
            _guardrail_warn(reason=reason or "invalid_line", stage="schedule_tweets", run_id=run_id, details={"line_length": len(normalized)})
            continue
        filtered_lines.append(normalized)

    if not filtered_lines:
        return 0, summarize_enrichment_observability([])

    tweets = create_tweets(filtered_lines)
    source_lines = observability_lines if observability_lines else filtered_lines
    batching_summary = summarize_enrichment_observability(source_lines)
    batching_summary["batched_tweets"] = len(tweets)
    batching_summary_payload = json.dumps(batching_summary, sort_keys=True)
    print(f"[enrichment-summary] stage=schedule_tweets data={batching_summary_payload}")
    log_event(stage="schedule_tweets", result=f"batching_summary={batching_summary_payload}", run_id=run_id)

    now = datetime.now(eastern)
    start_time = now.replace(hour=9, minute=45, second=0, microsecond=0)
    end_time = now.replace(hour=23, minute=59, second=59, microsecond=0)
    delay = 0
    scheduled_jobs = 0

    _emit_enrichment_threshold_warnings(batching_summary, run_id=run_id, stage="schedule_tweets")

    for tweet in tweets[::-1]:
        now = datetime.now(eastern)
        tweet_time = (now + timedelta(minutes=1, seconds=delay)) if (start_time <= now <= end_time) else datetime.now().replace(hour=9, minute=45, second=delay, microsecond=0)

        job_id = get_tweet_job_id(tweet)
        if daily_scheduler.get_job(job_id) is not None:
            log_event(stage="schedule_tweets", result="skipped_duplicate_job", game_id=job_id, run_id=run_id)
            continue

        daily_scheduler.add_job(send_tweet, args=[tweet], trigger="date", run_date=tweet_time, id=job_id, replace_existing=False)
        log_event(stage="schedule_tweets", result="scheduled", game_id=job_id, run_id=run_id)
        delay += 5
        scheduled_jobs += 1
    return scheduled_jobs, batching_summary


def check_and_predict():
    global daily_scheduler, current_run_id
    daily_scheduler = None
    current_run_id = str(uuid4())
    log_event(stage="check_and_predict", result="started", run_id=current_run_id)

    validate_runtime()
    storage = get_primary_storage()
    write_stats = WriteStats()

    if not simulation_enabled():
        try:
            load_unchecked_predictions(storage=storage, write_stats=write_stats)
        except Exception as e:
            log_event(stage="load_unchecked_predictions", result=f"error: {e}", run_id=current_run_id)

    daily_scheduler = BlockingScheduler(job_defaults={"coalesce": False}, timezone=eastern)
    daily_scheduler.add_listener(print_next_job, EVENT_SCHEDULER_STARTED)
    daily_scheduler.add_listener(print_next_job, EVENT_JOB_EXECUTED)
    daily_scheduler.add_listener(print_next_job, EVENT_JOB_MISSED)

    tweet_lines, observability_lines, predicted_games = generate_daily_predictions(storage=storage, run_id=current_run_id, write_stats=write_stats)
    scheduled_jobs, schedule_summary = schedule_tweets(tweet_lines, run_id=current_run_id, observability_lines=observability_lines)

    if not posting_disabled():
        daily_scheduler.start()
        time.sleep(10)
    else:
        print("[dry-run] scheduler start skipped")
    daily_scheduler = None
    threshold_warnings = _emit_enrichment_threshold_warnings(schedule_summary, run_id=current_run_id, stage="run_summary")
    summary = (
        f"run_summary predicted_games={predicted_games} "
        f"scheduled_jobs={scheduled_jobs} "
        f"sqlite_write_successes={write_stats.success} "
        f"sqlite_write_failures={write_stats.failure} "
        f"enrichment_mode={get_enrichment_mode()} "
        f"threshold_warnings={','.join(threshold_warnings) if threshold_warnings else 'none'}"
    )
    print(f"[predict-summary] {summary}")
    report_payload = {
        "predicted_games": predicted_games,
        "scheduled_jobs": scheduled_jobs,
        "sqlite_write_successes": write_stats.success,
        "sqlite_write_failures": write_stats.failure,
        "enrichment_mode": get_enrichment_mode(),
        "schedule_summary": schedule_summary,
        "threshold_warnings": threshold_warnings,
    }
    report_path = _write_enrichment_report(run_id=current_run_id, report=report_payload)
    if report_path:
        log_event(stage="check_and_predict", result=f"report_written={report_path}", run_id=current_run_id)
    log_event(stage="check_and_predict", result=summary, run_id=current_run_id)
    current_run_id = None


if __name__ == "__main__":
    check_and_predict()
