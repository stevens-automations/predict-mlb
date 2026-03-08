from paths import get_env_path, get_predictions_db_path, load_env, resolve_path
from sqlite_phase1 import ensure_predictions_schema
import os

REQUIRED_ENV_VARS = [
    "CONSUMER_KEY",
    "CONSUMER_SECRET",
    "ACCESS_TOKEN",
    "ACCESS_TOKEN_SECRET",
    "ODDS_API_KEY",
]


def validate_runtime() -> None:
    load_env()
    missing = [name for name in REQUIRED_ENV_VARS if not os.getenv(name)]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    model_path = get_env_path("SELECTED_MODEL_PATH", "models/mlb4year.txt")
    scaler_path = get_env_path("SCALER_PATH", "models/scalers/mlb4year_scaler.pkl")
    db_path = get_predictions_db_path()

    required_paths = [model_path, scaler_path]
    for path in required_paths:
        if not os.path.exists(path):
            raise RuntimeError(f"Required runtime file not found: {path}")

    db_dir = os.path.dirname(resolve_path(db_path))
    if db_dir and not os.path.isdir(db_dir):
        raise RuntimeError(f"SQLite directory does not exist: {db_dir}")

    if not os.getenv("SQLITE_DB_PATH") and not os.getenv("PREDICTIONS_DB_PATH"):
        raise RuntimeError("SQLite DB path must be configured via SQLITE_DB_PATH or PREDICTIONS_DB_PATH")

    try:
        ensure_predictions_schema(db_path)
    except Exception as exc:
        raise RuntimeError(f"Failed to initialize/validate SQLite schema at {db_path}: {exc}") from exc
