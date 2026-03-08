from paths import get_env_path, load_env, resolve_path
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
    data_path = get_env_path("DATA_SHEET_PATH", "data/predictions.xlsx")

    required_paths = [model_path, scaler_path]
    for path in required_paths:
        if not os.path.exists(path):
            raise RuntimeError(f"Required runtime file not found: {path}")

    data_dir = os.path.dirname(resolve_path(data_path))
    if data_dir and not os.path.isdir(data_dir):
        raise RuntimeError(f"Data directory does not exist: {data_dir}")
