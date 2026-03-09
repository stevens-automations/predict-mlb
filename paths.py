from dotenv import load_dotenv  # type: ignore
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, ".env")


def load_env() -> None:
    load_dotenv(ENV_PATH)


def resolve_path(relative_or_absolute: str) -> str:
    if os.path.isabs(relative_or_absolute):
        return relative_or_absolute
    return os.path.join(BASE_DIR, relative_or_absolute)


def get_env_path(name: str, default: str) -> str:
    load_env()
    value = os.getenv(name, default)
    return resolve_path(value)


def get_predictions_db_path() -> str:
    """Resolve SQLite path for runtime storage (prefers SQLITE_DB_PATH)."""
    load_env()
    value = os.getenv("SQLITE_DB_PATH") or os.getenv("PREDICTIONS_DB_PATH") or "data/predictions.db"
    return resolve_path(value)
