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
