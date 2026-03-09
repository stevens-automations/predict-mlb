from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import json
import os


TRUE_VALUES = {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class SimulationConfig:
    enabled: bool
    date: Optional[str]
    fixture_path: Optional[str]


def _truthy(value: Optional[str]) -> bool:
    return str(value or "").strip().lower() in TRUE_VALUES


def get_simulation_config() -> SimulationConfig:
    enabled = _truthy(os.getenv("PREDICT_SIM_MODE"))
    date = str(os.getenv("PREDICT_SIM_DATE") or "").strip() or None
    fixture_path = str(os.getenv("PREDICT_SIM_FIXTURE_PATH") or "").strip() or None
    return SimulationConfig(enabled=enabled, date=date, fixture_path=fixture_path)


def simulation_enabled() -> bool:
    return get_simulation_config().enabled


def resolve_sim_date(default_dt: datetime) -> datetime:
    cfg = get_simulation_config()
    if not (cfg.enabled and cfg.date):
        return default_dt
    parsed = datetime.strptime(cfg.date, "%Y-%m-%d")
    return default_dt.replace(year=parsed.year, month=parsed.month, day=parsed.day)


def _fixture_path() -> Path:
    cfg = get_simulation_config()
    if not cfg.fixture_path:
        raise RuntimeError("PREDICT_SIM_FIXTURE_PATH must be set when PREDICT_SIM_MODE=true")
    path = Path(cfg.fixture_path)
    if not path.is_absolute():
        path = Path(__file__).resolve().parent / path
    return path


def load_sim_fixture() -> Dict[str, Any]:
    path = _fixture_path()
    if not path.exists():
        raise RuntimeError(f"Simulation fixture not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise RuntimeError("Simulation fixture must be a JSON object")
    return payload


def get_simulated_games() -> List[Dict[str, Any]]:
    fixture = load_sim_fixture()
    games = fixture.get("games", [])
    if not isinstance(games, list):
        raise RuntimeError("Simulation fixture field 'games' must be a list")
    return games


def get_simulated_prediction(game_id: int | str) -> Optional[Dict[str, Any]]:
    fixture = load_sim_fixture()
    predictions = fixture.get("predictions", [])
    gid = str(game_id)
    for item in predictions:
        if str(item.get("game_id")) == gid:
            return item
    return None


def posting_disabled() -> bool:
    if simulation_enabled():
        return True
    return _truthy(os.getenv("PREDICT_DRY_RUN")) or _truthy(os.getenv("PREDICT_DISABLE_POST"))
