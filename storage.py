"""Storage abstraction scaffold for future backend migration.

Current runtime still uses Excel directly.
This module is intentionally not wired into runtime behavior yet.
"""

from typing import Protocol
import pandas as pd  # type: ignore


class PredictionStorage(Protocol):
    """Minimal storage contract for prediction persistence."""

    def read_predictions(self, path: str) -> pd.DataFrame:
        ...

    def write_predictions(self, path: str, df: pd.DataFrame) -> None:
        ...


class ExcelPredictionStorage:
    """Excel-backed storage implementation placeholder.

    TODO:
    - Add schema validation helpers for read/write boundaries.
    - Add atomic write strategy for safer persistence.
    - Introduce SQLite implementation and switch via configuration.
    """

    def read_predictions(self, path: str) -> pd.DataFrame:
        return pd.read_excel(path)

    def write_predictions(self, path: str, df: pd.DataFrame) -> None:
        df.to_excel(path, index=False)
