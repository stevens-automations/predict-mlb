from typing import Tuple


def calculate_win_percentages(
    h_wins: int, h_loses: int, a_wins: int, a_loses: int
) -> Tuple[float, float]:
    home_pct = round(h_wins / (h_loses + h_wins), 3) if (h_loses + h_wins) > 0 else 0.000
    away_pct = round(a_wins / (a_loses + a_wins), 3) if (a_loses + a_wins) > 0 else 0.000
    return home_pct, away_pct


def get_predicted_winner_location(winner: str, home: str) -> str:
    return "home" if winner == home else "away"
