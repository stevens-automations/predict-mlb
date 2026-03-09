from server.get_odds import get_todays_odds
from server.tweet_generator import gen_game_line
from datetime import datetime
from storage import get_primary_storage
import pandas as pd  # type: ignore


def prepare(game_info: pd.Series) -> str:
    """Refresh odds for a game row in SQLite and return tweet line."""
    storage = get_primary_storage()
    games, retrieval_time = get_todays_odds()
    home_odds, away_odds, home_odds_bookmaker, away_odds_bookmaker = (None, None, None, None)

    for game in games:
        if (
            game.get("home_team") == game_info.get("home")
            and game.get("away_team") == game_info.get("away")
            and game.get("time") == game_info.get("time")
        ):
            home, away = game_info["home"], game_info["away"]
            home_odds = str(game.get(f"{home}_odds"))
            away_odds = str(game.get(f"{away}_odds"))
            home_odds_bookmaker = game.get(f"{home}_bookmaker")
            away_odds_bookmaker = game.get(f"{away}_bookmaker")
            break

    df = storage.read_predictions()
    if "game_id" not in df.columns:
        return gen_game_line(game_info)

    game_id = game_info.get("game_id")
    matching_rows = df.index[df["game_id"] == game_id]
    if matching_rows.empty:
        return gen_game_line(game_info)

    row_index = matching_rows[0]
    df.at[row_index, "home_odds"] = home_odds if home_odds else df.at[row_index, "home_odds"]
    df.at[row_index, "away_odds"] = away_odds if away_odds else df.at[row_index, "away_odds"]
    df.at[row_index, "home_odds_bookmaker"] = home_odds_bookmaker if home_odds_bookmaker else df.at[row_index, "home_odds_bookmaker"]
    df.at[row_index, "away_odds_bookmaker"] = away_odds_bookmaker if away_odds_bookmaker else df.at[row_index, "away_odds_bookmaker"]
    df.at[row_index, "odds_retrieval_time"] = retrieval_time if home_odds else df.at[row_index, "odds_retrieval_time"]

    print(
        f"\n{datetime.now().strftime('%D - %I:%M:%S %p')}... Odds checked for updates: "
        f"{game_info['away']} ({'no update' if not away_odds else str(away_odds)}) @ "
        f"{game_info['home']} ({'no update' if not home_odds else str(home_odds)})\n"
    )

    updated_game_row = df.loc[row_index]
    tweet = gen_game_line(updated_game_row)
    df.at[row_index, "tweet"] = tweet
    storage.replace_predictions(df)
    return tweet
