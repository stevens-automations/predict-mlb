import unittest

from reliability_utils import calculate_win_percentages, get_predicted_winner_location
from time_utils import parse_iso_z_to_eastern


class TestReliabilityFixes(unittest.TestCase):
    def test_get_win_percentage_away_pct(self):
        home_pct, away_pct = calculate_win_percentages(80, 40, 50, 70)
        self.assertEqual(home_pct, 0.667)
        self.assertEqual(away_pct, 0.417)

    def test_predicted_winner_location_equality(self):
        winner = "New York Mets"
        home = "".join(["New", " York", " Mets"])
        self.assertEqual(get_predicted_winner_location(winner, home), "home")

    def test_today_filter_timezone_consistency(self):
        # 2026-03-08T02:30Z is 2026-03-07 21:30 in New York (EST)
        et = parse_iso_z_to_eastern("2026-03-08T02:30:00Z")
        self.assertEqual(et.year, 2026)
        self.assertEqual(et.month, 3)
        self.assertEqual(et.day, 7)


if __name__ == "__main__":
    unittest.main()
