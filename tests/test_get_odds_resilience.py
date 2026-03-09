import unittest
from unittest.mock import patch

from server import get_odds


class _Resp:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload or []

    def json(self):
        return self._payload


class TestGetOddsResilience(unittest.TestCase):
    def test_make_request_retries_and_uses_stale_cache_on_failure(self):
        stale_data = [{"id": "cached"}]
        with patch.object(get_odds, "_read_cached_odds", return_value=(stale_data, get_odds.datetime.now())), \
             patch.object(get_odds.requests, "get", return_value=_Resp(500)), \
             patch.object(get_odds.time, "sleep", return_value=None), \
             patch.dict("os.environ", {"ODDS_REQUEST_RETRIES": "2", "ODDS_REQUEST_BACKOFF_SEC": "0"}, clear=False):
            data, _t = get_odds.make_request()

        self.assertEqual(data, stale_data)

    def test_make_request_short_circuits_to_stale_cache_when_circuit_open(self):
        stale_data = [{"id": "cached"}]
        get_odds._odds_circuit_open_until = get_odds.time.time() + 60
        try:
            with patch.object(get_odds, "_read_cached_odds", return_value=(stale_data, get_odds.datetime.now())), \
                 patch.object(get_odds.requests, "get") as request_mock:
                data, _t = get_odds.make_request()
            self.assertEqual(data, stale_data)
            request_mock.assert_not_called()
        finally:
            get_odds._odds_circuit_open_until = 0.0


if __name__ == "__main__":
    unittest.main()
