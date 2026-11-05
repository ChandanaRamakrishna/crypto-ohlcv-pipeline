import os
import sys
import unittest
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))
from ohlcv_utils import parse_klines  # noqa: E402


def kline(open_ms, close_ms):
    # Binance layout: [open_time, o, h, l, c, volume, close_time, quote_vol, trades, ...]
    return [open_ms, "100.5", "101.0", "99.5", "100.8", "12.34", close_ms, "1240.0", 321, "0", "0", "0"]


class ParseKlinesTest(unittest.TestCase):
    def test_converts_completed_candle(self):
        now = datetime(2026, 11, 5, 12, 0, tzinfo=timezone.utc)
        raw = [kline(1_793_000_000_000, 1_793_003_599_999)]   # closed well before 'now'
        rows = parse_klines("BTCUSDT", "1h", raw, now=now)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0:2], ["BTCUSDT", "1h"])
        self.assertEqual(rows[0][3:8], ["100.5", "101.0", "99.5", "100.8", "12.34"])
        self.assertEqual(rows[0][8], 321)

    def test_drops_still_forming_candle(self):
        now = datetime(2026, 11, 5, 12, 0, tzinfo=timezone.utc)
        future_close_ms = int(now.timestamp() * 1000) + 600_000
        raw = [kline(future_close_ms - 3_600_000, future_close_ms)]
        self.assertEqual(parse_klines("BTCUSDT", "1h", raw, now=now), [])

    def test_open_time_is_utc_iso(self):
        now = datetime(2030, 1, 1, tzinfo=timezone.utc)
        rows = parse_klines("ETHUSDT", "1h", [kline(0, 3_599_999)], now=now)
        self.assertEqual(rows[0][2], "1970-01-01T00:00:00+00:00")


if __name__ == "__main__":
    unittest.main()
