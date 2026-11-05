"""Pure helper functions (no Airflow imports) so they are easy to unit test."""
from datetime import datetime, timezone


def parse_klines(symbol, timeframe, raw, now=None):
    """Turn Binance kline arrays into JSON-friendly rows.

    Binance returns the still-forming candle as the last element; we drop any
    candle whose close time is in the future so only completed candles are stored.

    Row layout: [symbol, timeframe, open_time_iso, open, high, low, close, volume, trades]
    Prices are kept as strings so no precision is lost before they reach numeric columns.
    """
    now = now or datetime.now(timezone.utc)
    rows = []
    for k in raw:
        open_time = datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc)
        close_time = datetime.fromtimestamp(k[6] / 1000, tz=timezone.utc)
        if close_time > now:
            continue
        rows.append([
            symbol, timeframe, open_time.isoformat(),
            k[1], k[2], k[3], k[4], k[5], int(k[8]),
        ])
    return rows
