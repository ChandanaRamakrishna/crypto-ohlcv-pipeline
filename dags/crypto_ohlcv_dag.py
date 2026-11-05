import os
from datetime import datetime, timedelta
from decimal import Decimal

import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from ohlcv_utils import parse_klines

BASE_URL = os.getenv("BINANCE_BASE_URL", "https://data-api.binance.vision")
SYMBOLS = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT").split(",")
TIMEFRAME = "1h"
LIMIT = 500            # candles fetched per symbol per run (~21 days of hourly data)
WINDOW_HOURS = 48      # how far back the quality checks look
STALE_HOURS = 3        # newest candle older than this => stale
MAX_HOURLY_MOVE = 0.15 # a >15% move between consecutive closes is flagged
CONN_ID = "market_db"

# Each check returns (symbol, bad_count) rows for symbols that violate it.
CHECKS = {
    "missing_candles": """
        SELECT symbol, count(*) FROM (
            SELECT symbol, open_time,
                   lag(open_time) OVER (PARTITION BY symbol ORDER BY open_time) AS prev_open
            FROM candles
            WHERE timeframe = '1h'
              AND open_time >= now() - make_interval(hours => %(window_hours)s)
        ) t
        WHERE prev_open IS NOT NULL AND open_time - prev_open > interval '1 hour'
        GROUP BY symbol
    """,
    "invalid_ohlc": """
        SELECT symbol, count(*) FROM candles
        WHERE timeframe = '1h'
          AND open_time >= now() - make_interval(hours => %(window_hours)s)
          AND (open_price IS NULL OR high_price IS NULL OR low_price IS NULL
               OR close_price IS NULL OR volume IS NULL
               OR least(open_price, high_price, low_price, close_price) <= 0
               OR volume < 0
               OR high_price < low_price
               OR high_price < greatest(open_price, close_price)
               OR low_price  > least(open_price, close_price))
        GROUP BY symbol
    """,
    "price_jump": """
        SELECT symbol, count(*) FROM (
            SELECT symbol,
                   close_price / NULLIF(
                       lag(close_price) OVER (PARTITION BY symbol ORDER BY open_time), 0
                   ) - 1 AS ret
            FROM candles
            WHERE timeframe = '1h'
              AND open_time >= now() - make_interval(hours => %(window_hours)s)
        ) t
        WHERE abs(ret) > %(max_move)s
        GROUP BY symbol
    """,
    "stale_data": """
        SELECT s.symbol, 1
        FROM unnest(%(symbols)s::text[]) AS s(symbol)
        LEFT JOIN candles c ON c.symbol = s.symbol AND c.timeframe = '1h'
        GROUP BY s.symbol
        HAVING max(c.open_time) IS NULL
            OR max(c.open_time) < now() - make_interval(hours => %(stale_hours)s)
    """,
}

UPSERT_SQL = """
    INSERT INTO candles
        (symbol, timeframe, open_time, open_price, high_price, low_price,
         close_price, volume, trades)
    VALUES %s
    ON CONFLICT (symbol, timeframe, open_time) DO UPDATE SET
        open_price  = EXCLUDED.open_price,
        high_price  = EXCLUDED.high_price,
        low_price   = EXCLUDED.low_price,
        close_price = EXCLUDED.close_price,
        volume      = EXCLUDED.volume,
        trades      = EXCLUDED.trades,
        ingested_at = now()
"""


@dag(
    dag_id="crypto_ohlcv_hourly",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["portfolio", "market-data"],
)
def crypto_ohlcv_hourly():

    @task
    def extract():
        """Pull recent hourly candles for every symbol from the Binance public API."""
        data = {}
        for symbol in SYMBOLS:
            resp = requests.get(
                f"{BASE_URL}/api/v3/klines",
                params={"symbol": symbol, "interval": TIMEFRAME, "limit": LIMIT},
                timeout=30,
            )
            resp.raise_for_status()
            data[symbol] = parse_klines(symbol, TIMEFRAME, resp.json())
        return data

    @task
    def load(data):
        """Idempotent upsert: re-running (or overlapping windows) never creates duplicates."""
        rows = [
            (r[0], r[1], datetime.fromisoformat(r[2]),
             Decimal(r[3]), Decimal(r[4]), Decimal(r[5]), Decimal(r[6]), Decimal(r[7]), r[8])
            for symbol_rows in data.values()
            for r in symbol_rows
        ]
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                execute_values(cur, UPSERT_SQL, rows, page_size=1000)
            conn.commit()
        finally:
            conn.close()
        return {"rows_upserted": len(rows)}

    @task
    def quality_check(load_result):
        """Run every check, log each result to dq_results, fail the task on any violation."""
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        run_id = get_current_context()["run_id"]
        params = {
            "symbols": SYMBOLS,
            "window_hours": WINDOW_HOURS,
            "stale_hours": STALE_HOURS,
            "max_move": MAX_HOURLY_MOVE,
        }
        results, failures = [], []
        for name, sql in CHECKS.items():
            bad = {sym: int(n) for sym, n in hook.get_records(sql, parameters=params)}
            for symbol in SYMBOLS:
                n = bad.get(symbol, 0)
                status = "FAIL" if n else "PASS"
                results.append((run_id, name, symbol, status, n))
                if n:
                    failures.append(f"{name}/{symbol}={n}")
        hook.insert_rows(
            table="dq_results",
            rows=results,
            target_fields=["run_id", "check_name", "symbol", "status", "bad_count"],
        )
        if failures:
            raise AirflowException("Data quality checks failed: " + "; ".join(failures))
        return {"checks_run": len(results), "failures": 0}

    quality_check(load(extract()))


crypto_ohlcv_hourly()
