# Crypto OHLCV Pipeline (Airflow + PostgreSQL)

An hourly Airflow pipeline that pulls candlestick (OHLCV) data for several crypto pairs from the
Binance public market-data API, loads it into PostgreSQL with idempotent upserts, runs automated
data-quality checks, and exposes a window-function analytics view.

```
Binance public API --> extract --> load (upsert) --> quality_check --> candle_metrics view
                                      |                    |
                                  candles table        dq_results table
```

## Run it

Prerequisites: Docker Desktop (4 GB+ RAM allocated). Start `docker compose pull` early; the Airflow image is large.

```bash
docker compose up -d
# wait ~2 minutes, then open http://localhost:8080  (login: admin / admin)
docker compose exec airflow airflow dags trigger crypto_ohlcv_hourly
```

Check the results:

```bash
docker compose exec postgres psql -U market -d market -f /queries.sql
```

Run the unit tests (no Docker needed): `python -m unittest tests.test_ohlcv_utils -v`

## Demo the quality checks catching bad data

```bash
# insert an off-schedule candle with a negative price (the upsert will not overwrite it)
docker compose exec postgres psql -U market -d market -c \
 "INSERT INTO candles (symbol,timeframe,open_time,open_price,high_price,low_price,close_price,volume,trades)
  VALUES ('BTCUSDT','1h', date_trunc('hour', now()) - interval '90 minutes', -5, 10, 1, 5, 1, 1);"

docker compose exec airflow airflow dags trigger crypto_ohlcv_hourly
# quality_check turns red in the UI; see why:
docker compose exec postgres psql -U market -d market -c \
 "SELECT check_name, symbol, status, bad_count FROM dq_results ORDER BY id DESC LIMIT 8;"

# clean up
docker compose exec postgres psql -U market -d market -c \
 "DELETE FROM candles WHERE close_price = 1 AND open_price = -5;"
```

## Design notes

- **Idempotent loads:** primary key `(symbol, timeframe, open_time)` plus `INSERT ... ON CONFLICT DO UPDATE`.
  Each run re-fetches the last 500 candles, so a missed run self-heals on the next one.
- **Duplicates** are prevented by the primary key rather than detected after the fact.
- **Only completed candles** are stored; the still-forming candle is dropped in `parse_klines`.
- **Quality checks:** missing candles (gaps), invalid OHLC (nulls, non-positive prices, high < low),
  price jumps above 15% between consecutive closes, and stale data. Each result is logged to
  `dq_results`, and any failure fails the Airflow task.
- **Analytics view:** `candle_metrics` uses `lag`, rolling `avg`, `stddev_samp`, a rolling
  volume-weighted price (approximated with close prices) and a cross-sectional `rank`.

## Limitations

- Hourly candles for 4 symbols; this is not tick data and not high-throughput.
- Airflow runs with the SequentialExecutor and a SQLite metadata DB, which is fine for a demo, not production.
- The VWAP is approximated from close prices, not trade-level data.
- No alerting beyond the failing task, and no backfill beyond the last 500 candles.

## Troubleshooting

- **HTTP 451 / 403 from the API:** your network or region may be blocked. Change `BINANCE_BASE_URL`
  in `docker-compose.yml` (for example to `https://api.binance.com`) and run `docker compose up -d` again.
- **DAG not showing:** `docker compose logs airflow | tail -50` and look for import errors.
- **Port 5433 or 8080 in use:** change the left-hand port number in `docker-compose.yml`.
