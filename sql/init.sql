-- Runs once, the first time the Postgres container starts.

CREATE TABLE IF NOT EXISTS candles (
    symbol        text         NOT NULL,
    timeframe     text         NOT NULL,           -- e.g. '1h'
    open_time     timestamptz  NOT NULL,
    open_price    numeric(24,8),
    high_price    numeric(24,8),
    low_price     numeric(24,8),
    close_price   numeric(24,8),
    volume        numeric(30,8),
    trades        integer,
    ingested_at   timestamptz  NOT NULL DEFAULT now(),
    -- the primary key is what makes re-runs idempotent (no duplicates, ever)
    PRIMARY KEY (symbol, timeframe, open_time)
);

-- Price columns are deliberately nullable with no CHECK constraints: this is a raw
-- landing table, and the DAG's quality-check task is what catches bad values.
CREATE INDEX IF NOT EXISTS idx_candles_time ON candles (timeframe, open_time);

CREATE TABLE IF NOT EXISTS dq_results (
    id          bigserial    PRIMARY KEY,
    run_ts      timestamptz  NOT NULL DEFAULT now(),
    run_id      text         NOT NULL,             -- Airflow run id
    check_name  text         NOT NULL,
    symbol      text         NOT NULL,
    status      text         NOT NULL,             -- PASS / FAIL
    bad_count   integer      NOT NULL DEFAULT 0
);

-- Analytics layer: returns, moving average, rolling volatility, approx VWAP, cross-sectional rank
CREATE OR REPLACE VIEW candle_metrics AS
WITH r AS (
    SELECT symbol, timeframe, open_time, close_price, volume,
           close_price / NULLIF(
               lag(close_price) OVER (PARTITION BY symbol, timeframe ORDER BY open_time), 0
           ) - 1 AS ret
    FROM candles
)
SELECT symbol,
       timeframe,
       open_time,
       close_price,
       volume,
       ret,
       avg(close_price) OVER w7                                       AS ma_7,
       stddev_samp(ret) OVER w24                                      AS vol_24,
       sum(close_price * volume) OVER w24
           / NULLIF(sum(volume) OVER w24, 0)                          AS vwap_24_approx,
       rank() OVER (PARTITION BY timeframe, open_time ORDER BY ret DESC) AS ret_rank
FROM r
WINDOW w7  AS (PARTITION BY symbol, timeframe ORDER BY open_time ROWS BETWEEN 6  PRECEDING AND CURRENT ROW),
       w24 AS (PARTITION BY symbol, timeframe ORDER BY open_time ROWS BETWEEN 23 PRECEDING AND CURRENT ROW);
