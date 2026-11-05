\echo '--- 1. Row counts and coverage per symbol'
SELECT symbol, count(*) AS candles, min(open_time) AS first_candle, max(open_time) AS last_candle
FROM candles GROUP BY symbol ORDER BY symbol;

\echo '--- 2. Latest metrics per symbol (window-function view)'
SELECT DISTINCT ON (symbol)
       symbol, open_time, close_price,
       round(ret * 100, 3)       AS ret_pct,
       round(ma_7, 2)            AS ma_7,
       round(vol_24 * 100, 3)    AS vol_24_pct,
       round(vwap_24_approx, 2)  AS vwap_24
FROM candle_metrics
ORDER BY symbol, open_time DESC;

\echo '--- 3. Summed hourly returns over the last 24 hours, best to worst'
SELECT symbol, round(sum(ret) * 100, 2) AS approx_24h_return_pct
FROM candle_metrics
WHERE open_time >= now() - interval '24 hours'
GROUP BY symbol ORDER BY approx_24h_return_pct DESC;

\echo '--- 4. Latest data-quality results'
SELECT run_ts, check_name, symbol, status, bad_count
FROM dq_results ORDER BY id DESC LIMIT 20;

\echo '--- 5. Query plan for a typical lookup (shows the primary-key index in use)'
EXPLAIN ANALYZE
SELECT * FROM candles
WHERE symbol = 'BTCUSDT' AND timeframe = '1h' AND open_time >= now() - interval '7 days';
