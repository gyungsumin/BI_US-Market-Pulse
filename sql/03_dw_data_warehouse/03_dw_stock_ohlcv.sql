CREATE OR REPLACE TABLE `bi-us-market-pulse.03_dw_data_warehouse.stock_ohlcv` AS
WITH BASE_GENERAL AS
(
  SELECT
    date
    , country_code
    , exchange
    , ticker
    , name
    , ohlcv
    , ingestion_date
    , CURRENT_TIMESTAMP() AS task_timestamp
    , ohlcv.high
    , ohlcv.low
    , ohlcv.close
    , ohlcv.adj_close
    , ohlcv.volume
    , strategy
    , AVG(ohlcv.close) OVER (
        PARTITION BY country_code, exchange, ticker
        ORDER BY date
        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
    ) AS ma60
    , AVG(ohlcv.close) OVER (
        PARTITION BY country_code, exchange, ticker
        ORDER BY date
        ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
    ) AS ma200
    , MAX(ohlcv.close) OVER (
        PARTITION BY country_code, exchange, ticker
        ORDER BY date
        ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
    ) AS high_52w
  FROM `bi-us-market-pulse.02_dp_data_preprocessing.stock_ohlcv`
)
, BASE_RSI14 AS
(
  WITH BASE1 AS
  -- Calculate day-over-day price change (delta)
  (
    SELECT
      *
      -- Difference between today's close and previous day's close
      , close - LAG(close, 1, 0) OVER (
        PARTITION BY country_code, exchange, ticker
        ORDER BY date
        ) AS _delta
    FROM BASE_GENERAL
  )
  , BASE2 AS
  (
    -- Separate upward (gain) and downward (loss) movements
    SELECT
      *,
      GREATEST(_delta, 0)  AS _gain,      -- 상승 폭 (positive price change)
      GREATEST(-_delta, 0) AS _loss       -- 하락 폭 (absolute negative change)
    FROM BASE1
  )
  , BASE3 AS
  (
    -- Calculate 14-period average gain / average loss (SMA-based)
    SELECT
      *
      , AVG(_gain) OVER (
        PARTITION BY country_code, exchange, ticker
        ORDER BY date
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS _avg_gain_14
      , AVG(_loss) OVER (
        PARTITION BY country_code, exchange, ticker
        ORDER BY date
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS _avg_loss_14
    FROM BASE2
  )
SELECT
  date
  , country_code
  , exchange
  , ticker
  , name
  , ohlcv
  , ingestion_date
  , task_timestamp
  , ohlcv.high
  , ohlcv.low
  , ohlcv.close
  , ohlcv.adj_close
  , ohlcv.volume
  , strategy
  , ma60
  , ma200
  , high_52w
  -- RSI14
  , CASE
    -- less than 14 observations
    WHEN _avg_gain_14 IS NULL OR _avg_loss_14 IS NULL THEN NULL
    -- If average loss is zero, RSI is set to 100
    WHEN _avg_loss_14 = 0 THEN 100
    -- RSI formula
    ELSE 100 - (100 / (1 + SAFE_DIVIDE(_avg_gain_14, _avg_loss_14)))
  END AS rsi14
FROM BASE3
)
, BASE_MACD AS
(
  WITH BASE1 AS
  (
    SELECT
      *
      -- EMA weights
      , 2.0 / (12 + 1) AS _alpha_12
      , 2.0 / (26 + 1) AS _alpha_26
      , 2.0 / (9  + 1) AS _alpha_9
    FROM BASE_RSI14
  )
  , BASE2 AS
  (
    SELECT
      *
      -- EMA(12) approximation
      , SUM(ohlcv.close * POW(1 - _alpha_12, _offset)) OVER (
        PARTITION BY country_code, exchange, ticker
        ORDER BY date
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
      )
      /
      SUM(POW(1 - _alpha_12, _offset)) OVER (
        PARTITION BY country_code, exchange, ticker
        ORDER BY date
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
      ) AS _ema_12
      -- EMA(26) approximation
      , SUM(ohlcv.close * POW(1 - _alpha_26, _offset)) OVER (
        PARTITION BY country_code, exchange, ticker
        ORDER BY date
        ROWS BETWEEN 25 PRECEDING AND CURRENT ROW
      )
      /
      SUM(POW(1 - _alpha_26, _offset)) OVER (
        PARTITION BY country_code, exchange, ticker
        ORDER BY date
        ROWS BETWEEN 25 PRECEDING AND CURRENT ROW
      ) AS _ema_26,
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY country_code, exchange, ticker
          ORDER BY date DESC
        ) - 1 AS _offset
      FROM BASE1
      )
  )
  , BASE3 AS
  (
    SELECT
      * EXCEPT(_offset)
      -- MACD line
      , _ema_12 - _ema_26 AS macd
    FROM BASE2
  )
  , BASE4 AS
  (
    SELECT
      *
      -- Signal line (EMA 9 of MACD)
      , SUM(macd * POW(1 - _alpha_9, _offset)) OVER (
        PARTITION BY country_code, exchange, ticker
        ORDER BY date
        ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
      )
      /
      SUM(POW(1 - _alpha_9, _offset)) OVER (
        PARTITION BY country_code, exchange, ticker
        ORDER BY date
        ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
      ) AS signal
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY country_code, exchange, ticker
          ORDER BY date DESC
        ) - 1 AS _offset
      FROM BASE3
    )
  )
  SELECT * FROM BASE4
)
, BASE_OTHERS AS
(
  SELECT
    *
    , SAFE_DIVIDE(ohlcv.close - ma200, ma200) AS ma200_gap
    , SAFE_DIVIDE(ohlcv.close - high_52w, high_52w) AS close_52w_high_pct
  FROM BASE_MACD
)
SELECT
  date
  , country_code
  , exchange
  , ticker
  , name
  , ohlcv
  , ingestion_date
  , task_timestamp
  , ohlcv.high
  , ohlcv.low
  , ohlcv.close
  , ohlcv.adj_close
  , ohlcv.volume
  , strategy
  , ma60
  , ma200
  , ma200_gap
  , high_52w
  , close_52w_high_pct
  , rsi14
  , macd
  , signal
  , NULL AS bollinger_band
  , NULL AS atr
FROM BASE_OTHERS
;
