MERGE `bi-us-market-pulse.02_dp_data_preprocessing.stock_ohlcv` AS T
USING 
(
  WITH _index AS
  (
    SELECT
      country_code
      , ticker
      , MAX(strategy) AS strategy
    FROM `bi-us-market-pulse.01_dl_data_lake.gspread_ticker_request`
    WHERE 1=1
      AND merge_indicator != 'delete'
    GROUP BY 1, 2
  )
  SELECT
    A.date
    , A.country_code
    , A.exchange
    , A.ticker
    , A.name
    , A.ohlcv
    , A.ingestion_date
    , CURRENT_TIMESTAMP() AS task_timestamp
    , A.ohlcv.high
    , A.ohlcv.low
    , A.ohlcv.close
    , A.ohlcv.adj_close
    , A.ohlcv.volume
    , B.strategy
  FROM `bi-us-market-pulse.01_dl_data_lake.stock_ohlcv` AS A
  INNER JOIN _index AS B
  ON 1=1
    AND A.country_code = B.country_code
    AND A.ticker = B.ticker
) AS S
ON 1=1
  AND T.date = S.date
  AND T.country_code = S.country_code
  AND T.ticker = S.ticker

-- 최근 N일 구간 Update
WHEN MATCHED
  AND T.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
THEN UPDATE SET
  T.exchange = S.exchange,
  T.name = S.name,
  T.strategy = S.strategy,
  T.ohlcv.high = S.ohlcv.high,
  T.ohlcv.low = S.ohlcv.low,
  T.ohlcv.close = S.ohlcv.close,
  T.ohlcv.adj_close = S.ohlcv.adj_close,
  T.ohlcv.volume = S.ohlcv.volume,
  T.ingestion_date = S.ingestion_date,
  T.task_timestamp = CURRENT_TIMESTAMP()

-- 신규 Date Insert
WHEN NOT MATCHED
THEN INSERT
(
  date,
  country_code,
  exchange,
  strategy,
  ticker,
  name,
  ohlcv,
  ingestion_date,
  task_timestamp
)
VALUES (
  S.date,
  S.country_code,
  S.exchange,
  S.strategy,
  S.ticker,
  S.name,
  S.ohlcv,
  S.ingestion_date,
  S.task_timestamp
);
