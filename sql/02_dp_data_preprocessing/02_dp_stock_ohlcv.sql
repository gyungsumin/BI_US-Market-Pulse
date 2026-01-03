CREATE OR REPLACE TABLE `bi-us-market-pulse.02_dp_data_preprocessing.stock_ohlcv` AS
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
LEFT JOIN _index AS B
ON 1=1
  AND A.country_code = B.country_code
  AND A.ticker = B.ticker
;