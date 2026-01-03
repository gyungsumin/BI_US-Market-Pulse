CREATE OR REPLACE TABLE `bi-us-market-pulse.03_dw_data_warehouse.stock_ohlcv` AS
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
        ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
  ) AS ma200
FROM `bi-us-market-pulse.02_dp_data_preprocessing.stock_ohlcv`
;