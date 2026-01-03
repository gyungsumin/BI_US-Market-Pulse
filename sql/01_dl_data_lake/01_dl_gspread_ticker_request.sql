-- Truncate 01_dl_data_lake.gspread_ticker_request
TRUNCATE TABLE `bi-us-market-pulse.01_dl_data_lake.gspread_ticker_request`;
INSERT INTO `bi-us-market-pulse.01_dl_data_lake.gspread_ticker_request`
WITH _ticker_tobe AS
(
  SELECT
    UPPER(country_code) AS country_code
    , UPPER(exchange) AS exchange
    , UPPER(ticker) AS ticker
    , MAX(name) AS name
    , MAX(strategy) AS strategy
    , MAX(ingestion_date) AS ingestion_date
    , MAX(task_timestamp) AS task_timestamp
  FROM `bi-us-market-pulse.01_dl_data_lake.gspread_ticker_request_ingestion`
  GROUP BY country_code, exchange, ticker
)
, _ticker_asis AS 
(
  SELECT *
  FROM `bi-us-market-pulse.01_dl_data_lake.gspread_ticker_request`
)
, _ticker_final AS
(
  SELECT
    A.country_code
    , A.exchange
    , A.ticker
    , A.name
    , A.strategy
    , A.ingestion_date
    , A.task_timestamp
    , CASE
      WHEN A.country_code IS NULL THEN 'delete'
      WHEN B.country_code IS NULL THEN 'insert'  -- Newly added
      ELSE 'insert' 
      END AS merge_indicator
    , COALESCE(DATE_ADD(max_d, INTERVAL 1 DAY), '2025-01-01') AS request_date_from
    , DATE_SUB(A.ingestion_date, INTERVAL 1 DAY) AS request_date_to
  FROM _ticker_tobe AS A
  FULL JOIN _ticker_asis AS B
  USING(country_code, exchange, ticker)
  LEFT JOIN
  (
    SELECT
      country_code
      , exchange
      , ticker
      , MIN(date) AS min_d
      , MAX(date) AS max_d
    FROM `bi-us-market-pulse.01_dl_data_lake.stock_ohlcv_ingestion`
    GROUP BY country_code, exchange, ticker
  ) AS C
  USING(country_code, exchange, ticker)
)
SELECT * FROM _ticker_final
;


