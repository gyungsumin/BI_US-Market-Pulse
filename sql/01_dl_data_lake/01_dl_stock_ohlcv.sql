-- Delete dropped tickers
DELETE FROM `bi-us-market-pulse.01_dl_data_lake.stock_ohlcv` AS A
WHERE EXISTS
(
    SELECT 1
    FROM (
        SELECT DISTINCT
            country_code
            , ticker
        FROM `bi-us-market-pulse.01_dl_data_lake.gspread_ticker_request`
        WHERE 1=1
            AND merge_indicator = 'delete'
    ) AS B
    WHERE 1=1
        AND A.country_code = B.country_code
        AND A.ticker = B.ticker
)
;

-- Insert new data for existing tickers
MERGE `bi-us-market-pulse.01_dl_data_lake.stock_ohlcv` AS T
USING `bi-us-market-pulse.01_dl_data_lake.stock_ohlcv_ingestion` AS S
ON 1=1
  AND T.date = S.date
  AND T.country_code = S.country_code
  AND T.ticker = S.ticker
WHEN MATCHED THEN UPDATE SET
  T.exchange = S.exchange,
  T.name = S.name,
  T.ohlcv.high = S.ohlcv.high,
  T.ohlcv.low = S.ohlcv.low,
  T.ohlcv.close = S.ohlcv.close,
  T.ohlcv.adj_close = S.ohlcv.adj_close,
  T.ohlcv.volume = S.ohlcv.volume,
  T.ingestion_date = S.ingestion_date,
  T.task_timestamp = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
(
  date,
  country_code,
  exchange,
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
  S.ticker,
  S.name,
  S.ohlcv,
  S.ingestion_date,
  S.task_timestamp
);
