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
INSERT INTO `bi-us-market-pulse.01_dl_data_lake.stock_ohlcv`
SELECT *
FROM `bi-us-market-pulse.01_dl_data_lake.stock_ohlcv_ingestion`
;

-- 중복탐지 제거 로직 필요하긴 함
