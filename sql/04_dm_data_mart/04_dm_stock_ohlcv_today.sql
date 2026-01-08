CREATE OR REPLACE TABLE `bi-us-market-pulse.04_dm_data_mart.stock_ohlcv_today` AS
SELECT
    *
FROM `bi-us-market-pulse.03_dw_data_warehouse.stock_ohlcv`
WHERE 1=1
    AND DATE = (SELECT MAX(DATE) FROM `bi-us-market-pulse.03_dw_data_warehouse.stock_ohlcv`)