DECLARE _cv_max_date DATE;
SET _cv_max_date = (SELECT MAX(date) FROM `bi-us-market-pulse.03_dw_data_warehouse.stock_ohlcv`);

CREATE OR REPLACE TABLE `bi-us-market-pulse.04_dm_data_mart.01_trade_entry_01_1day` AS
SELECT
    *
FROM `bi-us-market-pulse.03_dw_data_warehouse.stock_ohlcv`
WHERE 1=1
    AND DATE = _cv_max_date