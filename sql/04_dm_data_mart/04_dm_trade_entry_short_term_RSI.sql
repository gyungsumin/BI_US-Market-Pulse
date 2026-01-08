DECLARE _cv_max_date DATE;
DECLARE _cv_min_date DATE;
SET _cv_max_date = (SELECT MAX(date) FROM `bi-us-market-pulse.03_dw_data_warehouse.stock_ohlcv`);
SET _cv_min_date = (DATE_SUB(_cv_max_date, INTERVAL 30 DAY));

CREATE OR REPLACE TABLE `bi-us-market-pulse.04_dm_data_mart.01_trade_entry_02_short_term_01_30days` AS
SELECT
    *
FROM `bi-us-market-pulse.03_dw_data_warehouse.stock_ohlcv`
WHERE 1=1
    AND DATE BETWEEN _cv_min_date AND _cv_max_date