CREATE TABLE `bi-us-market-pulse.04_dm_data_mart.stock_ohlcv`
(
    date DATE NOT NULL,
    country_code STRING NOT NULL,
    strategy STRING NOT NULL,
    exchange STRING NOT NULL,
    ticker STRING NOT NULL,
    name STRING NOT NULL,
    ohlcv STRUCT<
        high FLOAT64 NOT NULL,
        low FLOAT64 NOT NULL,
        close FLOAT64 NOT NULL,
        adj_close FLOAT64 NOT NULL,
        volume INT64 NOT NULL
    > NOT NULL,
    ingestion_date DATE NOT NULL,
    task_timestamp TIMESTAMP NOT NULL,
    ma60 FLOAT64,
    ma200 FLOAT64,
    ma200_gap FLOAT64,
    high_52w FLOAT64,
    close_52w_high_pct FLOAT64,
    rsi14 FLOAT64,
    macd FLOAT64,
    signal FLOAT64,
    bollinger_band FLOAT64,
    atr FLOAT64
)
PARTITION BY date
CLUSTER BY exchange, ticker;

