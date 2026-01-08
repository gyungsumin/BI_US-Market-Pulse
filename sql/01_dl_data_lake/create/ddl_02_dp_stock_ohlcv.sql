CREATE TABLE `bi-us-market-pulse.02_dp_data_preprocessing.stock_ohlcv`
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
    task_timestamp TIMESTAMP NOT NULL
)
PARTITION BY date
CLUSTER BY exchange, ticker;

