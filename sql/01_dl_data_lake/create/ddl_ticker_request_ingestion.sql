CREATE TABLE `bi-us-market-pulse.01_dl_data_lake.gspread_ticker_request_ingestion` (
    country_code STRING NOT NULL,
    exchange STRING NOT NULL,
    ticker STRING NOT NULL,
    name STRING NOT NULL,
    strategy STRING NOT NULL,
    ingestion_date DATE NOT NULL,
    task_timestamp TIMESTAMP NOT NULL
);

CREATE TABLE `bi-us-market-pulse.01_dl_data_lake.gspread_ticker_request` (
    country_code STRING NOT NULL,
    exchange STRING NOT NULL,
    ticker STRING NOT NULL,
    name STRING NOT NULL,
    strategy STRING NOT NULL,
    ingestion_date DATE NOT NULL,
    task_timestamp TIMESTAMP NOT NULL,
    merge_indicator STRING NOT NULL,
    request_date_from DATE,
    request_date_to date
);