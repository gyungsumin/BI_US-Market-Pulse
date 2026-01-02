# Data backfill
from src.constants import GSHEET_TICKER_fileId, GSHEET_TICKER_sheetName

import pandas as pd
from datetime import datetime, timezone
from google.cloud import bigquery


def gspread_to_bq(client_gspread, client_bigquery, ingestion_date):
    # Get recent ticker list from spreadsheet
    rows = client_gspread.open_by_key(GSHEET_TICKER_fileId).worksheet(GSHEET_TICKER_sheetName).get_all_values()

    df_ticker = pd.DataFrame(rows[1:], columns=rows[0])
    df_ticker = df_ticker.rename(columns={
        'Country Code': 'country_code',
        'Exchange': 'exchange',
        'Ticker': 'ticker',
        'Name': 'name',
        'Strategy': 'strategy'
    })
    df_ticker = df_ticker.groupby(['country_code', 'exchange', 'ticker'],
                                  as_index=False)[['name', 'strategy']].max()

    df_ticker['ingestion_date'] = ingestion_date
    df_ticker["task_timestamp"] = datetime.now(timezone.utc)

    # truncate
    job_config = bigquery.LoadJobConfig(
        write_disposition='WRITE_TRUNCATE',
        schema=[
            bigquery.SchemaField('country_code', 'STRING'),
            bigquery.SchemaField('exchange', 'STRING'),
            bigquery.SchemaField('ticker', 'STRING'),
            bigquery.SchemaField('strategy', 'STRING'),
            bigquery.SchemaField('name', 'STRING'),
            bigquery.SchemaField('ingestion_date', 'DATE'),
            bigquery.SchemaField('task_timestamp', 'TIMESTAMP'),
        ]
    )

    job = client_bigquery.load_table_from_dataframe(
        df_ticker,
        'bi-us-market-pulse.01_dl_data_lake.gspread_ticker_request_ingestion',
        job_config=job_config,
    )

    job.result()  # 작업 완료 대기
