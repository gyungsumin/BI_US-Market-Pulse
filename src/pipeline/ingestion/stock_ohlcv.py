from google.cloud import bigquery

import pandas as pd
import yfinance as yf
from datetime import datetime, timezone
import time


def yfinance_to_bq(client_bigquery, ingestion_date):
    df_request = _get_request_list(client_bigquery=client_bigquery)
    if df_request.empty:
        return None

    df_yfinance = _get_yfinance(df_request)
    if df_yfinance.empty:
        return None

    df_yfinance['ingestion_date'] = ingestion_date
    df_yfinance["task_timestamp"] = datetime.now(timezone.utc)

    # To bq
    job_config = bigquery.LoadJobConfig(
        write_disposition='WRITE_TRUNCATE',
        schema=[
            bigquery.SchemaField('date', 'DATE', mode='REQUIRED'),
            bigquery.SchemaField('country_code', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('exchange', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('ticker', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField(
                'ohlcv',
                'RECORD',
                mode='REQUIRED',
                fields=[
                    bigquery.SchemaField('high', 'FLOAT', mode='REQUIRED'),
                    bigquery.SchemaField('low', 'FLOAT', mode='REQUIRED'),
                    bigquery.SchemaField('close', 'FLOAT', mode='REQUIRED'),
                    bigquery.SchemaField('adj_close', 'FLOAT', mode='REQUIRED'),
                    bigquery.SchemaField('volume', 'INTEGER', mode='REQUIRED'),
                ],
            ),
            bigquery.SchemaField('ingestion_date', 'DATE', mode='REQUIRED'),
            bigquery.SchemaField('task_timestamp', 'TIMESTAMP', mode='REQUIRED'),
        ]
    )

    job = client_bigquery.load_table_from_dataframe(
        df_yfinance,
        'bi-us-market-pulse.01_dl_data_lake.stock_ohlcv_ingestion',
        job_config=job_config
    )

    job.result()


def _get_request_list(client_bigquery):
    sql = '''
    SELECT DISTINCT
        country_code
        , exchange
        , ticker
        , request_date_from
        , request_date_to
    FROM `bi-us-market-pulse.01_dl_data_lake.gspread_ticker_request`
    WHERE 1=1
        AND merge_indicator = 'insert'
    '''

    return client_bigquery.query(sql).to_dataframe()


def _get_yfinance(request_list):
    df_result = pd.DataFrame()

    for e in request_list.index:
        df_temp = yf.download(
            tickers=[request_list.loc[e]['ticker']],
            start=request_list.loc[e]['request_date_from'],
            end=request_list.loc[e]['request_date_to'],
            auto_adjust=False,
            progress=False
        )
        df_temp.columns = df_temp.columns.get_level_values(0)
        df_temp['Ticker'] = request_list.loc[e]['ticker']

        if not df_temp.empty:
            info = yf.Ticker(request_list.loc[e]['ticker']).info
            df_temp['Exchange'] = info['exchange']
            df_temp['Name'] = info.get('shortName') or info.get('longName')

            df_temp['CountryCode'] = request_list.loc[e]['country_code']

            df_result = pd.concat([df_result, df_temp])

        time.sleep(10)

    df_result = df_result.reset_index().rename(columns={
        'Date': 'date',
        'High': 'high',
        'Low': 'low',
        'Close': 'close',
        'Adj Close': 'adj_close',
        'Volume': 'volume',
        'Ticker': 'ticker',
        'Exchange': 'exchange',
        'Name': 'name',
        'CountryCode': 'country_code'
    })

    df_result['date'] = pd.to_datetime(df_result['date']).dt.date

    df_result['ohlcv']  =  df_result.apply(
        lambda r: {
            'high': float(r['high']),
            'low': float(r['low']),
            'close': float(r['close']),
            'adj_close': float(r['adj_close']),
            'volume': int(r['volume']),
        },
        axis=1
    )

    return df_result[['date', 'country_code', 'exchange', 'ticker', 'name', 'ohlcv']]


if __name__ == '__main__':
    t = yf.download(
        tickers=['AAPL'],
        start='2025-01-01',
        end='2025-01-10',
        progress=False,
        auto_adjust = False,
    )
    print(t)