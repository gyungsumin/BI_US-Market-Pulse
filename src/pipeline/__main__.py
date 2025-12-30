from src.env import PROJECT_ROOT
from src.utils import connector

# from src.pipeline._01_dl_data_lake import backfill, pipeline
from src.pipeline.ingestion import gspread_ticker_request_ingestion

import datetime
from pathlib import Path

def main():
    # ingestion_date
    ingestion_date = datetime.date.today()

    # connectors
    client_gspread = connector.get_client_gspread()
    client_bigquery = connector.get_client_bigquery()

    # Data Lake
    ## Data ingestion: gspread_ticker_request
    ## Truncate & Insert gspread_ticker_request_ingestion (최신 request 파일 초기 적재)
    gspread_ticker_request_ingestion.run(client_gspread=client_gspread,
                                         client_bigquery=client_bigquery,
                                         ingestion_date=ingestion_date)
    ## Truncate & Insert gspread_ticker_request (최신 request 기준 ticker_request 적재 및 전처리)
    _run_query(
        client=client_bigquery,
        query_path=PROJECT_ROOT / Path('sql/01_dl_data_lake/01_dl_gspread_ticker_request.sql')
    )

    # Data ingestion: yfinance
    ## ticker_request에서 신규, 업데이트 대상 조회
    ## 데이터 취득
    ## 임시 테이블 truncate

    # dl 테이블 merge (bq)
    ## dl 본 테이블에 merge (insert & update & delete 모두)
    ## 잘해봐 특정 범위만 머지, 전 범위 딜리트니까

def _run_query(client, query_path):
    with open(query_path, 'r', encoding='utf-8') as f:
        sql = f.read()

    job = client.query(sql)
    job.result()

if __name__ == '__main__':
    main()