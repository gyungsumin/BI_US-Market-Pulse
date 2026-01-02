from src.env import PROJECT_ROOT
from src.utils import connector, bq

# from src.pipeline._01_dl_data_lake import backfill, pipeline
from src.pipeline.ingestion import ticker_request, stock_ohlcv

import datetime
from pathlib import Path

def main():
    # ingestion_date
    ingestion_date = datetime.date.today()

    # connectors
    client_gspread = connector.get_client_gspread()
    client_bigquery = connector.get_client_bigquery()

    # 1. Data Lake
    ## 1-1. Data ingestion: gspread_ticker_request
    ## Truncate & Insert "01_dl_data_lake.gspread_ticker_request_ingestion" (최신 request 파일 초기 적재)
    ticker_request.gspread_to_bq(client_gspread=client_gspread,
                                 client_bigquery=client_bigquery,
                                 ingestion_date=ingestion_date)
    ## Truncate & Insert "01_dl_data_lake.gspread_ticker_request" (최신 request 기준 ticker_request 적재 및 전처리)
    bq.run_query(
        client=client_bigquery,
        query_path=PROJECT_ROOT / Path('sql/01_dl_data_lake/01_dl_gspread_ticker_request.sql')
    )

    # 1-2 Data ingestion: yfinance
    stock_ohlcv.yfinance_to_bq(client_bigquery=client_bigquery, ingestion_date=ingestion_date)

    ## ticker_request에서 신규, 업데이트 대상 조회
    ## 데이터 취득
    ## 임시 테이블 truncate

    # dl 테이블 merge (bq)
    ## dl 본 테이블에 merge (insert & update & delete 모두)
    ## 잘해봐 특정 범위만 머지, 전 범위 딜리트니까


if __name__ == '__main__':
    main()