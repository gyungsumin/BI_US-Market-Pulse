# Data backfill
from src.constants import GSHEET_TICKER_fileId, GSHEET_TICKER_sheetName
# import yaml
import pandas as pd


def run(client):
    # Get current spreadsheet ticker list
    rows = client.open_by_key(GSHEET_TICKER_fileId).worksheet(GSHEET_TICKER_sheetName).get_all_values()
    df_ticker = pd.DataFrame(rows[1:], columns=rows[0])  # header + body

    return df_ticker


#
#
# def _insert_new_ticker(df):
#
#
#
#
#
#
#
#
# def insert_new_ticker(client):
#     # Read spreadsheet
#     ws = client.open_by_key(GSHEET_TICKER_fileId).worksheet(GSHEET_TICKER_sheetName)
#     rows = ws.get_all_values()
#
#     ws = client.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)
#     rows = ws.get_all_values()
#
#     if not rows:
#         print("Empty sheet")
#         return
#
#     df = pd.DataFrame(rows[1:], columns=rows[0])  # header + body
#
#     print(f"mode={'cloud' if IS_CLOUD_RUN else 'local'}")
#     print(f"rows={len(df)} cols={len(df.columns)}")
#     print(df.head(PREVIEW_ROWS))
#
# # 수집 대상 항목 취득
# ## bq에서 현재 수집 중인 종목 수집 (min max date와 함께)
# ## 스프레드시트와 변동 탐지
# ## 변동 없음: 데일리 업데이트 라벨
# ## 제거됨: DL 삭제 라벨
# ## 추가됨: 신규 전 기간 적재 라벨
# ## 종목 별 적재 대상 min max 데이트 필드
#
# # class yfinanceRequest
# ## 티커별로 리퀴스트

# class tableDailyIngestion
## dl 테이블에 insert

# class tableRemoveTicker
## snp 탈락 티커 remove
##

## 스프레드 etf 리스트 탐지
### 변경 x: 데일리 업데이트 라벨
### 제거됨: DL 삭제 라벨
### 추가됨: 신규 전 기간 적재 라벨
## 현존하는 S&P 500 리스트 탐지 (위키)
### 변경 x: 데일리 업데이트 라벨
### 제거됨: DL 삭제 라벨
### 추가됨: 신규 전 기간 적재 라벨
## def 현재 gcp에 존재하는 s&p 500 리스트
#
# def _
#
#
#
#
#
# import pandas as pd
#
#
# WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
#
# tables = pd.read_html(WIKI_URL)
# sp500 = tables[0]  # 첫 번째 테이블이 S&P 500
#
# symbols = sp500["Symbol"].tolist()
# symbols[:10]
#
#
