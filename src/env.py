import os
from pathlib import Path


# ===== Project root =====
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Apply dotenv() only when local execution
if not os.getenv("K_SERVICE"):
    from dotenv import load_dotenv
    load_dotenv()
    load_dotenv(PROJECT_ROOT / ".env", override=False)

# ===== GCP EV =====
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCP_PROJECT_NAME = os.getenv('GCP_PROJECT_NAME')

# print(Path(__file__).resolve())
# print(Path(__file__).resolve().parents[1])
#
# mport os
#
# # --- 환경 변수 관리 ---
# if not os.environ.get('K_SERVICE'):  # 로컬 실행 시 로컬 환경변수 호출
#     from dotenv import load_dotenv
#     load_dotenv()
#
# # --- SQL 쿼리 경로 ---
# # Docker 컨테이너 내부의 절대 경로 기준
# SQL_GET_CURRENT_DB = os.environ.get('SQL_GET_CURRENT_DB')
# SQL_INSERT_NEW_POST = os.environ.get('SQL_INSERT_NEW_POST')
#
# # --- GCP 환경 변수 ---
# EV_PROJECT_ID = os.environ.get('EV_PROJECT_ID')
# EV_REGION = os.environ.get('EV_REGION')
# EV_INSTANCE_NAME = os.environ.get('EV_INSTANCE_NAME')
# EV_DB_USER = os.environ.get('EV_DB_USER')
# EV_DB_NAME = os.environ.get('EV_DB_NAME')
#
# # --- GCP Secret Manager ---
# SECRET_CLOUDSQL_PASSWORD = os.environ.get('SECRET_CLOUDSQL_PASSWORD')
# SECRET_TEAMSWEBHOOKURL_TEST = os.environ.get('SECRET_TEAMSWEBHOOKURL_TEST')
# SECRET_TEAMSWEBHOOKURL_MAIN = os.environ.get('SECRET_TEAMSWEBHOOKURL_MAIN')
# SECRET_PROXYURL = os.environ.get('SECRET_PROXYURL')