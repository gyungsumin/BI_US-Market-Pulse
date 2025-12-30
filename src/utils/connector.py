# Local env OAuth
from google.auth import default as google_auth_default
from google.oauth2.service_account import Credentials
from src.env import GCP_SA_SPREADSHEET_ACCESS

# Env
from src.env import IS_CLOUD_RUN
from src.env import GCP_PROJECT_ID

# API
import gspread
from google.cloud import bigquery


# Google Spreadsheet
def get_client_gspread():
    # Get Spreadsheet API Access
    if IS_CLOUD_RUN:
        creds, _ = google_auth_default(scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])
    else:
        creds = Credentials.from_service_account_file(
            str(GCP_SA_SPREADSHEET_ACCESS),
            scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
        )

    return gspread.authorize(creds)  # client


# BigQuery
def get_client_bigquery():
    if IS_CLOUD_RUN:
        creds, _ = google_auth_default(
            scopes=["https://www.googleapis.com/auth/bigquery"]
        )
    else:
        creds = Credentials.from_service_account_file(
            str(GCP_SA_SPREADSHEET_ACCESS),
            scopes=["https://www.googleapis.com/auth/bigquery"]
        )

    return bigquery.Client(
        credentials=creds,
        project=GCP_PROJECT_ID,
    )
