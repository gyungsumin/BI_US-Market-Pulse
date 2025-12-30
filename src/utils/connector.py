import gspread
from google.auth import default as google_auth_default
from google.oauth2.service_account import Credentials

from src.env import IS_CLOUD_RUN,GCP_SA_SPREADSHEET_ACCESS


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
