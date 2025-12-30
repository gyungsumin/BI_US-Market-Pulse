"""
env.py
- Env activation by Local / Cloud environment
- General EVs: .env for local, .yaml for Cloud
- Credential EVs: JSON for local, ADC for Cloud
"""

from pathlib import Path
import os


# Environment detection
IS_CLOUD_RUN = bool(os.getenv("K_SERVICE"))
if IS_CLOUD_RUN:
    PROJECT_ROOT = Path('')
else:  # Local env
    # local project root
    PROJECT_ROOT = Path(__file__).resolve().parents[1]

    # .env read
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env", override=False)


# Assign EV (Fixed)
## [1] GCP EV
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
GCP_PROJECT_NAME = os.environ.get('GCP_PROJECT_NAME')

# Assign EV (Require path)
## [0] Credentials & ADC
if not IS_CLOUD_RUN:
    # Cloud 환경은 ADC로 불필요
    GCP_SA_SPREADSHEET_ACCESS = PROJECT_ROOT / os.environ.get('GCP_SA_SPREADSHEET_ACCESS')
# SQL_PATH = PROJECT_ROOT / os.environ.get('SQL_PATH')

# if IS_CLOUD_RUN:  # Cloud
#     pass
#     # GCP_SA_SPREADSHEET_ACCESS = os.environ.get('GCP_SA_SPREADSHEET_ACCESS') -> 불필요 (ADC)
#     SQL_PATH = os.environ.get('SQL_PATH')
# else:
#     GCP_SA_SPREADSHEET_ACCESS = PROJECT_ROOT / os.environ.get('GCP_SA_SPREADSHEET_ACCESS')
#     SQL_PATH = PROJECT_ROOT / os.environ.get('SQL_PATH')


