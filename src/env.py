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
if not IS_CLOUD_RUN:  # Local env
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
if IS_CLOUD_RUN:  # Cloud
    pass
    # GCP_SA_SPREADSHEET_ACCESS = os.environ.get('GCP_SA_SPREADSHEET_ACCESS') -> 불필요 (ADC)
else:
    GCP_SA_SPREADSHEET_ACCESS = PROJECT_ROOT / os.environ.get('GCP_SA_SPREADSHEET_ACCESS')

## [2] SQL query files

