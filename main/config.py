"""
Centralized configuration module for the ETL pipeline.
Contains constants, default configurations, and DAG settings.
"""
from datetime import timedelta
from typing import Dict, Any

# --- File System Configuration ---
DEFAULT_TEMP_DIR = "/opt/airflow/temp"
BACKUP_RETENTION_DAYS = 7

# --- Email Configuration Constants ---
SMTP_CREDENTIALS_VAR = "SMTP_CREDENTIALS_JSON"
ETL_EMAIL_RECIPIENT_VAR = "TO_EMAILS"

# --- Email Service Configuration ---
EMAIL_CONFIG = {
    'default_recipients': ["rizkyyanuarkristianto@gmail.com"],
    'max_recipients_per_email': 50,
    'email_timeout_seconds': 30,
    'retry_attempts': 3,
    'enable_email_validation': True,
    'fallback_on_failure': True,
}

# --- Email Template Configuration ---
EMAIL_TEMPLATE_CONFIG = {
    'company_name': 'Perusahaan Anda',
    'support_email': 'support@company.com',
    'timezone': 'Asia/Jakarta',
    'date_format': '%H:%M:%S %d-%m-%Y',
}

# --- Recipient Group Configuration ---
RECIPIENT_GROUPS = {
    'developers': ['dev@company.com'],
    'managers': ['manager@company.com'],
    'operations': ['ops@company.com'],
    'all_stakeholders': []  # Will be populated from ETL_EMAIL_RECIPIENT_VAR
}

# --- XCom Keys ---
XCOM_TRANSFORMED_ASSET_COUNT = "transformed_asset_count"
XCOM_TRANSFORMED_USER_COUNT = "transformed_user_count"
XCOM_LOAD_SUMMARY_REPORT = "load_summary_report"

# --- Task Configuration ---
DEFAULT_RETRY_COUNT = 1
DEFAULT_RETRY_DELAY = timedelta(minutes=5)

# --- DAG Configuration ---
DEFAULT_DAG_ARGS: Dict[str, Any] = {
    'owner': 'etl_team',
    'depends_on_past': False,
    'email_on_failure': False,  # Using custom on_failure_callback instead
    'email_on_retry': False,
    'retries': DEFAULT_RETRY_COUNT,
    'retry_delay': DEFAULT_RETRY_DELAY,
    # on_failure_callback will be set in the dag.py file
}

# --- Pipeline Configuration ---
PIPELINE_CONFIG = {
    'max_workers': 4,
    'chunk_size': 1000,
    'timeout_seconds': 3600,
    'enable_parallel_processing': True,
}
