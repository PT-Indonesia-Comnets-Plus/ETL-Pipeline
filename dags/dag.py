from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.sdk import Variable
# import json # No longer directly used in this file for parsing variables
from main.email_service import EmailServiceForAirflow
# Impor kelas-kelas ETL Anda (runner untuk task)
from main.extract import Extractor
from main.transform_asset import AssetTransformer as AssetTaskRunner
from main.transform_user import UserTransformer as UserTaskRunner
from main.validate import DataValidator
from main.load import Loader
from main.utils.create_database import ensure_database_schema
import logging

# --- Configuration Constants ---
SMTP_CREDENTIALS_VAR = "SMTP_CREDENTIALS_JSON"
ETL_EMAIL_RECIPIENT_VAR = "ETL_NOTIFICATION_RECIPIENT"

# XCom Keys (ensure these match keys used in your tasks)
XCOM_TRANSFORMED_ASSET_COUNT = "transformed_asset_count"
XCOM_TRANSFORMED_USER_COUNT = "transformed_user_count"
XCOM_LOAD_SUMMARY_REPORT = "load_summary_report"

# Standard Airflow loggers
dag_logger = logging.getLogger("airflow.dag.asset_data_pipeline")
task_logger = logging.getLogger("airflow.task")

# --- ETL Task Callables ---

# Fungsi callable untuk setiap task ETL utama


def run_extractor(**kwargs):
    ti = kwargs['ti']
    extractor = Extractor()
    extractor.run(ti=ti)


def run_asset_transformer(**kwargs):
    ti = kwargs['ti']
    asset_task_runner_instance = AssetTaskRunner()
    asset_task_runner_instance.run(ti=ti)


def run_user_transformer(**kwargs):
    ti = kwargs['ti']
    user_task_runner_instance = UserTaskRunner()
    user_task_runner_instance.run(ti=ti)


def run_validator(**kwargs):
    ti = kwargs['ti']
    validator = DataValidator()
    validator.run(ti=ti)


def run_loader(**kwargs):
    ti = kwargs['ti']
    loader = Loader()
    loader.run(ti=ti)

# --- Email Notification Functions ---


def _get_email_service_instance():
    """Helper to create and configure EmailServiceForAirflow instance."""
    logger = logging.getLogger("airflow.task.email_setup")
    try:
        smtp_config = Variable.get(SMTP_CREDENTIALS_VAR, deserialize_json=True)
        return EmailServiceForAirflow(
            smtp_server=smtp_config["smtp_server"],
            smtp_port=int(smtp_config["smtp_port"]),
            smtp_username=smtp_config["smtp_username"],
            smtp_password=smtp_config["smtp_password"]
        )
    except Exception as e:
        logger.error(
            f"Failed to get SMTP credentials from Airflow Variable '{SMTP_CREDENTIALS_VAR}': {e}")
        return None


def _send_etl_status_email(context, status: str):
    """
    Generic function to send ETL status email (success or failure).
    """
    ti = context['ti']
    dag_run = context.get('dag_run')
    run_id = dag_run.run_id if dag_run else ti.run_id
    dag_id = ti.dag_id
    # Available in callbacks and PythonOperator
    task_instance = context.get('task_instance')

    logger = logging.getLogger("airflow.task.send_etl_email")

    email_service_instance = _get_email_service_instance()
    if not email_service_instance:
        logger.error(
            "Email service instance could not be created. Skipping email notification.")
        return

    try:
        recipients_str = Variable.get(
            ETL_EMAIL_RECIPIENT_VAR, default_var="rizkyyanuarkristianto@gmail.com")
        recipients_list = [email.strip()
                           for email in recipients_str.split(',') if email.strip()]

        if not recipients_list or "rizkyyanuarkristianto@gmail.com" in recipients_list:
            logger.warning(
                f"Airflow Variable '{ETL_EMAIL_RECIPIENT_VAR}' not set, is empty, or uses default fallback. Please configure it with valid email addresses separated by commas. Current value: '{recipients_str}'")
            if not recipients_list:
                recipients_list = ["rizkyyanuarkristianto@gmail.com"]

    except Exception as e:
        logger.error(
            f"Failed to get recipient from Variable '{ETL_EMAIL_RECIPIENT_VAR}'. Error: {e}")
        # Fallback to a default list on error
        recipients_list = ["rizkyyanuarkristianto@gmail.com"]

    subject_status = "BERHASIL" if status.upper() == "SUCCESS" else "GAGAL"
    subject = f'Airflow ETL: {dag_id} - {subject_status} - Run ID: {run_id}'

    details_html_content = ""

    if status.upper() == "SUCCESS":
        new_asset_count_val = ti.xcom_pull(
            task_ids='transform_asset_data', key=XCOM_TRANSFORMED_ASSET_COUNT)
        new_user_count_val = ti.xcom_pull(
            task_ids='transform_user_data', key=XCOM_TRANSFORMED_USER_COUNT)
        load_summary_val = ti.xcom_pull(
            task_ids='load', key=XCOM_LOAD_SUMMARY_REPORT)

        details_html_content = "<h4>Ringkasan Data Baru yang Diproses dari Sumber:</h4><ul>"
        details_html_content += f"<li><strong>Data Aset Baru dari Sumber:</strong> {new_asset_count_val if new_asset_count_val is not None else 'Tidak ada'} baris.</li>"
        details_html_content += f"<li><strong>Data User Baru dari Sumber:</strong> {new_user_count_val if new_user_count_val is not None else 'Tidak ada'} baris.</li></ul>"

        details_html_content += "<h4>Ringkasan Data yang Dimuat:</h4><ul>"
        if load_summary_val and isinstance(load_summary_val, dict):
            if not load_summary_val:  # Check if the dictionary is empty
                details_html_content += "<li>Tidak ada data yang dimuat pada run ini.</li>"
            else:
                for table_name, summary_details in load_summary_val.items():
                    if isinstance(summary_details, dict):
                        rows = summary_details.get("rows_processed", "N/A")
                        drive_status = summary_details.get(
                            "drive_upload_status", "N/A")
                        supabase_status = summary_details.get(
                            "supabase_load_status", "N/A")
                        details_html_content += (
                            f"<li><strong>Tabel {table_name}:</strong> "
                            f"Baris diproses: {rows}, "
                            f"Status Upload Drive: {drive_status}, "
                            f"Status Load Supabase: {supabase_status}</li>"
                        )
                    # Fallback if summary_details is not a dict (e.g., just a status string)
                    else:
                        details_html_content += f"<li><strong>Tabel {table_name}:</strong> {summary_details}</li>"
        else:
            details_html_content += "<li>Ringkasan load tidak tersedia atau format tidak sesuai.</li>"
        details_html_content += "</ul>"

    elif status.upper() == "FAILURE":
        exception_info = context.get('exception')
        # task_instance is already available from context
        failed_task_id = task_instance.task_id if task_instance else "N/A"
        log_url = task_instance.log_url if task_instance else "#"

        details_html_content = f"""
        <h4>Detail Kegagalan:</h4>
        <ul>
            <li><strong>Task Gagal:</strong> {failed_task_id}</li>
            <li><strong>Pesan Error:</strong> <pre>{exception_info}</pre></li>
        </ul>
        <p>Silakan periksa log untuk detail lebih lanjut: <a href="{log_url}" target="_blank" rel="noopener noreferrer">Lihat Log Task</a></p>
        """

    email_service_instance.send_etl_notification(
        # Pass as comma-separated string if send_etl_notification expects string
        recipient=",".join(recipients_list),
        subject=subject,
        pipeline_name=dag_id,
        status=status,
        run_id=run_id,
        details_html=details_html_content
    )


def send_dag_success_notification_task(**kwargs):
    """Callable for PythonOperator to send success email for the entire DAG."""
    _send_etl_status_email(context=kwargs, status="SUCCESS")


def send_task_failure_notification_callback(context):
    """Callable for on_failure_callback to send failure email when a task fails."""
    _send_etl_status_email(context=context, status="FAILURE")

# --- DAG Definition ---


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,  # Using custom on_failure_callback
    'email_on_retry': False,
    'retries': 1,  # Adjust as needed for production
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_task_failure_notification_callback,
}

with DAG(
    dag_id='asset_data_pipeline',
    default_args=default_args,
    description='ETL pipeline for processing asset and user data, loading to Supabase and Google Drive.',
    start_date=datetime(2023, 1, 1),
    # Set to a cron expression for scheduled runs, e.g., '0 2 * * *' for 2 AM daily, or None for unscheduled
    schedule=None,
    catchup=False,
    tags=['etl_main', 'supabase', 'production']
) as dag:
    ensure_schema_task = PythonOperator(
        task_id='ensure_database_schema',
        python_callable=ensure_database_schema,
    )

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=run_extractor,
    )

    transform_asset_task = PythonOperator(
        task_id='transform_asset_data',
        python_callable=run_asset_transformer,
    )

    transform_user_task = PythonOperator(
        task_id='transform_user_data',
        python_callable=run_user_transformer,
    )

    validate_data_task = PythonOperator(
        task_id='validate_and_spliting',
        python_callable=run_validator,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=run_loader,
    )

    # Task PythonOperator untuk mengirim email kustom sukses
    send_email_task = PythonOperator(
        task_id='send_notification_email',
        python_callable=send_dag_success_notification_task,
        trigger_rule='all_success'  # Ensures this runs only if all upstream tasks succeed
    )

    # Definisikan alur dependensi
    ensure_schema_task >> extract_task
    extract_task >> [transform_asset_task, transform_user_task]
    [transform_asset_task, transform_user_task] >> validate_data_task
    validate_data_task >> load_task
    load_task >> send_email_task  # send_email_task depends on load_task success
