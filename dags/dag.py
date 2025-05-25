"""
Asset Data Pipeline DAG

This DAG orchestrates the complete ETL pipeline for processing asset and user data.
The pipeline includes extraction, transformation, validation, and loading stages
with email notifications for success/failure status.

Author: ETL Team
Version: 2.0
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from main.config import DEFAULT_DAG_ARGS
from main.email_service import (
    send_dag_success_notification_task,
    send_task_failure_notification_callback
)
from main.tasks import (
    run_extractor,
    run_asset_transformer,
    run_user_transformer,
    run_validator,
    run_loader
)
from main.utils.create_database import ensure_database_schema

# Configure DAG arguments with failure callback
dag_args = DEFAULT_DAG_ARGS.copy()
dag_args['on_failure_callback'] = send_task_failure_notification_callback

with DAG(
    dag_id='asset_data_pipeline',
    default_args=dag_args,
    description='ETL pipeline for processing asset and user data, loading to Supabase and Google Drive.',
    start_date=datetime(2023, 1, 1),
    schedule=None,  # Set to cron expression for scheduled runs, e.g., '0 2 * * *' for 2 AM daily
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
    )    # Task untuk mengirim email notifikasi sukses
    send_email_task = PythonOperator(
        task_id='send_notification_email',
        python_callable=send_dag_success_notification_task,
        trigger_rule='all_success'
    )

    # Define task dependencies
    ensure_schema_task >> extract_task
    extract_task >> [transform_asset_task, transform_user_task]
    [transform_asset_task, transform_user_task] >> validate_data_task
    validate_data_task >> load_task
    load_task >> send_email_task
