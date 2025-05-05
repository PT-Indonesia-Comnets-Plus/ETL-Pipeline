from __future__ import annotations
import textwrap
import pendulum

from airflow.models import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.log.logging_mixin import LoggingMixin

# Import helper functions and classes
from main.extract import Extractor
from main.transform import Transformer
from main.load import Loader

# Set up logging
t_log = LoggingMixin().log

# Instantiate the DAG
with DAG(
    "asset_data_pipeline",
    default_args={
        'owner': 'airflow',
        'retries': 1,  # Retry once if failed
        'retry_delay': pendulum.duration(minutes=5),
    },
    description="DAG Pipeline untuk mengelola data aset",
    schedule='@daily',
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=['etl', 'supabase', 'google sheets'],
) as dag:

    # Empty task to mark the start
    start = EmptyOperator(task_id='start')

    # Documentation for the DAG
    dag.doc_md = __doc__

    # Task untuk Extract data
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=Extractor().run)  # Panggil fungsi extract langsung
    # provide_context=True, # Tidak diperlukan lagi di Airflow 2+
    extract_task.doc_md = textwrap.dedent("""
        #### Extract task
        Extract task that pulls data from Google Sheets and saves it as Parquet files.
    """)

    # Task untuk Transform data
    transform_task = PythonOperator(
        task_id="transform",
        python_callable=Transformer().run)  # Panggil fungsi transform langsung
    # provide_context=True, # Tidak diperlukan lagi di Airflow 2+

    transform_task.doc_md = textwrap.dedent("""
        #### Transform task
        Transform data (example: merge, clean, aggregate) and log basic statistics.
    """)

    # Task untuk Load data
    load_task = PythonOperator(
        task_id="load",
        python_callable=Loader().run)  # Panggil metode run dari instance Loader
    # provide_context=True, # Tidak diperlukan lagi di Airflow 2+

    load_task.doc_md = textwrap.dedent("""
        #### Load task
        Load the transformed data into the desired storage (e.g., database, file).
    """)

    # Task sequence
    start >> extract_task >> transform_task >> load_task
