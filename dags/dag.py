from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Impor kelas-kelas ETL Anda (runner untuk task)
from main.extract import Extractor
from main.transform_asset import Transformer as AssetTaskRunner
from main.transform_user import Transformer as UserTaskRunner
from main.validate import DataValidator
from main.load import Loader
# Impor fungsi dari modul utilitas
from main.utils.create_database import ensure_database_schema
from airflow.providers.smtp.operators.smtp import EmailOperator

import logging  # Add this import for the workaround
dag_logger = logging.getLogger(
    "airflow.task.asset_data_pipeline")  # Or a more specific logger


# Fungsi callable untuk setiap task ETL utama
def run_extractor(**kwargs):
    ti = kwargs['ti']
    extractor = Extractor()
    extractor.run(ti=ti)


def run_asset_transformer(**kwargs):
    ti = kwargs['ti']
    # Instansiasi kelas runner yang benar
    asset_task_runner_instance = AssetTaskRunner()
    asset_task_runner_instance.run(ti=ti)


def run_user_transformer(**kwargs):
    ti = kwargs['ti']
    # Instansiasi kelas runner yang benar
    user_task_runner_instance = UserTaskRunner()
    user_task_runner_instance.run(ti=ti)


def run_validator(**kwargs):
    ti = kwargs['ti']
    validator = DataValidator()
    validator.run(ti=ti)


def run_loader(**kwargs):
    ti = kwargs['ti']
    # Ambil path dari XComs
    # Key XCom sekarang "final_data_paths_for_load" dari task validator
    final_data_paths_dict = ti.xcom_pull(
        task_ids="validate_and_combine_data", key="final_data_paths_for_load")

    # Periksa apakah dictionary path ada dan tidak kosong
    if not final_data_paths_dict or not isinstance(final_data_paths_dict, dict):
        dag_logger.error(  # Changed from ti.log.error
            "Final data paths dictionary for load not found in XComs or is invalid.")
        # Jika tidak ada data sama sekali, mungkin tidak perlu error, tapi log warning
        # raise ValueError("Transformed data paths dictionary is required for Loader.")
        dag_logger.warning(  # Changed from ti.log.warning
            "Tidak ada data yang akan di-load karena final_data_paths_for_load kosong atau tidak valid.")
        return  # Keluar dari fungsi jika tidak ada yang di-load

    loader = Loader()
    # Modifikasi Loader untuk menerima path dan memuat ke Supabase
    loader.run_load_to_supabase(
        transformed_data_paths=final_data_paths_dict, ti=ti)  # Kirim dictionary final


default_args = {
    'owner': 'airflow',
    'email': ['rizkyyanuarkristianto@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

with DAG(
    dag_id='asset_data_pipeline',  # Nama DAG baru atau disesuaikan
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['etl_main', 'supabase'],
    default_args=default_args,
) as dag:
    ensure_schema_task = PythonOperator(
        task_id='ensure_database_schema',
        python_callable=ensure_database_schema,
    )

    extract_task = PythonOperator(
        task_id='extract_data',
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
        task_id='validate_and_combine_data',
        python_callable=run_validator,
    )

    load_task = PythonOperator(
        task_id='load_data_to_supabase',
        python_callable=run_loader,  # run_loader sekarang akan memanggil logika Supabase
    )

    send_success_email = EmailOperator(
        task_id='send_success_notification_email',
        to='rizkyyanuarkristianto@gmail.com',
        subject='Airflow ETL: Data Aset & User Berhasil Dimuat ke Supabase',
        html_content="""
            <h3>Notifikasi Keberhasilan Proses ETL ke Supabase</h3>
            <p>Proses ETL untuk data aset dan user telah berhasil dijalankan pada <strong>{{ ds }}</strong>.</p>
            <p>Data telah dimuat ke database Supabase.</p>
            
            <h4>Ringkasan Data yang Dimuat:</h4>
            <ul>
            {% set summary = ti.xcom_pull(task_ids='load_data_to_supabase', key='load_summary') %}
            {% if summary %}
              {% for table, count in summary.items() %}
                <li><strong>{{ table }}:</strong> {{ count }} baris diproses/dimuat.</li>
              {% endfor %}
            {% else %}
              <li>Tidak ada ringkasan data yang tersedia dari proses load.</li>
            {% endif %}
            </ul>

            <p><strong>Run ID:</strong> {{ run_id }}</p>
            <p>Silakan periksa data di Supabase.</p>
        """,
        trigger_rule='all_success'  # Kirim email hanya jika semua task upstream sukses
    )

    # Definisikan alur dependensi
    ensure_schema_task >> extract_task
    extract_task >> [transform_asset_task, transform_user_task]
    [transform_asset_task, transform_user_task] >> validate_data_task
    validate_data_task >> load_task
    load_task >> send_success_email
