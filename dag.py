# airflow/dags/your_main_etl_dag.py
from datetime import datetime, timedelta
from airflow.decorators import DAG, task  # Menggunakan TaskFlow API
# from airflow.operators.python import PythonOperator # Jika menggunakan PythonOperator

# Impor fungsi-fungsi logika ETL dari modul terpisah
# Pastikan struktur folder Anda sesuai (misal: your_project/etl_logic.py)
from main.etl_pipeline import (
    load_and_initial_clean_func,
    transform_data_func,
    split_validate_data_func,
    log_dataframe_info_func,
    create_table_if_not_exists,  # Fungsi untuk DDL, jika tidak pakai operator khusus
    load_to_supabase_func
)

# Impor hook jika dipakai langsung di task definition (jarang dengan TaskFlow)
# from airflow.providers.postgres.hooks.postgres import PostgresHook


# --- Konfigurasi DAG ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- Konfigurasi Proyek (Opsional, tapi disarankan) ---
# Anda bisa mendefinisikan variabel konfigurasi di sini
# yang akan diteruskan ke task-task Anda.
SHEETS_ASSET_ID = '1bbXV377Gu4MxJzbRWxFr_kbSZyzyP80kCyhR80TDWzQ'
SHEETS_ASSET_NAME = 'Datek Aset All'
SHEETS_USER_ID = '1LMyZprJ_w3X6DC7Jqu0CpJaVQ3u8VUEMvSEADv1EMjc'
SHEETS_USER_NAME = 'Data All'
# Nama ENV var di Airflow Worker
GOOGLE_CREDENTIALS_ENV_VAR = 'GOOGLE_SHEET_CREDENTIALS'
# Nama Connection ID di Airflow UI
POSTGRES_CONN_ID = 'your_supabase_connection_id'
# Lokasi folder SQL di Airflow Worker
SQL_DDL_DIR = 'main/sql/'
SQL_DDL_FILE = 'schema.sql'


# --- Definisi DAG ---
dag = DAG(
    'weather-data-pipeline',
    default_args=default_args,
    description='Pipeline for retrieving and storing data aset and user from spreadsheet into PostgreSQL',
    schedule_interval='@hourly',
    catchup=False
)


def etl_pipeline():
    # --- Definisi Task Instances ---
    # Setiap panggilan fungsi yang dihias @task akan membuat task instance
    # Teruskan parameter konfigurasi yang dibutuhkan
    extract_initial_task = load_and_initial_clean_func(
        # task_id ditentukan di sini saat dipanggil
        task_id='extract_and_initial_clean',
        aset_spreadsheet_id=SHEETS_ASSET_ID,
        aset_sheet_name=SHEETS_ASSET_NAME,
        user_spreadsheet_id=SHEETS_USER_ID,
        user_sheet_name=SHEETS_USER_NAME,
        google_credentials_env_var_name=GOOGLE_CREDENTIALS_ENV_VAR,
        # temp_dir akan otomatis disediakan oleh Airflow
    )

    transform_asset_task = transform_asset_data_func(
        task_id='transform_asset_data',
        # Output dari task sebelumnya (path file) diteruskan sebagai input
        input_file_paths_initial=extract_initial_task.output,
        # temp_dir akan otomatis disediakan oleh Airflow
    )

    split_asset_task = split_validate_data_func(
        task_id='split_asset_tables',
        input_file_path_processed_asset=transform_asset_task.output,
        # temp_dir akan otomatis disediakan oleh Airflow
    )

    transform_user_task = transform_user_and_filter_func(
        task_id='transform_user_and_filter',
        input_file_paths_initial=extract_initial_task.output,  # Membutuhkan data user awal
        # Membutuhkan data asset untuk set valid ID
        input_file_path_processed_asset=transform_asset_task.output,
        # temp_dir akan otomatis disediakan oleh Airflow
    )

    log_info_task = log_dataframe_info_func(
        task_id='log_dataframe_info',
        input_file_paths_split_asset=split_asset_task.output,
        input_file_paths_filtered_user=transform_user_task.output,
        # temp_dir tidak selalu perlu di sini jika task ini hanya membaca dan log
    )

    # Task DDL (menggunakan fungsi terpisah dari etl_logic atau operator khusus)
    # Jika fungsi create_db_tables_func menggunakan Hook.run(sql=path):
    create_tables_task = create_table_if_not_exists(
        task_id='create_database_tables',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql_dir=SQL_DDL_DIR,
        sql_file_name=SQL_DDL_FILE,
    )
    # Jika Anda menggunakan operator bawaan Airflow (misal: PostgresOperator untuk SQL file):
    # from airflow.providers.postgres.operators.postgres import PostgresOperator
    # create_tables_task = PostgresOperator(
    #      task_id='create_database_tables',
    #      postgres_conn_id=POSTGRES_CONN_ID,
    #      sql=os.path.join(SQL_DDL_DIR, SQL_DDL_FILE), # Berikan path lengkap
    #      # Ini cara yang lebih standar jika task DDL hanya eksekusi SQL file
    # )

    load_task = load_to_supabase_func(
        task_id='load_to_supabase',
        postgres_conn_id=POSTGRES_CONN_ID,
        # Membutuhkan path file dari task split asset dan task transform user
        input_file_paths_asset_split=split_asset_task.output,
        input_file_paths_user_filtered=transform_user_task.output,
        # temp_dir tidak perlu di sini jika task hanya membaca file input
    )

    # --- Definisi Dependensi Task ---
    # Gunakan operator bitshift >> untuk mendefinisikan urutan
    # Task awal jalankan 2 task transform paralel
    extract_initial_task >> [transform_asset_task, transform_user_task]
    # Transform aset perlu selesai sebelum split
    transform_asset_task >> split_asset_task
    # Kedua task transform/split perlu selesai sebelum logging dan loading
    # Logging setelah kedua sumber diproses
    [split_asset_task, transform_user_task] >> log_info_task
    # Load setelah kedua sumber diproses
    [split_asset_task, transform_user_task] >> load_task

    # Task DDL harus berjalan sebelum task Load
    create_tables_task >> load_task
