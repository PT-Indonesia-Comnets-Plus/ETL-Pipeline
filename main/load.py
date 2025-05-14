from airflow.utils.log.logging_mixin import LoggingMixin
import os
import pandas as pd
import io
import json
from airflow.sdk import Variable
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
# Import untuk koneksi database Supabase
from .utils.config_database import initialize_database_connections

# --- Configuration Constants ---
GOOGLE_CREDENTIALS_KEY_VAR = "Google_Credentials_Key"
GOOGLE_DRIVE_TARGET_FOLDER_ID_VAR = "GOOGLE_CREDENTIALS_TARGET_FOLDER_ID"
XCOM_FINAL_DATA_PATHS_KEY = "final_data_paths_for_load"
XCOM_VALIDATE_TASK_ID = "validate_and_spliting"


class Loader:
    def __init__(self):
        self.log = LoggingMixin().log
        self.drive_service = None
        self.db_pool = None
        self.db_engine = None

    def _authenticate_drive(self):
        """Authenticate with Google Drive API."""
        try:
            self.log.info("🔐 Autentikasi Google Drive...")
            creds_json = Variable.get(GOOGLE_CREDENTIALS_KEY_VAR)
            creds_dict = json.loads(creds_json)
            scope = ["https://www.googleapis.com/auth/drive"]
            credentials = service_account.Credentials.from_service_account_info(
                creds_dict, scopes=scope)
            service = build('drive', 'v3', credentials=credentials)
            self.log.info("✅ Autentikasi Google Drive berhasil.")
            return service
        except Exception as e:
            self.log.error(
                f"❌ Autentikasi Google Drive gagal dengan Variable '{GOOGLE_CREDENTIALS_KEY_VAR}': {e}")
            raise

    def _initialize_db_resources(self):
        """Initialize the database connection pool and SQLAlchemy engine if not already initialized."""
        if self.db_pool is None or self.db_engine is None:
            self.log.info(
                "🔌 Menginisialisasi psycopg2 pool dan SQLAlchemy engine untuk Loader...")
            try:
                pool, engine = initialize_database_connections()
                self.db_pool = pool
                self.db_engine = engine

                if self.db_pool and self.db_engine:
                    self.log.info(
                        "✅ Psycopg2 pool dan SQLAlchemy engine berhasil diinisialisasi untuk Loader.")
                else:
                    self.log.error(
                        "❌ Gagal menginisialisasi psycopg2 pool atau SQLAlchemy engine untuk Loader (salah satu atau keduanya None).")
                    raise ConnectionError(
                        "Gagal mendapatkan DB pool atau engine dari initialize_database_connections.")
            except ImportError:
                self.log.error(
                    "❌ Gagal mengimpor 'initialize_database_connections'. Pastikan path impor benar.")
                raise
            except Exception as e:
                self.log.error(
                    f"❌ Error tidak terduga saat menginisialisasi database resources untuk Loader: {e}")
                raise

    def _create_drive_folder_if_not_exists(self, folder_name, parent_folder_id):
        """Mencari atau membuat folder di Google Drive dan mengembalikan ID-nya."""
        query = f"name='{folder_name}' and '{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        response = self.drive_service.files().list(
            q=query, spaces='drive', fields='files(id, name)').execute()
        folders = response.get('files', [])

        if folders:
            self.log.info(
                f"ℹ️ Folder '{folder_name}' sudah ada dengan ID: {folders[0]['id']}")
            return folders[0]['id']
        else:
            self.log.info(
                f"⏳ Membuat folder '{folder_name}' di dalam folder ID: {parent_folder_id}")
            file_metadata = {
                'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_folder_id]}
            folder = self.drive_service.files().create(
                body=file_metadata, fields='id').execute()
            self.log.info(
                f"✅ Folder '{folder_name}' dibuat dengan ID: {folder.get('id')}")
            return folder.get('id')  # type: ignore

    def _upload_df_to_drive(self, table_name: str, df: pd.DataFrame, target_folder_id: str) -> str:
        """Load the transformed data for a specific table by uploading CSV to Google Drive"""
        self.log.info(
            f"   -> Memproses data untuk upload Drive '{table_name}': {len(df)} baris.")

        if df.empty:
            self.log.warning(
                f"   -> DataFrame '{table_name}' kosong, tidak diupload ke Google Drive.")
            return "Skipped (empty)"

        try:
            csv_file_name = f"{table_name}.csv"
            self.log.info(
                f"   -> Mengonversi '{table_name}' ke CSV dan mengupload ke Google Drive folder ID: {target_folder_id}...")

            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()
            media_bytes = io.BytesIO(csv_content.encode('utf-8'))

            file_metadata = {'name': csv_file_name,
                             'parents': [target_folder_id]}
            media_body = MediaIoBaseUpload(
                media_bytes, mimetype='text/csv', resumable=True)  # mimetype 'text/csv'

            uploaded_file = self.drive_service.files().create(
                body=file_metadata, media_body=media_body, fields='id').execute()
            self.log.info(
                f"     ✅ Berhasil upload '{csv_file_name}' (ID: {uploaded_file.get('id')}) ke Google Drive.")
            return "Success"
        except Exception as e:
            self.log.error(
                f"     ❌ Gagal upload '{table_name}' ke Google Drive: {e}")
            return f"Failed ({type(e).__name__})"

    def _load_df_to_supabase(self, table_name: str, df: pd.DataFrame) -> str:
        """Loads a DataFrame to the specified Supabase table."""
        if self.db_engine is None:
            self.log.error(
                f"   -> ❌ SQLAlchemy engine tidak terinisialisasi. Tidak dapat load '{table_name}' ke Supabase.")
            return "Failed (DB pool not init)"

        if df.empty:
            self.log.warning(
                f"   -> DataFrame '{table_name}' kosong, tidak di-load ke Supabase.")
            return "Skipped (empty)"

        try:
            self.log.info(
                f"   -> Memulai load data ke tabel Supabase '{table_name}': {len(df)} baris.")
            df.to_sql(name=table_name, con=self.db_engine, if_exists='append',
                      index=False, schema='public', method='multi', chunksize=1000)
            self.log.info(
                f"     ✅ Berhasil load {len(df)} baris ke tabel Supabase '{table_name}'.")
            return "Success"
        except Exception as e:
            self.log.error(
                f"     ❌ Gagal load data ke tabel Supabase '{table_name}': {e}")
            return f"Failed ({type(e).__name__})"

    def run(self, ti):
        """Run the loading process"""
        self.log.info("--- Memulai Task Load ---")
        run_id = ti.run_id

        # Inisialisasi service Google Drive
        if self.drive_service is None:
            self.log.info(
                "Drive service belum terautentikasi, menjalankan _authenticate_drive()...")
            self.drive_service = self._authenticate_drive()  # type: ignore

        # Inisialisasi koneksi database
        self._initialize_db_resources()

        try:
            parent_folder_id = Variable.get(GOOGLE_DRIVE_TARGET_FOLDER_ID_VAR)
            self.log.info(f"ID Folder utama Google Drive: {parent_folder_id}")
        except Exception as e:
            self.log.error(
                f"❌ Variabel Airflow '{GOOGLE_DRIVE_TARGET_FOLDER_ID_VAR}' tidak ditemukan atau gagal diambil: {e}")
            raise ValueError(
                f"Variabel Airflow '{GOOGLE_DRIVE_TARGET_FOLDER_ID_VAR}' belum di-set atau tidak valid.")

        # Mengganti karakter ':' dengan '_' agar aman untuk nama folder
        safe_run_id = run_id.replace(":", "_").replace("+", "_")
        run_folder_name = f"airflow_run_{safe_run_id}"

        try:
            run_folder_id = self._create_drive_folder_if_not_exists(
                run_folder_name, parent_folder_id)  # type: ignore
        except Exception as e:
            self.log.error(
                f"❌ Gagal membuat atau mencari folder run '{run_folder_name}' di Google Drive: {e}")
            raise

        # 1. Tarik dictionary path file dari XCom.
        transformed_paths = ti.xcom_pull(
            task_ids=XCOM_VALIDATE_TASK_ID, key=XCOM_FINAL_DATA_PATHS_KEY)
        load_summary_report = {}

        if not transformed_paths or not isinstance(transformed_paths, dict):
            self.log.warning(
                f"⚠️ Tidak ada path file hasil transformasi yang diterima dari XCom (task_ids='{XCOM_VALIDATE_TASK_ID}', key='{XCOM_FINAL_DATA_PATHS_KEY}'). XCom value: {transformed_paths}")
        else:
            self.log.info(f"Menerima path file dari XCom: {transformed_paths}")

            # Tentukan urutan load yang diinginkan, user_terminals pertama
            preferred_load_order = [
                "user_terminals",
                "pelanggans",
                "clusters",
                "home_connecteds",
                "dokumentasis",
                "additional_informations"
            ]
            processed_tables = set()

            # Fungsi untuk memproses satu tabel
            def process_table(table_name, file_path, run_folder_id, summary_report):
                self.log.info(f"--- Memproses tabel: {table_name} ---")
                nonlocal processed_tables

                table_summary = {
                    "rows_processed": 0, "drive_upload_status": "N/A", "supabase_load_status": "N/A"}

                if not file_path:  # Jika path adalah None dari task upstream
                    self.log.warning(
                        f"   -> Path file untuk '{table_name}' adalah None (kosong atau gagal disimpan di task sebelumnya). Dilewati.")
                    table_summary["drive_upload_status"] = "Skipped (No Path)"
                    table_summary["supabase_load_status"] = "Skipped (No Path)"
                # Jika path ada tapi file tidak ditemukan
                elif not os.path.exists(file_path):
                    self.log.warning(
                        f"   -> File untuk '{table_name}' tidak ditemukan di path: {file_path}. Dilewati.")

                try:
                    self.log.info(f"   -> Membaca file Parquet: {file_path}")
                    df_to_load = pd.read_parquet(file_path)
                    if not df_to_load.empty:
                        table_summary["rows_processed"] = len(df_to_load)
                        # Upload ke Google Drive
                        drive_status = self._upload_df_to_drive(
                            table_name, df_to_load, run_folder_id)
                        table_summary["drive_upload_status"] = drive_status

                        # Load ke Supabase
                        supabase_status = self._load_df_to_supabase(
                            table_name, df_to_load)
                        table_summary["supabase_load_status"] = supabase_status
                    else:
                        self.log.info(
                            f"   -> File '{file_path}' (tabel '{table_name}') kosong setelah dibaca. Dilewati.")
                        table_summary["drive_upload_status"] = "Skipped (Empty File)"
                        table_summary["supabase_load_status"] = "Skipped (Empty File)"
                except Exception as e:
                    self.log.error(
                        f"   -> Gagal membaca atau memproses file {file_path} untuk tabel '{table_name}': {e}")
                    table_summary[
                        "drive_upload_status"] = f"Error processing file: {type(e).__name__}"
                    table_summary[
                        "supabase_load_status"] = f"Error processing file: {type(e).__name__}"
                summary_report[table_name] = table_summary
                processed_tables.add(table_name)

            # Proses tabel sesuai urutan prioritas
            for table_name in preferred_load_order:
                if table_name in transformed_paths:
                    file_path = transformed_paths[table_name]
                    process_table(table_name, file_path,
                                  run_folder_id, load_summary_report)

            # Proses sisa tabel yang belum diproses
            for table_name, file_path in transformed_paths.items():
                if table_name not in processed_tables:
                    process_table(table_name, file_path,
                                  run_folder_id, load_summary_report)

        # Pastikan key XCom ini sesuai dengan yang ditarik di task notifikasi email
        ti.xcom_push(key="load_summary_report", value=load_summary_report)
        self.log.info(f"Ringkasan load dikirim ke XCom: {load_summary_report}")
        self.log.info("✅ Task Load selesai.")
