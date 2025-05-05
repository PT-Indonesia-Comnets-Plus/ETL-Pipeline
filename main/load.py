from airflow.utils.log.logging_mixin import LoggingMixin
import os
import pandas as pd
import io
import json
from airflow.sdk import Variable  # Gunakan SDK Variable
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload


class Loader:
    def __init__(self):
        self.log = LoggingMixin().log
        self.drive_service = None  # Inisialisasi service sebagai None

    def _authenticate_drive(self):
        """Authenticate with Google Drive API. Dipanggil saat run()."""
        try:
            self.log.info("🔐 Autentikasi Google Drive...")
            creds_json = Variable.get("GOOGLE_CREDENTIALS_JSON")
            creds_dict = json.loads(creds_json)
            # Scope untuk Google Drive (full access)
            scope = ["https://www.googleapis.com/auth/drive"]
            credentials = service_account.Credentials.from_service_account_info(
                creds_dict, scopes=scope)
            # Build the Drive v3 service
            service = build('drive', 'v3', credentials=credentials)
            self.log.info("✅ Autentikasi Google Drive berhasil.")
            return service
        except Exception as e:
            self.log.error(f"❌ Autentikasi Google Drive gagal: {e}")
            raise

    def _create_drive_folder_if_not_exists(self, folder_name, parent_folder_id):
        """Mencari atau membuat folder di Google Drive dan mengembalikan ID-nya."""
        query = f"name='{folder_name}' and '{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        response = self.drive_service.files().list(
            q=query, spaces='drive', fields='files(id, name)').execute()
        folders = response.get('files', [])

        if folders:
            self.log.info(
                f"Folder '{folder_name}' sudah ada dengan ID: {folders[0]['id']}")
            return folders[0]['id']
        else:
            self.log.info(
                f"Membuat folder '{folder_name}' di dalam folder ID: {parent_folder_id}")
            file_metadata = {
                'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_folder_id]}
            folder = self.drive_service.files().create(
                body=file_metadata, fields='id').execute()
            self.log.info(
                f"✅ Folder '{folder_name}' dibuat dengan ID: {folder.get('id')}")
            return folder.get('id')

    def load_data(self, table_name, df, target_folder_id):
        """Load the transformed data for a specific table by uploading CSV to Google Drive"""
        self.log.info(
            f"  -> Memproses data untuk '{table_name}': {len(df)} baris.")
        # --- Ganti dengan logika load sebenarnya ---
        # Contoh: Load ke database Supabase/Postgres
        # from airflow.providers.postgres.hooks.postgres import PostgresHook
        # try:
        #     hook = PostgresHook(postgres_conn_id='your_supabase_conn_id') # Ganti dengan ID koneksi kamu
        #     engine = hook.get_sqlalchemy_engine()
        #     # Sesuaikan nama tabel jika perlu (misal, dari key dict)
        #     db_table_name = table_name # Atau mapping lain
        #     df.to_sql(db_table_name, engine, if_exists='append', index=False, chunksize=1000) # Gunakan chunksize
        #     self.log.info(f"     ✅ Berhasil load ke tabel '{db_table_name}'.")
        # except Exception as e:
        #     self.log.error(f"     ❌ Gagal load ke tabel '{db_table_name}': {e}")

        # --- Logika Upload ke Google Drive ---
        if df.empty:
            self.log.warning(
                f"  -> DataFrame '{table_name}' kosong, tidak diupload ke Google Drive.")
            return

        try:
            csv_file_name = f"{table_name}.csv"
            self.log.info(
                f"  -> Mengonversi '{table_name}' ke CSV dan mengupload ke Google Drive folder ID: {target_folder_id}...")

            # Buat CSV dalam memory
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()
            media = io.BytesIO(csv_content.encode('utf-8'))  # Encode ke bytes

            file_metadata = {'name': csv_file_name,
                             'parents': [target_folder_id]}
            media_body = MediaIoBaseUpload(
                media, mimetype='text/csv', resumable=True)
            self.drive_service.files().create(
                body=file_metadata, media_body=media_body, fields='id').execute()
            self.log.info(
                f"     ✅ Berhasil upload '{csv_file_name}' ke Google Drive.")
        except Exception as e:
            self.log.error(
                f"     ❌ Gagal upload '{table_name}' ke Google Drive: {e}")
        # -----------------------------------------

    def run(self, **kwargs):
        """Run the loading process"""
        ti = kwargs['ti']
        self.log.info("--- Memulai Task Load ---")

        # --- Pindahkan autentikasi ke sini ---
        if self.drive_service is None:
            self.log.info(
                "Drive service belum terautentikasi, menjalankan _authenticate_drive()...")
            self.drive_service = self._authenticate_drive()
        # ------------------------------------

        # Dapatkan ID folder utama dari Airflow Variables
        try:
            parent_folder_id = Variable.get(
                "GOOGLE_CREDENTIALS_TARGET_FOLDER_ID")
        except KeyError:
            self.log.error(
                "❌ Variabel Airflow 'GOOGLE_DRIVE_TARGET_FOLDER_ID' tidak ditemukan!")
            raise ValueError(
                "Variabel Airflow 'GOOGLE_DRIVE_TARGET_FOLDER_ID' belum di-set.")

        # Buat subfolder unik untuk run DAG ini
        run_id = ti.run_id
        run_folder_name = f"airflow_run_{run_id}"
        try:
            run_folder_id = self._create_drive_folder_if_not_exists(
                run_folder_name, parent_folder_id)
        except Exception as e:
            self.log.error(
                f"❌ Gagal membuat atau mencari folder run di Google Drive: {e}")
            raise

        # 1. Tarik dictionary path file dari XCom
        transformed_paths = ti.xcom_pull(
            task_ids="transform", key="transformed_data_paths")
        if not transformed_paths or not isinstance(transformed_paths, dict):
            self.log.warning(
                "⚠️ Tidak ada path file hasil transformasi yang diterima dari XCom.")
        else:
            self.log.info(f"Menerima path file: {transformed_paths}")
            # 2. Iterasi dan load setiap file
            for table_name, file_path in transformed_paths.items():
                if not file_path or not os.path.exists(file_path):
                    self.log.warning(
                        f"  -> File untuk '{table_name}' tidak ditemukan di path: {file_path}. Dilewati.")
                    continue

                try:
                    self.log.info(f"  -> Membaca file: {file_path}")
                    df_to_load = pd.read_parquet(file_path)
                    if not df_to_load.empty:
                        # Panggil load_data dengan nama tabel, df, dan ID folder run
                        self.load_data(table_name, df_to_load, run_folder_id)
                    else:
                        self.log.info(
                            f"  -> File '{file_path}' kosong. Dilewati.")
                except Exception as e:
                    self.log.error(
                        f"  -> Gagal membaca atau memproses file {file_path}: {e}")

        self.log.info("✅ Task Load selesai.")
