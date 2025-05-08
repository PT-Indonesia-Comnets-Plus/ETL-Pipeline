from airflow.utils.log.logging_mixin import LoggingMixin
import os
import pandas as pd
import io
import json
from airflow.models import Variable  # Diubah dari airflow.sdk
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from sqlalchemy import create_engine, text
from psycopg2 import Error as Psycopg2Error, extras as psycopg2_extras
# Untuk koneksi pool
from main.utils.config_database import initialize_database_connections


class Loader:
    def __init__(self):
        self.log = LoggingMixin().log
        self.db_pool = initialize_database_connections()
        if not self.db_pool:
            self.log.error(
                "Loader tidak mendapatkan database pool. Operasi ke Supabase akan gagal.")
        self.google_drive_client = None  # Nama variabel disesuaikan

    def _authenticate_gdrive(self):
        """Authenticate with Google Drive API."""
        try:
            self.log.info("🔐 Autentikasi Google Drive...")
            creds_json = Variable.get("GOOGLE_CREDENTIALS_JSON")
            creds_dict = json.loads(creds_json)
            # Scope untuk Google Drive (full access)
            scope = ["https://www.googleapis.com/auth/drive"]
            credentials = service_account.Credentials.from_service_account_info(
                creds_dict, scopes=scope)
            client = build('drive', 'v3', credentials=credentials)
            self.log.info("✅ Autentikasi Google Drive berhasil.")
            return client
        except Exception as e:
            self.log.error(f"❌ Autentikasi Google Drive gagal: {e}")
            raise

    def _create_drive_folder_if_not_exists(self, folder_name, parent_folder_id):
        """Mencari atau membuat folder di Google Drive dan mengembalikan ID-nya."""
        query = f"name='{folder_name}' and '{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"  # type: ignore
        response = self.google_drive_client.files().list(  # type: ignore
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
            folder = self.google_drive_client.files().create(  # type: ignore
                body=file_metadata, fields='id').execute()
            self.log.info(
                f"✅ Folder '{folder_name}' dibuat dengan ID: {folder.get('id')}")
            return folder.get('id')

    def load_data(self, table_name, df, target_folder_id):
        """Load the transformed data for a specific table by uploading CSV to Google Drive"""
        # Metode ini sekarang lebih fokus ke GDrive, logika Supabase ada di _load_df_to_supabase_table
        self.log.info(
            f"  -> Memproses data untuk '{table_name}': {len(df)} baris.")

        # --- Logika Upload ke Google Drive ---
        if df.empty:
            self.log.warning(
                f"  -> DataFrame '{table_name}' kosong, tidak diupload ke Google Drive.")
            return

        if self.google_drive_client is None:
            self.google_drive_client = self._authenticate_gdrive()
            if self.google_drive_client is None:
                self.log.error(
                    "Tidak bisa upload ke GDrive, autentikasi gagal.")
                return

        try:
            csv_file_name = f"{table_name}.csv"
            self.log.info(
                f"  -> Mengonversi '{table_name}' ke CSV dan mengupload ke Google Drive folder ID: {target_folder_id}...")

            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()
            media_bytes = io.BytesIO(csv_content.encode('utf-8'))

            file_metadata = {'name': csv_file_name,
                             'parents': [target_folder_id]}
            media_body = MediaIoBaseUpload(
                media_bytes, mimetype='text/csv', resumable=True)
            self.google_drive_client.files().create(  # type: ignore
                body=file_metadata, media_body=media_body, fields='id').execute()
            self.log.info(
                f"     ✅ Berhasil upload '{csv_file_name}' ke Google Drive.")
        except Exception as e:
            self.log.error(
                f"     ❌ Gagal upload '{table_name}' ke Google Drive: {e}")

    def _load_df_to_supabase_table(self, df: pd.DataFrame, table_name: str, primary_keys: list = None):
        """
        Loads a DataFrame into a specified Supabase (PostgreSQL) table using psycopg2.extras.execute_values.
        Handles ON CONFLICT DO UPDATE for tables with primary keys.
        """
        if df.empty:
            self.log.info(
                f"DataFrame untuk tabel {table_name} kosong, tidak ada data untuk dimuat.")
            return 0  # Kembalikan 0 jika tidak ada yang dimuat

        if not self.db_pool:
            self.log.error(
                f"Database pool tidak tersedia. Tidak bisa memuat data ke tabel {table_name}.")
            raise ConnectionError(
                f"Database pool not available for loading to {table_name}")

        conn = None
        rows_affected = 0
        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as cur:
                cols = ",".join(df.columns)
                placeholders = ",".join(["%s"] * len(df.columns))
                data_tuples = [tuple(x) for x in df.to_numpy()]

                insert_query = f"INSERT INTO {table_name} ({cols}) VALUES %s"

                if primary_keys and isinstance(primary_keys, list) and len(primary_keys) > 0:
                    conflict_target = ",".join(primary_keys)
                    update_cols = [
                        col for col in df.columns if col not in primary_keys]
                    if update_cols:
                        set_statement = ", ".join(
                            [f"{col}=EXCLUDED.{col}" for col in update_cols])
                        insert_query += f" ON CONFLICT ({conflict_target}) DO UPDATE SET {set_statement}"
                    else:
                        insert_query += f" ON CONFLICT ({conflict_target}) DO NOTHING"
                else:
                    insert_query += " ON CONFLICT DO NOTHING"

                self.log.info(
                    f"Executing batch insert/upsert for table {table_name} with {len(data_tuples)} rows.")
                self.log.debug(f"Query: {insert_query[:200]}...")

                psycopg2_extras.execute_values(cur, insert_query, data_tuples)
                conn.commit()
                rows_affected = cur.rowcount
                self.log.info(
                    f"Berhasil memuat/memperbarui {rows_affected} baris ke tabel {table_name}.")

        except Psycopg2Error as db_err:
            self.log.error(
                f"Database error saat memuat data ke tabel {table_name}: {db_err}")
            if conn:
                conn.rollback()
            raise
        except Exception as e:
            self.log.error(
                f"Error saat memuat data ke tabel {table_name}: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn and self.db_pool:
                self.db_pool.putconn(conn)
        return rows_affected

    def run_load_to_supabase(self, transformed_data_paths: dict, ti):
        """
        Loads transformed data (split into multiple DataFrames for different tables)
        from Parquet files into Supabase.
        """
        self.log.info(f"--- Memulai Task Load ke Supabase ---")

        if not transformed_data_paths or not isinstance(transformed_data_paths, dict):
            self.log.error(
                "Transformed data paths tidak valid atau tidak ditemukan.")
            raise ValueError("Transformed data paths dictionary is required.")

        load_order = [
            "user_terminals", "clusters", "home_connecteds",
            "dokumentasis", "additional_informations", "pelanggans"
        ]

        load_summary = {}  # Untuk menyimpan ringkasan jumlah baris
        for table_name in load_order:
            file_path = transformed_data_paths.get(table_name)
            if file_path and os.path.exists(file_path):
                try:
                    df_to_load = pd.read_parquet(file_path)
                    self.log.info(
                        f"Membaca data untuk {table_name} dari {file_path} ({len(df_to_load)} baris).")
                    primary_keys = []
                    if table_name == "user_terminals":
                        primary_keys = ["fat_id"]
                    elif table_name == "pelanggans":
                        primary_keys = ["id_permohonan"]

                    rows_loaded = self._load_df_to_supabase_table(
                        df_to_load, table_name, primary_keys=primary_keys)
                    load_summary[table_name] = rows_loaded
                except Exception as e:
                    self.log.error(
                        f"Gagal memproses atau memuat file {file_path} untuk tabel {table_name}: {e}")
                    # Pertimbangkan apakah akan raise error atau melanjutkan dengan tabel lain
            else:
                self.log.warning(
                    f"Tidak ada file data untuk tabel {table_name} atau path tidak valid: {file_path}. Dilewati.")

        # Push ringkasan ke XCom
        if load_summary:
            ti.xcom_push(key="load_summary", value=load_summary)
            self.log.info(f"Ringkasan load dikirim via XCom: {load_summary}")
        self.log.info("--- Selesai Task Load ke Supabase ---@")

    def run(self, **kwargs):
        """Run the loading process"""
        ti = kwargs['ti']
        self.log.info("--- Memulai Task Load ---")

        # --- Autentikasi Google Drive jika diperlukan ---
        if self.google_drive_client is None:
            self.log.info(
                "Drive service belum terautentikasi, menjalankan _authenticate_drive()...")
            self.google_drive_client = self._authenticate_gdrive()

        # Dapatkan ID folder utama dari Airflow Variables
        try:
            parent_folder_id = Variable.get(
                "GOOGLE_DRIVE_TARGET_FOLDER_ID")  # Sesuaikan nama variabel jika perlu
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
                run_folder_name, parent_folder_id)  # type: ignore
        except Exception as e:
            self.log.error(
                f"❌ Gagal membuat atau mencari folder run di Google Drive: {e}")
            raise

        # 1. Tarik dictionary path file dari XCom
        transformed_paths = ti.xcom_pull(
            task_ids="transform_data", key="transformed_data_paths")  # Sesuaikan task_id jika perlu
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
