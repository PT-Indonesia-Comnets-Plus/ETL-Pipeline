from airflow.utils.log.logging_mixin import LoggingMixin
import os
import pandas as pd
import io
import json
from airflow.models import Variable  # Cara import Variable yang lebih umum
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload


class Loader:
    def __init__(self):
        self.log = LoggingMixin().log
        self.drive_service = None

    def _authenticate_drive(self):
        """Authenticate with Google Drive API. Dipanggil saat run()."""
        try:
            self.log.info("🔐 Autentikasi Google Drive...")
            # Pastikan nama Variable ini sesuai dengan yang Anda set di Airflow UI
            creds_json = Variable.get("Google_Credentials_Key")
            creds_dict = json.loads(creds_json)
            scope = ["https://www.googleapis.com/auth/drive"]
            credentials = service_account.Credentials.from_service_account_info(
                creds_dict, scopes=scope)
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
            f"   -> Memproses data untuk '{table_name}': {len(df)} baris.")

        if df.empty:
            self.log.warning(
                f"   -> DataFrame '{table_name}' kosong, tidak diupload ke Google Drive.")
            return

        try:
            # Anda menyimpan Parquet, tapi upload CSV. Jika ingin upload Parquet, sesuaikan.
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

            # Jika ingin upload Parquet asli:
            # parquet_buffer = io.BytesIO()
            # df.to_parquet(parquet_buffer, index=False)
            # parquet_buffer.seek(0) # Penting untuk BytesIO
            # media_bytes = parquet_buffer
            # file_metadata = {'name': f"{table_name}.parquet", 'parents': [target_folder_id]}
            # media_body = MediaIoBaseUpload(media_bytes, mimetype='application/vnd.apache.parquet', resumable=True) # atau 'application/octet-stream'

            uploaded_file = self.drive_service.files().create(
                body=file_metadata, media_body=media_body, fields='id').execute()
            self.log.info(
                f"     ✅ Berhasil upload '{csv_file_name}' (ID: {uploaded_file.get('id')}) ke Google Drive.")
        except Exception as e:
            self.log.error(
                f"     ❌ Gagal upload '{table_name}' ke Google Drive: {e}")

    def run(self, ti):
        """Run the loading process"""
        temp_dir = "/opt/airflow/temp"  # Tidak digunakan jika membaca langsung dari XCom path
        # Mungkin tidak perlu jika tidak ada penyimpanan sementara di task ini
        os.makedirs(temp_dir, exist_ok=True)
        self.log.info("--- Memulai Task Load ---")

        if self.drive_service is None:
            self.log.info(
                "Drive service belum terautentikasi, menjalankan _authenticate_drive()...")
            self.drive_service = self._authenticate_drive()

        try:
            # Pastikan nama Variable ini KONSISTEN dengan yang Anda set di Airflow UI
            parent_folder_id = Variable.get(
                "GOOGLE_CREDENTIALS_TARGET_FOLDER_ID")
            self.log.info(f"ID Folder utama Google Drive: {parent_folder_id}")
        except KeyError:
            self.log.error(
                "❌ Variabel Airflow 'GOOGLE_CREDENTIALS_TARGET_FOLDER_ID' tidak ditemukan!")
            raise ValueError(
                "Variabel Airflow 'GOOGLE_CREDENTIALS_TARGET_FOLDER_ID' belum di-set.")

        run_id = ti.run_id
        # Mengganti karakter ':' dengan '_' agar aman untuk nama folder
        safe_run_id = run_id.replace(":", "_").replace("+", "_")
        run_folder_name = f"airflow_run_{safe_run_id}"

        try:
            run_folder_id = self._create_drive_folder_if_not_exists(
                run_folder_name, parent_folder_id)
        except Exception as e:
            self.log.error(
                f"❌ Gagal membuat atau mencari folder run '{run_folder_name}' di Google Drive: {e}")
            raise

        # 1. Tarik dictionary path file dari XCom.
        # PASTIKAN task_ids adalah ID task upstream yang benar
        # PERBAIKAN UTAMA DI SINI:
        transformed_paths = ti.xcom_pull(
            task_ids="validate_and_spliting_data", key="final_data_paths_for_load")

        if not transformed_paths or not isinstance(transformed_paths, dict):
            self.log.warning(
                f"⚠️ Tidak ada path file hasil transformasi yang diterima dari XCom (key='final_data_paths_for_load', task_ids='validate_and_spliting_data'). XCom value: {transformed_paths}")
        else:
            self.log.info(f"Menerima path file dari XCom: {transformed_paths}")
            # 2. Iterasi dan load setiap file
            for table_name, file_path in transformed_paths.items():
                if not file_path:  # Jika path adalah None dari task upstream
                    self.log.warning(
                        f"   -> Path file untuk '{table_name}' adalah None (kosong atau gagal disimpan di task sebelumnya). Dilewati.")
                    continue

                # Jika path ada tapi file tidak ditemukan
                if not os.path.exists(file_path):
                    self.log.warning(
                        f"   -> File untuk '{table_name}' tidak ditemukan di path: {file_path}. Dilewati.")
                    continue

                try:
                    self.log.info(f"   -> Membaca file Parquet: {file_path}")
                    df_to_load = pd.read_parquet(file_path)
                    if not df_to_load.empty:
                        self.load_data(table_name, df_to_load, run_folder_id)
                    else:
                        self.log.info(
                            f"   -> File '{file_path}' (tabel '{table_name}') kosong setelah dibaca. Dilewati.")
                except Exception as e:
                    self.log.error(
                        f"   -> Gagal membaca atau memproses file {file_path} untuk tabel '{table_name}': {e}")

        self.log.info("✅ Task Load selesai.")
