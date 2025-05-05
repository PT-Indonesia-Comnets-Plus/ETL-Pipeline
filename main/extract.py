import os
import pandas as pd
from airflow.utils.log.logging_mixin import LoggingMixin
from google.oauth2 import service_account
import gspread
from airflow.sdk import Variable
import json
import re


class Extractor:
    def __init__(self):
        self.log = LoggingMixin().log
        self.temp_dir = "/opt/airflow/temp"
        os.makedirs(self.temp_dir, exist_ok=True)
        self.client = None  # Inisialisasi client sebagai None

    def _authenticate(self):
        """Authenticate with Google Sheets API. Dipanggil saat run()."""
        try:
            self.log.info("🔐 Autentikasi Google Sheets...")
            creds_json = Variable.get("GOOGLE_CREDENTIALS_JSON")
            creds_dict = json.loads(creds_json)
            scope = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]
            credentials = service_account.Credentials.from_service_account_info(
                creds_dict, scopes=scope)
            client = gspread.authorize(credentials)
            self.log.info("✅ Autentikasi berhasil.")
            return client
        except Exception as e:
            self.log.error(f"❌ Autentikasi gagal: {e}")
            raise

    def load_sheet(self, spreadsheet_id, sheet_name):
        """Load data from a specific sheet in Google Sheets"""
        try:
            sheet = self.client.open_by_key(
                spreadsheet_id).worksheet(sheet_name)
            data = sheet.get_all_values()
            if not data:
                return pd.DataFrame()
            if len(data) == 1:
                return pd.DataFrame(columns=data[0])
            return pd.DataFrame(data[1:], columns=data[0])
        except Exception as e:
            self.log.error(f"❌ Gagal memuat sheet {sheet_name}: {e}")
            raise

    def run(self, ti):
        """Run the extraction process"""
        # --- Pindahkan autentikasi ke sini ---
        if self.client is None:
            self.log.info(
                "Client belum terautentikasi, menjalankan _authenticate()...")
            self.client = self._authenticate()
        # ------------------------------------
        aset_id = Variable.get("ASET_SPREADSHEET_ID")
        user_id = Variable.get("USER_SPREADSHEET_ID")

        aset_df = self.load_sheet(aset_id, "Datek Aset All")
        user_df = self.load_sheet(user_id, "Data All")

        if aset_df.empty or user_df.empty:
            self.log.error("❌ Salah satu data kosong.")
            raise ValueError("Data tidak boleh kosong.")

        if aset_df.empty:
            return aset_df
        print("  Pipeline Step: Cleaning column names...")

        target_col_name = "Status OSP AMARTA"  # Assuming this is still relevant
        new_cols = []
        count = 1

        for col in aset_df.columns:
            current_col_name = col
            if col == target_col_name:
                current_col_name = f"{target_col_name} {count}"
                count += 1
            cleaned_name = re.sub(r"\s+", " ", str(current_col_name)).strip()
            new_cols.append(cleaned_name)

        aset_df.columns = new_cols

        aset_path = os.path.join(self.temp_dir, f"aset_data.parquet")
        user_path = os.path.join(self.temp_dir, f"user_data.parquet")

        aset_df.to_parquet(aset_path, index=False)
        user_df.to_parquet(user_path, index=False)

        self.log.info(f"📁 Aset disimpan di: {aset_path}")
        self.log.info(f"📁 User disimpan di: {user_path}")

        ti.xcom_push("aset_data_path", aset_path)
        ti.xcom_push("user_data_path", user_path)
