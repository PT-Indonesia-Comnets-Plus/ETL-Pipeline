import os
import pandas as pd
from airflow.utils.log.logging_mixin import LoggingMixin
# Import UserCleansingPipeline
from main.utils.cleansing_user import UserCleansingPipeline

# --- Constants ---
XCOM_EXTRACT_TASK_ID = "extract"
XCOM_USER_EXTRACTED_PATH_KEY = "user_data_extracted_path"
XCOM_TRANSFORMED_USER_PATH_KEY = "transformed_user_path"
XCOM_TRANSFORMED_USER_COUNT_KEY = "transformed_user_count"


class UserTransformer:
    def __init__(self):
        """Initializes the UserTransformer with a logger."""
        self.log = LoggingMixin().log

    def read_data(self, ti):
        """
        Reads user data path from XCom, loads the Parquet file into a DataFrame.
        Returns an empty DataFrame if the path is None or the file doesn't exist.
        """
        user_path = ti.xcom_pull(
            task_ids=XCOM_EXTRACT_TASK_ID, key=XCOM_USER_EXTRACTED_PATH_KEY)

        if not user_path:
            self.log.info(
                "ℹ️ Path data user dari XCom adalah None. Tidak ada data user baru untuk diproses.")
            return pd.DataFrame()

        if not os.path.exists(user_path):
            self.log.error(f"❌ File user tidak ditemukan: {user_path}")
            raise FileNotFoundError(
                f"File user yang diharapkan tidak ditemukan di path: {user_path}")

        self.log.info(f"📖 Membaca data user dari: {user_path}")
        try:
            user_df = pd.read_parquet(user_path)
            return user_df
        except Exception as e:
            self.log.error(
                f"❌ Gagal membaca file Parquet user di {user_path}: {e}")
            raise  # Re-raise exception to fail the task

    def transform_user_data(self, user_df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies transformations to the user DataFrame.
        Uses UserCleansingPipeline for cleaning.
        """
        if user_df.empty:
            self.log.info(
                "ℹ️ DataFrame user kosong, tidak ada transformasi yang dilakukan.")
            return user_df

        self.log.info(
            f"🚀 Memulai transformasi pada data user ({len(user_df)} baris)...")

        pipeline = UserCleansingPipeline()
        # UserCleansingPipeline.run() returns a dict like {"user": df}
        cleaned_data_dict = pipeline.run(user_df.copy())

        if not cleaned_data_dict or "user" not in cleaned_data_dict:
            self.log.error(
                "❌ UserCleansingPipeline tidak mengembalikan hasil yang diharapkan atau key 'user' tidak ditemukan.")
            # Return empty DataFrame or raise error, depending on desired behavior
            return pd.DataFrame()

        transformed_df = cleaned_data_dict["user"]
        self.log.info(
            f"✅ Transformasi data user selesai. Jumlah baris setelah transformasi: {len(transformed_df)}")
        return transformed_df

    def run(self, ti):
        """
        Orchestrates the user data transformation process.
        Reads data, applies transformations, saves the result, and pushes path to XCom.
        """
        self.log.info("--- Memulai Task Transform User ---")
        run_id = ti.run_id
        temp_dir = "/opt/airflow/temp"
        os.makedirs(temp_dir, exist_ok=True)

        user_df = self.read_data(ti)

        if user_df.empty:
            self.log.info(
                "ℹ️ DataFrame user kosong setelah dibaca (kemungkinan tidak ada data user baru dari extract). Tidak ada proses transformasi atau penyimpanan yang akan dilakukan.")
            ti.xcom_push(key=XCOM_TRANSFORMED_USER_PATH_KEY, value=None)
            ti.xcom_push(key=XCOM_TRANSFORMED_USER_COUNT_KEY, value=0)
            self.log.info(
                "✅ Task Transform User selesai (tidak ada data user baru untuk diproses).")
            return

        transformed_user_df = self.transform_user_data(user_df)

        if transformed_user_df.empty:
            self.log.warning(
                "ℹ️ DataFrame user kosong setelah proses transformasi. Tidak ada data user untuk disimpan.")
            ti.xcom_push(key=XCOM_TRANSFORMED_USER_PATH_KEY, value=None)
            ti.xcom_push(key=XCOM_TRANSFORMED_USER_COUNT_KEY, value=0)
            self.log.info(
                "✅ Task Transform User selesai (hasil transformasi user kosong).")
            return

        self.log.info(
            "💾 Menyimpan hasil transformasi user ke file sementara...")
        file_name = f"user_transformed_{run_id}.parquet"
        file_path = os.path.join(temp_dir, file_name)
        try:
            transformed_user_df.to_parquet(file_path, index=False)
            self.log.info(
                f"  ✅ Disimpan: {file_path} ({len(transformed_user_df)} baris)")
            ti.xcom_push(key=XCOM_TRANSFORMED_USER_PATH_KEY, value=file_path)
            ti.xcom_push(key=XCOM_TRANSFORMED_USER_COUNT_KEY,
                         value=len(transformed_user_df))
            self.log.info(
                f"✅ Task Transform User selesai. Path file dikirim via XCom: {file_path}")
        except Exception as e:
            self.log.error(f"  ❌ Gagal menyimpan {file_name}: {e}")
            ti.xcom_push(key=XCOM_TRANSFORMED_USER_PATH_KEY, value=None)
            ti.xcom_push(key=XCOM_TRANSFORMED_USER_COUNT_KEY, value=0)
            raise
