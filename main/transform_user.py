import os
import pandas as pd
from airflow.utils.log.logging_mixin import LoggingMixin


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
            task_ids="extract_data", key="user_data_new_path")

        if not user_path:
            self.log.info(
                "ℹ️ Path data user dari XCom adalah None. Tidak ada data user baru untuk diproses.")
            return pd.DataFrame()

        if not os.path.exists(user_path):
            self.log.error(f"❌ File user tidak ditemukan: {user_path}")
            # Mengembalikan DataFrame kosong agar task bisa selesai dengan baik jika diinginkan,
            # atau bisa juga raise FileNotFoundError jika ini kondisi kritis.
            # Untuk konsistensi dengan transform_asset.py, kita bisa raise error.
            raise FileNotFoundError(
                f"File user yang diharapkan tidak ditemukan di path: {user_path}")

        self.log.info(f"📖 Membaca data user dari: {user_path}")
        try:
            user_df = pd.read_parquet(user_path)
            return user_df
        except Exception as e:
            self.log.error(
                f"❌ Gagal membaca file Parquet user di {user_path}: {e}")
            # Kembalikan DataFrame kosong atau raise error tergantung kebutuhan
            return pd.DataFrame()

    def transform_user_data(self, user_df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies transformations to the user DataFrame.
        Placeholder for any user-specific cleaning or transformation logic.
        """
        if user_df.empty:
            self.log.info(
                "ℹ️ DataFrame user kosong, tidak ada transformasi yang dilakukan.")
            return user_df

        self.log.info(
            f"🚀 Memulai transformasi pada data user ({len(user_df)} baris)...")

        # --- Placeholder untuk UserCleansingPipeline atau logika transformasi lainnya ---
        # Contoh jika ada UserCleansingPipeline:
        # pipeline = UserCleansingPipeline()
        # transformed_user_df = pipeline.run(user_df.copy()) # Gunakan .copy() jika pipeline memodifikasi inplace

        # Untuk saat ini, kita asumsikan tidak ada transformasi tambahan di sini,
        # jadi kita kembalikan DataFrame apa adanya.
        transformed_user_df = user_df.copy()
        self.log.info(
            "✅ Transformasi data user selesai (atau dilewati jika tidak ada logika spesifik).")
        # ---------------------------------------------------------------------------

        return transformed_user_df

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
            ti.xcom_push(key="transformed_user_path", value=None)
            self.log.info(
                "✅ Task Transform User selesai (tidak ada data user baru untuk diproses).")
            return

        transformed_user_df = self.transform_user_data(user_df)

        if transformed_user_df.empty:
            self.log.warning(
                "ℹ️ DataFrame user kosong setelah proses transformasi. Tidak ada data user untuk disimpan.")
            ti.xcom_push(key="transformed_user_path", value=None)
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
                f"  -> ✅ Disimpan: {file_path} ({len(transformed_user_df)} baris)")
            ti.xcom_push(key="transformed_user_path", value=file_path)
            self.log.info(
                f"✅ Task Transform User selesai. Path file dikirim via XCom: {file_path}")
        except Exception as e:
            self.log.error(f"  -> ❌ Gagal menyimpan {file_name}: {e}")
            ti.xcom_push(key="transformed_user_path", value=None)
            raise
