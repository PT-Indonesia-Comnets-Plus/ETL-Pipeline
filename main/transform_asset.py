import os
import pandas as pd
from airflow.utils.log.logging_mixin import LoggingMixin
from main.utils.cleansing_asset import AssetCleansingPipeline

# --- Constants ---
XCOM_EXTRACT_TASK_ID = "extract"
XCOM_ASET_EXTRACTED_PATH_KEY = "aset_data_extracted_path"
XCOM_TRANSFORMED_ASSET_MASTER_PATH_KEY = "transformed_asset_master_path"
XCOM_TRANSFORMED_ASSET_COUNT_KEY = "transformed_asset_count"


class AssetTransformer:
    def __init__(self):
        self.log = LoggingMixin().log

    def read_data(self, ti):
        """Read data from XCom"""
        aset_path = ti.xcom_pull(
            task_ids=XCOM_EXTRACT_TASK_ID, key=XCOM_ASET_EXTRACTED_PATH_KEY)

        if not aset_path:
            self.log.info(
                "ℹ️ Path data aset dari XCom adalah None. Tidak ada data aset baru untuk diproses.")
            return pd.DataFrame()  # Kembalikan DataFrame kosong jika path None

        if not os.path.exists(aset_path):
            self.log.error(f"❌ File aset tidak ditemukan: {aset_path}")
            raise FileNotFoundError(
                f"File aset yang diharapkan tidak ditemukan di path: {aset_path}")

        self.log.info(f"📖 Membaca data aset dari: {aset_path}")
        aset_df = pd.read_parquet(aset_path)

        return aset_df

    def run(self, ti):
        """Run the asset transformation process"""
        self.log.info("--- Memulai Task Transform Aset ---")
        run_id = ti.run_id
        temp_dir = "/opt/airflow/temp"
        os.makedirs(temp_dir, exist_ok=True)

        aset_df = self.read_data(ti)

        if aset_df.empty:
            self.log.warning(
                "ℹ️ DataFrame aset kosong setelah dibaca (kemungkinan tidak ada data aset baru dari extract). Tidak ada transformasi yang akan dilakukan.")
            ti.xcom_push(
                key=XCOM_TRANSFORMED_ASSET_MASTER_PATH_KEY, value=None)
            ti.xcom_push(key=XCOM_TRANSFORMED_ASSET_COUNT_KEY, value=0)
            self.log.info(
                "✅ Task Transform Aset selesai (tidak ada data aset baru untuk diproses).")
            return

        # 2. Jalankan AssetCleansingPipeline pada aset_df
        self.log.info(
            f"🚀 Menjalankan AssetCleansingPipeline pada data aset ({len(aset_df)} baris)...")
        pipeline = AssetCleansingPipeline()

        transformed_asset_master_df = pipeline.run(aset_df)

        if transformed_asset_master_df is None:
            self.log.error(
                "❌ AssetCleansingPipeline gagal atau mengembalikan None.")
            ti.xcom_push(
                key=XCOM_TRANSFORMED_ASSET_MASTER_PATH_KEY, value=None)
            ti.xcom_push(key=XCOM_TRANSFORMED_ASSET_COUNT_KEY, value=0)
            raise ValueError(
                "AssetCleansingPipeline execution failed or returned None.")

        if transformed_asset_master_df.empty:
            self.log.warning(
                "ℹ️ DataFrame aset kosong setelah transformasi oleh AssetCleansingPipeline. Tidak ada perubahan data atau semua data terfilter.")
            ti.xcom_push(
                key=XCOM_TRANSFORMED_ASSET_MASTER_PATH_KEY, value=None)
            ti.xcom_push(key=XCOM_TRANSFORMED_ASSET_COUNT_KEY, value=0)
            self.log.info(
                "✅ Task Transform Aset selesai (hasil transformasi aset kosong).")
            return

        self.log.info(
            "💾 Menyimpan hasil transformasi master aset ke file sementara...")
        file_name = f"aset_master_transformed_{run_id}.parquet"
        file_path = os.path.join(temp_dir, file_name)
        try:
            transformed_asset_master_df.to_parquet(file_path, index=False)
            self.log.info(
                f"  ✅ Disimpan: {file_path} ({len(transformed_asset_master_df)} baris)")
            ti.xcom_push(
                key=XCOM_TRANSFORMED_ASSET_MASTER_PATH_KEY, value=file_path)
            ti.xcom_push(key=XCOM_TRANSFORMED_ASSET_COUNT_KEY,
                         value=len(transformed_asset_master_df))
            self.log.info(
                f"✅ Task Transform Aset selesai. Path file dikirim via XCom: {file_path}")
        except Exception as e:
            self.log.error(f"  ❌ Gagal menyimpan {file_name}: {e}")
            ti.xcom_push(
                key=XCOM_TRANSFORMED_ASSET_MASTER_PATH_KEY, value=None)
            ti.xcom_push(key=XCOM_TRANSFORMED_ASSET_COUNT_KEY, value=0)
            raise
