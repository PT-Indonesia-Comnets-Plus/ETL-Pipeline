import os
import pandas as pd
from airflow.utils.log.logging_mixin import LoggingMixin
# Pastikan ini sesuai dengan struktur folder kamu
from main.utils.cleansing import AssetPipeline


class Transformer:
    def __init__(self):
        self.log = LoggingMixin().log

    def read_data(self, ti):
        """Read data from XCom"""
        aset_path = ti.xcom_pull(task_ids="extract", key="aset_data_path")
        user_path = ti.xcom_pull(task_ids="extract", key="user_data_path")

        if not aset_path or not os.path.exists(aset_path):
            self.log.error(f"❌ File aset tidak ditemukan: {aset_path}")
            raise FileNotFoundError(aset_path)
        if not user_path or not os.path.exists(user_path):
            self.log.error(f"❌ File user tidak ditemukan: {user_path}")
            raise FileNotFoundError(user_path)

        self.log.info(f"📖 Membaca data aset dari: {aset_path}")
        aset_df = pd.read_parquet(aset_path)

        self.log.info(f"📖 Membaca data user dari: {user_path}")
        user_df = pd.read_parquet(user_path)

        return aset_df, user_df

    # Metode transform lama tidak lagi relevan dalam bentuk ini
    # def transform(self, aset_df, user_df):
    #     """Transform the extracted data"""
    #     self.log.info("🔄 Memulai proses transformasi...")
    #     total_rows = len(aset_df) + len(user_df)
    #     self.log.info(f"✅ Total baris gabungan: {total_rows}")
    #     return total_rows

    def run(self, ti):
        """Run the transformation process"""
        self.log.info("--- Memulai Task Transform ---")
        run_id = ti.run_id
        temp_dir = "/opt/airflow/temp"  # Pastikan konsisten dengan extract.py

        # 1. Baca data dari file sementara
        aset_df, user_df = self.read_data(ti)

        # 2. Jalankan AssetPipeline pada aset_df
        self.log.info("🚀 Menjalankan Asset Pipeline pada data aset...")
        pipeline = AssetPipeline()
        # run() sekarang mengembalikan dict
        split_asset_dfs = pipeline.run(aset_df)

        if split_asset_dfs is None:
            self.log.error("❌ Asset Pipeline gagal.")
            raise ValueError("Asset Pipeline execution failed.")

        # 3. Simpan hasil split ke file Parquet baru
        transformed_paths = {}
        self.log.info(
            "💾 Menyimpan hasil transformasi aset ke file sementara...")
        for table_name, df_split in split_asset_dfs.items():
            if not df_split.empty:
                # Tambahkan run_id untuk keunikan
                file_name = f"{table_name}_{run_id}.parquet"
                file_path = os.path.join(temp_dir, file_name)
                try:
                    df_split.to_parquet(file_path, index=False)
                    transformed_paths[table_name] = file_path
                    self.log.info(
                        f"  -> ✅ Disimpan: {file_path} ({len(df_split)} baris)")
                except Exception as e:
                    self.log.error(f"  -> ❌ Gagal menyimpan {file_name}: {e}")
                    # Pertimbangkan apakah mau raise error atau lanjut

        # 4. (Opsional) Simpan user_df jika perlu diteruskan
        user_transformed_path = os.path.join(
            temp_dir, f"user_data_transformed_{run_id}.parquet")
        try:
            user_df.to_parquet(user_transformed_path, index=False)
            # Tambahkan ke dict path
            transformed_paths["user_data"] = user_transformed_path
            self.log.info(
                f"  -> ✅ Disimpan: {user_transformed_path} ({len(user_df)} baris)")
        except Exception as e:
            self.log.error(
                f"  -> ❌ Gagal menyimpan user_data_transformed: {e}")

        # 5. Push dictionary path ke XCom
        if transformed_paths:
            ti.xcom_push("transformed_data_paths", transformed_paths)
            self.log.info(
                f"✅ Task Transform selesai. Path file dikirim via XCom: {transformed_paths}")
        else:
            self.log.warning(
                "⚠️ Tidak ada file hasil transformasi yang disimpan.")

        # Tidak perlu return value eksplisit jika pakai xcom_push
        # total_rows = self.transform(aset_df, user_df) # <-- Hapus ini
        # return total_rows # <-- Hapus ini
