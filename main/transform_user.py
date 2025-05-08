import os
import pandas as pd
from airflow.utils.log.logging_mixin import LoggingMixin
# Pastikan ini sesuai dengan struktur folder kamu
from main.utils.cleansing_user import UserTransformer


class Transformer:
    def __init__(self):
        self.log = LoggingMixin().log

    def read_data(self, ti):
        """Read data from XCom"""
        # Menggunakan key yang benar dari task extract
        user_path = ti.xcom_pull(
            task_ids="extract_data", key="user_data_new_path")

        if not user_path or not os.path.exists(user_path):
            self.log.error(f"❌ File user tidak ditemukan: {user_path}")
            raise FileNotFoundError(user_path)
        self.log.info(f"✅ Path data user diterima dari XCom: {user_path}")

        self.log.info(f"📖 Membaca data user dari: {user_path}")
        user_df = pd.read_parquet(user_path)

        return user_df

    def run(self, ti):
        """Run the transformation process"""
        self.log.info("--- Memulai Task Transform User ---")
        run_id = ti.run_id
        temp_dir = "/opt/airflow/temp"  # Pastikan konsisten dengan extract.py
        os.makedirs(temp_dir, exist_ok=True)

        # 1. Baca data dari file sementara
        user_df = self.read_data(ti)

        if user_df.empty:
            self.log.warning(
                "⚠️ DataFrame user kosong setelah dibaca. Tidak ada transformasi yang akan dilakukan.")
            # Push None atau path ke file kosong jika diperlukan oleh downstream task
            # Untuk saat ini, kita akan push None jika tidak ada data
            ti.xcom_push(key="transformed_user_path", value=None)
            self.log.info(
                "✅ Task Transform User selesai (tidak ada data untuk diproses).")
            return

        # 2. Jalankan UserTransformer pada user_df
        self.log.info(
            f"🚀 Menjalankan UserTransformer pada data user ({len(user_df)} baris)...")
        pipeline = UserTransformer()

        # UserTransformer.run() mengembalikan dict {"user": df}
        transformed_user_data_dict = pipeline.run(user_df)

        if not transformed_user_data_dict or "user" not in transformed_user_data_dict:
            self.log.error(
                "❌ UserTransformer tidak mengembalikan hasil yang diharapkan atau dictionary kosong.")
            # Push None jika transformasi gagal atau tidak menghasilkan data
            ti.xcom_push(key="transformed_user_path", value=None)
            raise ValueError(
                "UserTransformer execution failed or returned unexpected data.")

        transformed_user_df = transformed_user_data_dict["user"]

        if transformed_user_df.empty:
            self.log.warning(
                "⚠️ DataFrame user kosong setelah transformasi oleh UserTransformer.")
            ti.xcom_push(key="transformed_user_path", value=None)
            self.log.info(
                "✅ Task Transform User selesai (hasil transformasi kosong).")
            return

        # 3. Simpan hasil transformasi ke file Parquet baru
        self.log.info(
            "💾 Menyimpan hasil transformasi user ke file sementara...")
        # Nama file konsisten dengan yang mungkin diharapkan atau untuk kejelasan
        file_name = f"pelanggans_transformed_{run_id}.parquet"
        file_path = os.path.join(temp_dir, file_name)
        try:
            transformed_user_df.to_parquet(file_path, index=False)
            self.log.info(
                f"  -> ✅ Disimpan: {file_path} ({len(transformed_user_df)} baris)")
            # 4. Push path file ke XCom
            ti.xcom_push(key="transformed_user_path", value=file_path)
            self.log.info(
                f"✅ Task Transform User selesai. Path file dikirim via XCom: {file_path}")
        except Exception as e:
            self.log.error(f"  -> ❌ Gagal menyimpan {file_name}: {e}")
            # Push None jika gagal simpan
            ti.xcom_push(key="transformed_user_path", value=None)
            raise  # Re-raise exception agar task gagal
