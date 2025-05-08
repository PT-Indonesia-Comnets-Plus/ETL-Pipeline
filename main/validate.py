import os
import pandas as pd
from airflow.utils.log.logging_mixin import LoggingMixin


class DataValidator:
    def __init__(self):
        self.log = LoggingMixin().log

    def run(self, ti):
        self.log.info("--- Memulai Task Validate and Combine Data ---")
        run_id = ti.run_id
        temp_dir = "/opt/airflow/temp"  # Pastikan konsisten
        os.makedirs(temp_dir, exist_ok=True)

        # 1. Tarik path dari XComs
        # Dictionary path file-file aset dari transform_asset_data
        transformed_asset_paths = ti.xcom_pull(
            task_ids="transform_asset_data", key="transformed_asset_paths")
        # Path file user yang sudah ditransformasi dari transform_user_data
        transformed_user_path = ti.xcom_pull(
            task_ids="transform_user_data", key="transformed_user_path")

        if not transformed_asset_paths:  # Bisa jadi dict kosong jika tidak ada data aset
            self.log.warning(
                "⚠️ Tidak ada path data aset yang ditransformasi diterima. Validasi user mungkin terpengaruh.")
            transformed_asset_paths = {}  # Inisialisasi sebagai dict kosong

        # Siapkan dictionary untuk path final yang akan di-load
        # Mulai dengan semua path aset
        final_data_paths_for_load = transformed_asset_paths.copy()

        # 2. Baca data user_terminals (jika ada) dan data pelanggan (jika ada)
        asset_user_terminals_df = pd.DataFrame()  # Default DataFrame kosong
        user_terminals_path = transformed_asset_paths.get(
            "user_terminals")  # Kunci 'user_terminals' dari AssetPipeline

        if user_terminals_path and os.path.exists(user_terminals_path):
            self.log.info(
                f"📖 Membaca data user_terminals dari: {user_terminals_path}")
            asset_user_terminals_df = pd.read_parquet(user_terminals_path)
        elif "user_terminals" in transformed_asset_paths:  # Path ada di dict tapi file tidak ada
            self.log.warning(
                f"⚠️ File user_terminals '{user_terminals_path}' tidak ditemukan. Validasi FAT ID pelanggan tidak dapat dilakukan.")
        else:  # Kunci 'user_terminals' tidak ada di dict
            self.log.warning(
                "⚠️ Path untuk 'user_terminals' tidak ditemukan dalam transformed_asset_paths. Validasi FAT ID pelanggan tidak dapat dilakukan.")

        pelanggans_df_original = pd.DataFrame()
        if transformed_user_path and os.path.exists(transformed_user_path):
            self.log.info(
                f"📖 Membaca data pelanggan yang ditransformasi dari: {transformed_user_path}")
            pelanggans_df_original = pd.read_parquet(transformed_user_path)
        elif transformed_user_path:  # Path ada tapi file tidak ada
            self.log.warning(
                f"⚠️ File pelanggan '{transformed_user_path}' tidak ditemukan. Tidak ada data pelanggan untuk divalidasi/load.")
        else:  # Path user None
            self.log.info(
                "ℹ️ Tidak ada path data pelanggan yang ditransformasi. Tidak ada data pelanggan untuk divalidasi/load.")

        # 3. Lakukan validasi silang jika kedua DataFrame ada dan tidak kosong
        pelanggans_df_validated = pd.DataFrame()
        if not pelanggans_df_original.empty:
            if not asset_user_terminals_df.empty and 'fat_id' in asset_user_terminals_df.columns and 'fat_id' in pelanggans_df_original.columns:
                valid_fat_ids_from_assets = set(
                    asset_user_terminals_df['fat_id'].unique())
                self.log.info(
                    f"  -> {len(valid_fat_ids_from_assets)} FAT ID unik dari data aset (user_terminals) akan digunakan untuk validasi pelanggan.")

                original_pelanggan_count = len(pelanggans_df_original)
                # Filter pelanggans_df_original berdasarkan FAT ID yang valid
                pelanggans_df_validated = pelanggans_df_original[pelanggans_df_original['fat_id'].isin(
                    valid_fat_ids_from_assets)].copy()
                invalid_fat_id_count = original_pelanggan_count - \
                    len(pelanggans_df_validated)

                if invalid_fat_id_count > 0:
                    self.log.warning(
                        f"  -> {invalid_fat_id_count} baris data pelanggan memiliki FAT ID yang tidak ditemukan di data aset dan telah dihapus.")
                self.log.info(
                    f"  -> Data pelanggan setelah validasi FAT ID: {len(pelanggans_df_validated)} baris.")
            elif asset_user_terminals_df.empty:
                self.log.warning(
                    "  -> Tidak dapat validasi FAT ID pelanggan karena data aset (user_terminals) kosong.")
                pelanggans_df_validated = pd.DataFrame(
                    columns=pelanggans_df_original.columns)  # Hasilnya kosong
            elif 'fat_id' not in pelanggans_df_original.columns:
                self.log.warning(
                    "  -> Kolom 'fat_id' tidak ada di data pelanggan. Tidak dapat validasi.")
                # Lewatkan validasi jika kolom tidak ada
                pelanggans_df_validated = pelanggans_df_original.copy()
            else:  # 'fat_id' not in asset_user_terminals_df.columns
                self.log.warning(
                    "  -> Tidak dapat validasi FAT ID pelanggan karena kolom 'fat_id' tidak ada di data aset (user_terminals).")
                pelanggans_df_validated = pd.DataFrame(
                    columns=pelanggans_df_original.columns)  # Hasilnya kosong
        else:
            self.log.info(
                "ℹ️ DataFrame pelanggan asli kosong, tidak ada validasi FAT ID yang dilakukan.")
            # pelanggans_df_validated tetap DataFrame kosong

        # 4. Simpan data pelanggan yang sudah divalidasi (jika ada)
        # Kunci ini harus konsisten dengan yang diharapkan oleh Loader
        pelanggan_table_key_for_load = "pelanggans"

        if not pelanggans_df_validated.empty:
            # Pastikan 'id_permohonan' unik sebagai PK
            if 'id_permohonan' in pelanggans_df_validated.columns:
                pelanggans_df_validated.drop_duplicates(
                    subset=['id_permohonan'], keep='first', inplace=True)
                self.log.info(
                    f"  -> Data pelanggan valid setelah deduplikasi 'id_permohonan': {len(pelanggans_df_validated)} baris.")

            pelanggan_validated_file_name = f"{pelanggan_table_key_for_load}_validated_{run_id}.parquet"
            pelanggan_validated_path = os.path.join(
                temp_dir, pelanggan_validated_file_name)
            try:
                pelanggans_df_validated.to_parquet(
                    pelanggan_validated_path, index=False)
                # Tambahkan/Update path pelanggan di dictionary final
                final_data_paths_for_load[pelanggan_table_key_for_load] = pelanggan_validated_path
                self.log.info(
                    f"  -> ✅ Disimpan (pelanggan validated): {pelanggan_validated_path} ({len(pelanggans_df_validated)} baris)")
            except Exception as e:
                self.log.error(
                    f"  -> ❌ Gagal menyimpan data pelanggan yang divalidasi ({pelanggan_validated_file_name}): {e}")
        elif pelanggan_table_key_for_load in final_data_paths_for_load:
            # Jika pelanggan_df_validated kosong, hapus entri lama dari transformed_asset_paths jika ada
            # Ini untuk kasus dimana 'pelanggans' mungkin ada di transformed_asset_paths (seharusnya tidak)
            # atau jika kita mau memastikan hanya data valid yang diteruskan.
            self.log.warning(
                f"⚠️ Data pelanggan (tabel '{pelanggan_table_key_for_load}') kosong setelah validasi. Path lama (jika ada) tidak akan digunakan.")
            if pelanggan_table_key_for_load in final_data_paths_for_load:
                del final_data_paths_for_load[pelanggan_table_key_for_load]

        # 5. Push dictionary path final ke XCom
        ti.xcom_push(key="final_data_paths_for_load",
                     value=final_data_paths_for_load)
        self.log.info(
            f"✅ Task Validate and Combine Data selesai. Path file akhir dikirim via XCom: {final_data_paths_for_load}")
