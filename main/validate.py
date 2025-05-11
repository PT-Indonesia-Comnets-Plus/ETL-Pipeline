import os
import pandas as pd
from airflow.utils.log.logging_mixin import LoggingMixin
from typing import List, Dict, Any, Optional, Tuple


class DataValidator:
    def __init__(self):
        """Initializes the DataValidator with a logger."""
        self.log = LoggingMixin().log

    def split_data(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Splits the processed asset DataFrame into multiple DataFrames,
        each corresponding to a target database table schema.
        """
        self.log.info("--- Splitting Asset Data for Target Tables ---")
        if df.empty:
            self.log.warning(
                "Input Asset DataFrame is empty. Returning empty dictionary for asset splits.")
            return {}

        self.log.info(
            f"  -> Splitting asset data with {len(df)} rows.")

        split_dfs = {}
        table_definitions = {
            "user_terminals": [
                "hostname_olt", "latitude_olt", "longitude_olt", "brand_olt", "type_olt",
                "kapasitas_olt", "kapasitas_port_olt", "olt_port", "olt", "interface_olt",
                "fdt_id", "status_osp_amarta_fdt", "jumlah_splitter_fdt", "kapasitas_splitter_fdt",
                "fdt_new_existing", "port_fdt", "latitude_fdt", "longitude_fdt",
                "fat_id", "jumlah_splitter_fat", "kapasitas_splitter_fat", "latitude_fat", "longitude_fat",
                "status_osp_amarta_fat", "fat_kondisi", "fat_filter_pemakaian", "keterangan_full",
                "fat_id_x", "filter_fat_cap"
            ],
            "clusters": ["latitude_cluster", "longitude_cluster", "area_kp", "kota_kab", "kecamatan", "kelurahan", "up3", "ulp", "fat_id"],
            "home_connecteds": ["hc_old", "hc_icrm", "total_hc", "cleansing_hp", "fat_id"],
            "dokumentasis": ["status_osp_amarta_fat", "link_dokumen_feeder", "keterangan_dokumen", "link_data_aset", "keterangan_data_aset", "link_maps", "update_aset", "amarta_update", "fat_id"],
            "additional_informations": ["pa", "tanggal_rfs", "mitra", "kategori", "sumber_datek", "fat_id"]
        }

        for table_name, columns in table_definitions.items():
            available_cols = [col for col in columns if col in df.columns]
            if 'fat_id' not in available_cols and 'fat_id' in df.columns:
                available_cols.append('fat_id')
            if not available_cols or not any(col in df.columns for col in available_cols):
                self.log.warning(
                    f"  -> No columns available or defined columns not in DataFrame for asset table '{table_name}'. Skipping.")
                split_dfs[table_name] = pd.DataFrame()
                continue

            split_df = df[available_cols].copy()
            split_dfs[table_name] = split_df
            self.log.info(
                f"  ✅ Split asset data for '{table_name}' ({len(split_df)} rows, {len(available_cols)} columns).")

        self.log.info("✅ Asset data splitting finished.")
        return split_dfs

    def run(self, ti):
        """
        Orchestrates the data validation and splitting process.
        It retrieves transformed data paths from XCom, loads the data,
        splits the asset data, prepares user data, saves all resulting
        DataFrames to Parquet files, and pushes their paths to XCom for the Loader task.
        """
        self.log.info("--- Memulai Task Validate and Split Data ---")
        run_id = ti.run_id
        temp_dir = "/opt/airflow/temp"
        os.makedirs(temp_dir, exist_ok=True)

        transformed_asset_master_path = ti.xcom_pull(
            task_ids="transform_asset_data", key="transformed_asset_master_path")
        transformed_user_path = ti.xcom_pull(
            task_ids="transform_user_data", key="transformed_user_path")

        asset_master_df = pd.DataFrame()
        if transformed_asset_master_path and os.path.exists(transformed_asset_master_path):
            self.log.info(
                f"📖 Membaca data master aset yang sudah ditransformasi dari: {transformed_asset_master_path}")
            asset_master_df = pd.read_parquet(transformed_asset_master_path)
            self.log.info(
                f"  -> Data master aset dimuat: {len(asset_master_df)} baris.")
        else:
            self.log.warning(
                f"⚠️ File master aset yang sudah ditransformasi tidak ditemukan di: {transformed_asset_master_path}. Tidak ada data aset untuk diproses.")

        user_df = pd.DataFrame()
        if transformed_user_path and os.path.exists(transformed_user_path):
            self.log.info(
                f"📖 Membaca data user yang sudah ditransformasi dari: {transformed_user_path}")
            user_df = pd.read_parquet(transformed_user_path)
            self.log.info(f"  -> Data user dimuat: {len(user_df)} baris.")
        else:
            self.log.warning(
                f"⚠️ File user yang sudah ditransformasi tidak ditemukan di: {transformed_user_path}. Tidak ada data user untuk diproses.")

        final_data_for_load = {}

        if not asset_master_df.empty:
            split_asset_dfs = self.split_data(asset_master_df)
            for table_name, df_split in split_asset_dfs.items():
                if not df_split.empty:
                    final_data_for_load[table_name] = df_split
                else:
                    self.log.info(
                        f"DataFrame from asset split for table '{table_name}' is empty.")
        else:
            self.log.info(
                "Asset master DataFrame is empty, no asset data splitting performed.")

        if not user_df.empty:
            self.log.info(
                f"Preparing data for 'pelanggans' table from user_df ({len(user_df)} rows).")
            final_data_for_load["pelanggans"] = user_df
        else:
            self.log.info(
                "User DataFrame is empty, no data for 'pelanggans' table.")

        final_data_paths_for_load = {}
        if not final_data_for_load:
            self.log.warning(
                "Tidak ada data yang disiapkan untuk di-load. XCom 'final_data_paths_for_load' akan kosong.")
        else:
            self.log.info(
                "Saving DataFrames intended for loading to Parquet files...")
            for table_name, df_to_save in final_data_for_load.items():
                if not df_to_save.empty:
                    file_name = f"{table_name}_validated_{run_id}.parquet"
                    file_path = os.path.join(temp_dir, file_name)
                    try:
                        df_to_save.to_parquet(file_path, index=False)
                        self.log.info(
                            f"  -> ✅ Disimpan: {file_path} ({len(df_to_save)} baris untuk tabel '{table_name}')")
                        final_data_paths_for_load[table_name] = file_path
                    except Exception as e:
                        self.log.error(
                            f"  -> ❌ Gagal menyimpan {file_name} untuk tabel '{table_name}': {e}")
                        final_data_paths_for_load[table_name] = None
                else:
                    self.log.info(
                        f"DataFrame for table '{table_name}' is empty, not saved.")
                    final_data_paths_for_load[table_name] = None

        ti.xcom_push(key="final_data_paths_for_load",
                     value=final_data_paths_for_load)
        self.log.info(
            f"✅ Task Validate and Split Data finished. File paths pushed to XCom: {final_data_paths_for_load}")
