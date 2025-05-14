from psycopg2 import pool, Error as Psycopg2Error
from typing import List, Dict, Any, Optional, Tuple
import numpy as np
import re
import re
import pandas as pd
from airflow.utils.log.logging_mixin import LoggingMixin  # Ditambahkan
from .coordinate_cleaner import process_coordinate_column_to_lat_lon  # Ditambahkan


class AssetCleansingPipeline:
    """
    A class to encapsulate the asset data cleaning and preparation pipeline.
    Handles column renaming, capitalization, duplicate removal, missing value filling, type conversions, and data cleaning.
    Finally, splits the processed data into multiple DataFrames corresponding to target database tables.
    """

    def __init__(self):
        """Initializes the pipeline configuration with column settings."""
        self.log = LoggingMixin().log  # Ditambahkan
        self.exclude_columns = {
            "Hostname OLT", "FDT ID", "FATID", "Type OLT", "OLT", "ID FAT",
            "CLEANSING HP", "FAT ID X", "LINK DOKUMEN FEEDER", "LINK DATA ASET", "LINK MAPS"
        }
        # Target column name for duplicate handling during column name cleaning

        self.column_rename_map = {
            "Hostname OLT": "hostname_olt",
            "Kordinat OLT": "koordinat_olt",
            "Brand OLT": "brand_olt",
            "Type OLT": "type_olt",
            "Kapasitas OLT": "kapasitas_olt",
            "Kapasitas port OLT": "kapasitas_port_olt",
            "OLT Port": "olt_port",
            "OLT": "olt",
            "Interface OLT": "interface_olt",
            "Lokasi OLT": "lokasi_olt",
            "FDT ID": "fdt_id",
            "Status OSP AMARTA 1": "status_osp_amarta_fdt",
            "Jumlah Splitter FDT": "jumlah_splitter_fdt",
            "Kapasitas Splitter FDT": "kapasitas_splitter_fdt",
            "FDT New/Existing": "fdt_new_existing",
            "Port FDT": "port_fdt",
            "Koodinat FDT": "koordinat_fdt",
            "FATID": "fat_id",
            "Jumlah Splitter FAT": "jumlah_splitter_fat",
            "Kapasitas Splitter FAT": "kapasitas_splitter_fat",
            "Koodinat FAT": "koordinat_fat",
            "Status OSP AMARTA FAT": "status_osp_amarta_fat",
            "FAT KONDISI": "fat_kondisi",
            "FAT FILTER PEMAKAIAN": "fat_filter_pemakaian",
            "KETERANGAN FULL": "keterangan_full",
            "FAT ID X": "fat_id_x",
            "FILTER FAT CAP": "filter_fat_cap",
            "Cluster": "cluster",
            "Koordinat Cluster": "koordinat_cluster",
            "Area KP": "area_kp",
            "Kota/Kab": "kota_kab",
            "Kecamatan": "kecamatan",
            "Kelurahan": "kelurahan",
            "UP3": "up3",
            "ULP": "ulp",
            "LINK DOKUMEN FEEDER": "link_dokumen_feeder",
            "KETERANGAN DOKUMEN": "keterangan_dokumen",
            "LINK DATA ASET": "link_data_aset",
            "KETERANGAN DATA ASET": "keterangan_data_aset",
            "LINK MAPS": "link_maps",
            "UPDATE ASET": "update_aset",
            "AMARTA UPDATE": "amarta_update",
            "HC OLD": "hc_old",
            "HC iCRM+": "hc_icrm",
            "TOTAL HC": "total_hc",
            "CLEANSING HP": "cleansing_hp",
            "PA": "pa",
            "Tanggal RFS": "tanggal_rfs",
            "Mitra": "mitra",
            "Kategori": "kategori",
            "Sumber Datek": "sumber_datek"
        }

        self.astype_map = {
            "kapasitas_olt": "Int64",
            "kapasitas_port_olt": "Int64",
            "olt_port": "Int64",
            "jumlah_splitter_fdt": "Int64",
            "kapasitas_splitter_fdt": "Int64",
            "port_fdt": "Int64",
            "jumlah_splitter_fat": "Int64",
            "kapasitas_splitter_fat": "Int64",
            "hc_old": "Int64",
            "hc_icrm": "Int64",
            "total_hc": "Int64",
            "tanggal_rfs": "datetime64[ns]"
        }

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Renames columns based on predefined mappings."""
        self.log.info("  AssetPipeline Step: Renaming columns...")
        return df.rename(columns=self.column_rename_map, errors="ignore") if not df.empty else df

    @staticmethod
    def fill_na_values(df: pd.DataFrame) -> pd.DataFrame:
        """Fills missing values with default values for numerical columns."""
        if df.empty:
            AssetCleansingPipeline.get_logger().info(
                "  AssetPipeline Step: Skipping NA fill (empty DataFrame).")
            return df

        default_fill = {
            "Jumlah Splitter FDT": 0,
            "Kapasitas Splitter FDT": 0,
            "Jumlah Splitter FAT": 0,
            "Kapasitas Splitter FAT": 0,
            "Kapasitas OLT": 0,
            "Kapasitas port OLT": 0,
            "OLT Port": 0,
            "Port FDT": 0,
            "HC OLD": 0,
            "HC iCRM+": 0,
            "TOTAL HC": 0
        }

        AssetCleansingPipeline.get_logger().info(
            "  AssetPipeline Step: Filling NA values...")
        df.fillna(default_fill, inplace=True)
        return df

    # --- Moved inside the class ---
    def clean_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans column names: strip, remove extra spaces, handle 'Status OSP AMARTA' duplicates."""
        if df.empty:
            return df
        self.log.info("  AssetPipeline Step: Cleaning column names...")

        target_col_name = "Status OSP AMARTA"  # Assuming this is still relevant
        new_cols = []
        count = 1

        for col in df.columns:
            current_col_name = col
            if col == target_col_name:
                current_col_name = f"{target_col_name} {count}"
                count += 1
            cleaned_name = re.sub(r"\s+", " ", str(current_col_name)).strip()
            new_cols.append(cleaned_name)

        df.columns = new_cols
        return df

    def capitalize_columns_except(self, df: pd.DataFrame) -> pd.DataFrame:
        """Capitalizes string columns except those specified in `exclude_columns`."""
        if df.empty:
            return df
        self.log.info("  AssetPipeline Step: Capitalizing string values...")
        for col in df.columns:
            if col not in self.exclude_columns and df[col].dtype == "object":
                df[col] = df[col].apply(
                    lambda x: x.title().strip() if isinstance(x, str) else x)
        return df
    # -----------------------------

    def clean_column_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans specific known problematic values in columns."""
        self.log.info(
            "  AssetPipeline Step: Cleaning specific column values (e.g., tanggal_rfs)...")
        if "tanggal_rfs" in df.columns:
            df["tanggal_rfs"] = df["tanggal_rfs"].astype(
                str).str.replace("0203", "2023", regex=False)
        return df

    def _convert_column_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Converts column types based on predefined mappings."""
        if df.empty:
            return df

        int64_cols_map = {col: dtype for col,
                          dtype in self.astype_map.items() if dtype == "Int64"}
        datetime_cols_map = {
            col: dtype for col, dtype in self.astype_map.items() if dtype == "datetime64[ns]"}
        # Kolom yang tersisa untuk konversi tipe data umum
        other_cols_map = {
            col: dtype for col, dtype in self.astype_map.items()
            if dtype not in ["Int64", "datetime64[ns]"]
        }

        self.log.info(
            "  AssetPipeline Step: Converting column types (Int64)...")
        for col in int64_cols_map:
            if col in df.columns:
                # Pastikan kolom adalah string sebelum operasi string replace
                df[col] = df[col].astype(str).str.replace(
                    "[., ]", "", regex=True).str.strip()
                df[col] = pd.to_numeric(
                    df[col], errors="coerce").astype("Int64")

        self.log.info(
            "  AssetPipeline Step: Converting column types (Datetime)...")
        for col in datetime_cols_map:
            if col in df.columns:
                # Metode clean_column_values seharusnya sudah membersihkan dan mengubah ke string
                # Peringatan mengindikasikan format dd-mm-yyyy, jadi dayfirst=True
                df[col] = pd.to_datetime(
                    df[col], errors="coerce", dayfirst=True)
                # Jika format spesifik diketahui dan lebih andal, gunakan:
                # df[col] = pd.to_datetime(df[col], errors="coerce", format='%d-%m-%Y')

        self.log.info(
            "  AssetPipeline Step: Converting column types (Others)...")
        # Filter other_cols_map untuk kolom yang benar-benar ada di df
        actual_other_cols_to_convert = {
            col: dtype for col, dtype in other_cols_map.items() if col in df.columns
        }
        if actual_other_cols_to_convert:
            df = df.astype(actual_other_cols_to_convert, errors="ignore")

        return df

    def _process_coordinate_column(self, df: pd.DataFrame, base_name: str) -> pd.DataFrame:
        """Cleans, splits, and converts a single coordinate column (e.g., 'koordinat_olt')."""
        original_col = f"koordinat_{base_name}"
        lat_col = f"latitude_{base_name}"
        lon_col = f"longitude_{base_name}"

        # Use the shared utility function
        return process_coordinate_column_to_lat_lon(df, original_col, lat_col, lon_col)

    def _process_all_coordinates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Processes all standard coordinate columns (OLT, FDT, FAT, Cluster)."""
        self.log.info(
            "  AssetPipeline Step: Processing all coordinate columns...")
        coordinate_bases = ["olt", "fdt", "fat", "cluster"]
        for base in coordinate_bases:
            df = self._process_coordinate_column(df, base)
        self.log.info(
            "  AssetPipeline Step: ✅ All coordinate processing finished.")
        return df

    # --- Main Pipeline Execution ---

    def run(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Runs the complete asset data cleaning and processing pipeline,
        returning a single processed DataFrame before splitting.

        Args:
            df: The raw input pandas DataFrame.

        Returns:
            A single processed pandas DataFrame containing all cleaned data,
            Returns None if a critical error occurs during processing.
        """
        if df.empty:
            self.log.info(
                "Input DataFrame for AssetPipeline is empty. Skipping.")
            return pd.DataFrame()  # Return empty DataFrame for consistency

        try:
            self.log.info("Starting Asset Cleansing Pipeline...")
            df = self.clean_column_names(df)  # Step 1 <-- Now called correctly
            df = self.capitalize_columns_except(df)  # Step 2
            df = self._rename_columns(df)  # Step 3
            df = self.fill_na_values(df)  # Step 4
            df = self.clean_column_values(df)  # Step 5
            df = self._process_all_coordinates(
                df)  # Step 6: Process coordinates
            # Step 7: Convert types AFTER coordinates are split
            df = self._convert_column_types(df)

            self.log.info("Asset Cleansing Pipeline finished successfully.")
            return df
        except Exception as e:
            self.log.error(
                f"ERROR during AssetCleansingPipeline execution: {e}", exc_info=True)
            return None  # Return None on critical pipeline error
