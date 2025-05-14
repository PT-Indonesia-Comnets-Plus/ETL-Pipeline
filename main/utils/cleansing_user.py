
from airflow.utils.log.logging_mixin import LoggingMixin
import pandas as pd
import numpy as np
import re
# Import shared coordinate cleaning function
from .coordinate_cleaner import process_coordinate_column_to_lat_lon


class UserCleansingPipeline:
    """
    Cleanses user data by removing duplicates and irrelevant information.
    """

    def __init__(self):
        self.log = LoggingMixin().log
        self.column_rename_map = {
            "SID": "sid",
            "ID Permohonan": "id_permohonan",
            "Koodinat Pelanggan": "koordinat_pelanggan",
            "Cust Name": "cust_name",
            "Telpn": "telpn",
            "ID FAT": "fat_id",
            "NOTES": "notes"
        }
        self.columns_to_drop = [
            'Koordinat FAT',
            'Hostname OLT',
            'FDT'
        ]

        self.colom_capitalize = [
            'cust_name',
            'notes'
        ]

    def _drop_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drops irrelevant columns."""
        self.log.info("  UserPipeline Step: Dropping irrelevant columns...")
        return df.drop(columns=self.columns_to_drop, errors="ignore") if not df.empty else df

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Renames columns based on predefined mappings."""
        self.log.info("  UserPipeline Step: Renaming columns...")
        return df.rename(columns=self.column_rename_map, errors="ignore") if not df.empty else df

    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Removes duplicate rows based on 'id_permohonan'."""
        self.log.info(
            "  UserPipeline Step: Removing duplicates based on 'id_permohonan'...")
        return df.drop_duplicates(subset=['id_permohonan']) if not df.empty else df

    def _capitalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Capitalizes specific columns."""
        self.log.info("  UserPipeline Step: Capitalizing specific columns...")
        for col in self.colom_capitalize:
            if col in df.columns:
                df[col] = df[col].str.capitalize()
        return df if not df.empty else df

    def _process_coordinate_column(self, df: pd.DataFrame, base_name: str) -> pd.DataFrame:
        """Cleans, splits, and converts a single coordinate column (e.g., 'koordinat_olt')."""
        original_col = f"koordinat_{base_name}"
        lat_col = f"latitude_{base_name}"
        lon_col = f"longitude_{base_name}"

        # Use the shared utility function
        return process_coordinate_column_to_lat_lon(df, original_col, lat_col, lon_col)

    def run(self, df: pd.DataFrame) -> dict:
        """Runs the cleansing pipeline on the provided DataFrame."""
        if df.empty:
            self.log.info(
                "Input DataFrame untuk UserTransformer kosong. Mengembalikan dictionary kosong.")
            return {}
        self.log.info(
            f"Running User Cleansing Pipeline... Input: {len(df)} baris.")
        df = self._drop_columns(df)
        df = self._rename_columns(df)
        df = self._remove_duplicates(df)
        self.log.info(
            f"  UserPipeline: DataFrame setelah _remove_duplicates: {len(df)} baris.")
        df = self._capitalize_columns(df)
        df = self._process_coordinate_column(df, 'pelanggan')
        self.log.info(
            f"  UserPipeline: DataFrame setelah _process_coordinate_column: {len(df)} baris.")

        # Return the cleaned DataFrame as a dictionary with a single key
        self.log.info(
            f"User Cleansing Pipeline selesai. Output: {len(df)} baris.")
        return {"user": df} if not df.empty else {}
