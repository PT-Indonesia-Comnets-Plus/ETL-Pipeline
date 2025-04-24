import pandas as pd
import numpy as np
from utils.data_cleansing import apply_cleaning, clean_invalid_characters


def clean_coordinate_column(df: pd.DataFrame, coord_col_name: str, latitude_col_name: str, longitude_col_name: str, t_log) -> pd.DataFrame:
    """
    Cleans a combined coordinate column and splits it into separate
    latitude and longitude columns (float). Handles various formats and errors.

    Args:
        df: The DataFrame to process.
        coord_col_name: The name of the source combined coordinate column (string).
        latitude_col_name: The name for the target latitude column (string).
        longitude_col_name: The name for the target longitude column (string).
        t_log: Airflow logger object.

    Returns:
        The DataFrame with added/updated latitude and longitude columns.
        Original coordinate column will be dropped.
        Returns the original DataFrame if the source column is not found or empty/invalid.
    """
    if coord_col_name not in df.columns:
        t_log.warning(
            f"Kolom '{coord_col_name}' tidak ditemukan. Melewatkan cleansing koordinat untuk kolom ini.")
        return df  # Return original DF if source column doesn't exist

    # Work on a copy if df might be a view (prevents SettingWithCopyWarning) - Optional but safer
    # df_copy = df.copy()

    t_log.info(
        f"Memproses kolom '{coord_col_name}' (gabungan Latitude, Longitude)...")

    try:
        # Langkah 1: Apply initial cleaning to the combined coordinate string
        # apply_cleaning harus mengembalikan string atau nilai yang bisa di-astype(str)
        # Kembalikan '' atau None/np.nan untuk nilai yang tidak valid
        # Gunakan .loc[] untuk menghindari SettingWithCopyWarning jika bekerja pada view
        cleaned_coord_col_name = f"{coord_col_name}_cleaned"
        df[cleaned_coord_col_name] = df[coord_col_name].apply(apply_cleaning)
        t_log.info(f"apply_cleaning pada '{coord_col_name}' selesai.")

        # Langkah 2: Split cleaned coordinate string into two new columns
        split_coords = df[cleaned_coord_col_name].str.split(',', expand=True)
        t_log.info(f"Split '{cleaned_coord_col_name}' selesai.")

        # Langkah 3: Tugaskan hasil split ke kolom target latitude dan longitude
        # Inisialisasi kolom target dengan NaN jika belum ada atau untuk menimpa data lama
        df[latitude_col_name] = np.nan
        df[longitude_col_name] = np.nan

        # Tugaskan kolom hasil split jika split_coords memiliki minimal 1 kolom
        if split_coords.shape[1] > 0:
            df[latitude_col_name] = split_coords[0]
            t_log.info(
                f"Kolom 0 hasil split ditugaskan ke {latitude_col_name}.")

        # Tugaskan kolom hasil split jika split_coords memiliki minimal 2 kolom
        if split_coords.shape[1] > 1:
            df[longitude_col_name] = split_coords[1]
            t_log.info(
                f"Kolom 1 hasil split ditugaskan ke {longitude_col_name}.")
        else:
            t_log.warning(
                f"Split tidak menghasilkan 2 kolom untuk '{coord_col_name}', '{longitude_col_name}' mungkin diisi NaN atau tidak berubah.")

        # Langkah 4: Lakukan pembersihan dan konversi akhir pada kolom latitude dan longitude
        t_log.info(
            f"Melakukan pembersihan dan konversi akhir pada '{latitude_col_name}' dan '{longitude_col_name}'.")

        # Pastikan kolom target adalah string untuk operasi selanjutnya
        df[latitude_col_name] = df[latitude_col_name].astype(str)
        df[longitude_col_name] = df[longitude_col_name].astype(str)
        t_log.info("latitude/longitude diubah ke string.")

        # Tangani string representasi null dan strip spasi
        df[latitude_col_name] = df[latitude_col_name].replace(
            ['', '<NA>', 'None'], np.nan).str.strip()
        df[longitude_col_name] = df[longitude_col_name].replace(
            ['', '<NA>', 'None'], np.nan).str.strip()
        t_log.info(
            "String kosong/null representation/spasi di-replace dengan NaN/dihapus.")

        # Apply clean_invalid_characters jika dibutuhkan.
        df[latitude_col_name] = df[latitude_col_name].apply(
            clean_invalid_characters)
        df[longitude_col_name] = df[longitude_col_name].apply(
            clean_invalid_characters)
        t_log.info("clean_invalid_characters selesai pada latitude/longitude.")

        # Final conversion to float, coercing any remaining errors to NaN
        df[latitude_col_name] = pd.to_numeric(
            df[latitude_col_name], errors='coerce')
        df[longitude_col_name] = pd.to_numeric(
            df[longitude_col_name], errors='coerce')
        t_log.info("Konversi akhir ke float selesai pada latitude/longitude.")

        # Drop the intermediate cleaned coordinate column and original source column
        cols_to_drop = [cleaned_coord_col_name, coord_col_name]
        df.drop(
            columns=[col for col in cols_to_drop if col in df.columns], inplace=True)
        t_log.info(
            f"Kolom '{coord_col_name}' dan '{cleaned_coord_col_name}' dihapus.")

        t_log.info(
            f"✅ Cleansing koordinat untuk kolom '{coord_col_name}' selesai.")

    except Exception as e:
        t_log.error(
            f"❌ Error dalam proses cleansing koordinat untuk kolom '{coord_col_name}': {e}")
        import traceback
        traceback.print_exc()
        # Ensure target columns exist even if there was an error
        if latitude_col_name not in df.columns:
            df[latitude_col_name] = np.nan
        if longitude_col_name not in df.columns:
            df[longitude_col_name] = np.nan
        t_log.warning(
            f"⚠️ Kolom {latitude_col_name} dan {longitude_col_name} mungkin tidak terproses sepenuhnya karena error.")
        # Decide whether to re-raise the exception or just log and continue for this specific column
        # raise # Uncomment if failure to clean *any* coordinate column is critical

    return df  # Return the modified DataFrame
