import pandas as pd
import numpy as np
import re
from typing import Any, Optional
from airflow.utils.log.logging_mixin import LoggingMixin

# Gunakan logger khusus untuk utilitas ini jika diperlukan, atau biarkan fungsi menjadi pure
# Untuk kesederhanaan, kita tidak akan membuat instance logger di setiap fungsi di sini,
# tetapi pemanggil (pipeline) dapat melakukan logging sebelum/sesudah memanggil fungsi-fungsi ini.
# Jika logging detail di dalam fungsi ini diperlukan, Anda bisa meneruskan instance logger
# atau membuat logger global untuk modul ini.

coordinate_cleaner_log = LoggingMixin().log


def _clean_comma_separated(coord: Any) -> Optional[str]:
    """Handles standard 'lat,lon', standardizes decimal, removes consecutive dots."""
    if pd.isna(coord):
        return None
    coord_str = str(coord).strip()
    if ',' not in coord_str:
        return None

    parts = [part.strip() for part in coord_str.split(',', 1)]
    if len(parts) == 2:
        lat_part = parts[0].replace(',', '.')
        lon_part = parts[1].replace(',', '.')
        lat_part = re.sub(r'\.+', '.', lat_part).strip('.')
        lon_part = re.sub(r'\.+', '.', lon_part).strip('.')
        try:
            float(lat_part)
            float(lon_part)
            return f"{lat_part},{lon_part}"
        except ValueError:
            return None
    return None


def _clean_degree_as_separator(coord: Any) -> Optional[str]:
    """Handles coordinates separated by '°'."""
    if pd.isna(coord):
        return None
    coord_str = str(coord)

    if '°' not in coord_str:
        return None

    try:
        parts = coord_str.split('°')
        if len(parts) >= 2:
            lat_part = parts[0].strip()
            lon_part = ''.join(parts[1:]).strip()
            return _clean_comma_separated(f"{lat_part},{lon_part}")
    except Exception:
        pass
    return None


def _clean_two_commas_with_space(coord: Any) -> Optional[str]:
    """Handles formats like 'lat, lon' or 'lat  lon' by replacing separators with a single comma."""
    if pd.isna(coord):
        return None
    coord_str = str(coord).strip()
    coord_str_standardized = coord_str.replace(', ', ',').replace(' ', ',')
    return _clean_comma_separated(coord_str_standardized)


def _clean_dot_space_separated(coord: Any) -> Optional[str]:
    """Handles format like '-7.90845. 113.35127' -> '-7.90845,113.35127'."""
    if pd.isna(coord):
        return None
    coord_str = str(coord).strip()
    if not coord_str:
        return None
    match = re.match(r'^(-?\d+\.?\d*)\.\s+(-?\d+\.?\d*)$', coord_str)
    if match:
        lat_part = match.group(1)
        lon_part = match.group(2)
        return f"{lat_part},{lon_part}"
    return None


def _clean_with_e_separator(coord: Any) -> Optional[str]:
    """Handles coordinates separated by 'E', potentially with 'S' prefix and scaling."""
    if pd.isna(coord):
        return None
    coord_str = str(coord).strip()
    if 'E' not in coord_str.upper():
        return None
    try:
        parts = re.split('E', coord_str, maxsplit=1, flags=re.IGNORECASE)
        if len(parts) == 2:
            lat_part = parts[0].strip()
            lon_part = parts[1].strip()
            is_south = False
            if lat_part.upper().startswith('S'):
                is_south = True
                lat_part = lat_part[1:].strip()

            lat_float = float(lat_part)
            lon_float = float(lon_part)

            lat_final = lat_float / 100 if abs(lat_float) > 90 else lat_float
            lon_final = lon_float / 100 if abs(lon_float) > 180 else lon_float

            if is_south:
                lat_final = -abs(lat_final)

            return f"{lat_final},{lon_final}"
    except (ValueError, TypeError, IndexError):
        pass
    return None


def _clean_dot_separated_no_comma(coord: Any) -> Optional[str]:
    """Handles formats like 'X.Y.A.B' -> 'X.Y,A.B' where dot acts as separator."""
    if pd.isna(coord):
        return None
    coord_str = str(coord).strip()
    if ',' in coord_str:
        return None

    match_simple_dot_sep = re.match(r'^(-?\d+\.\d+)\.(\d+\.?\d*)$', coord_str)
    if match_simple_dot_sep:
        lat_part = match_simple_dot_sep.group(1)
        lon_part = match_simple_dot_sep.group(2)
        return _clean_comma_separated(f"{lat_part},{lon_part}")
    return None


def _clean_merged_coordinates(coord: Any) -> Optional[str]:
    """Handles specific merged formats like '-7362714112732918' -> '-7.362714,112.732918'."""
    if pd.isna(coord):
        return None
    coord_str = str(coord).strip().replace(" ", "").replace(",", "")

    match_specific_merged = re.match(
        r'^(-?\d)(\d{6})(\d{3})(\d{6})$', coord_str)
    if match_specific_merged:
        lat_sign, lat_dec, lon_int, lon_dec = match_specific_merged.groups()
        lat = f"{lat_sign}.{lat_dec}"
        lon = f"{lon_int}.{lon_dec}"
        return f"{lat},{lon}"
    return None


def _clean_split_from_long_float(coord: Any) -> Optional[str]:
    """Handles large raw float/int numbers by splitting them based on assumed digit counts."""
    try:
        if not isinstance(coord, (float, int)) or pd.isna(coord):
            return None

        coord_sign = "-" if coord < 0 else ""
        coord_str_raw = "{:.10f}".format(abs(coord)).replace('.', '')
        coord_str = coord_str_raw.rstrip('0')

        if len(coord_str) < 10:  # Heuristic
            return None

        lat_int_digit = coord_str[0]
        lat_dec_digits = coord_str[1:7]
        lon_int_digits = coord_str[7:10]
        lon_dec_digits = coord_str[10:]

        lat = f"{coord_sign}{lat_int_digit}.{lat_dec_digits}"
        lon = f"{lon_int_digits}.{lon_dec_digits}"

        return _clean_comma_separated(f"{lat},{lon}")

    except (IndexError, ValueError, TypeError):
        pass
    return None


def apply_coordinate_cleaning_chain(coord: Any) -> Optional[str]:
    """Applies various cleaning functions in order of precedence."""
    if pd.isna(coord):
        return None

    cleaned = _clean_split_from_long_float(coord)
    if cleaned:
        return cleaned

    coord_str_raw = str(coord)
    coord_str = coord_str_raw.replace('Â', '').replace('\u00A0', ' ').strip()

    if not coord_str or coord_str.lower() in ['none', 'nan', '<na>']:
        return None

    cleaners = [
        _clean_degree_as_separator,
        _clean_two_commas_with_space,
        _clean_dot_space_separated,
        _clean_with_e_separator,
        _clean_dot_separated_no_comma,
        _clean_merged_coordinates,
    ]

    for cleaner_func in cleaners:
        cleaned = cleaner_func(coord_str)
        if cleaned:
            return cleaned

    if ',' in coord_str:  # Final attempt with the standard comma separator cleaner
        return _clean_comma_separated(coord_str)

    return None


def _clean_numeric_coord_part(value: Any) -> Optional[str]:
    """Removes characters not allowed in numeric coordinates (digits, dot, comma, minus)."""
    if pd.isna(value):
        return None
    try:
        cleaned = re.sub(r'[^\d\.,-]', '', str(value))
        return cleaned if cleaned else None
    except Exception:
        return None


def process_coordinate_column_to_lat_lon(df: pd.DataFrame, original_col: str, lat_col: str, lon_col: str) -> pd.DataFrame:
    """
    Cleans, splits, and converts a single coordinate column into separate latitude and longitude columns.
    Modifies the DataFrame in place for the new lat/lon columns and drops the original.
    """
    if original_col not in df.columns:
        coordinate_cleaner_log.warning(
            f"Coordinate Cleaner: Column '{original_col}' not found. Ensuring '{lat_col}' and '{lon_col}' exist as NaN.")
        if lat_col not in df.columns:
            df[lat_col] = np.nan
        if lon_col not in df.columns:
            df[lon_col] = np.nan
        return df

    coordinate_cleaner_log.info(
        f"Coordinate Cleaner: Processing column '{original_col}'...")

    cleaned_coords_series = df[original_col].apply(
        apply_coordinate_cleaning_chain)

    split_data = cleaned_coords_series.str.split(',', expand=True, n=1)

    temp_lat = split_data[0] if split_data.shape[1] > 0 else pd.Series(
        [np.nan] * len(df), index=df.index)
    temp_lon = split_data[1] if split_data.shape[1] > 1 else pd.Series(
        [np.nan] * len(df), index=df.index)

    df[lat_col] = temp_lat.apply(_clean_numeric_coord_part)
    df[lon_col] = temp_lon.apply(_clean_numeric_coord_part)

    df[lat_col] = pd.to_numeric(df[lat_col], errors='coerce')
    df[lon_col] = pd.to_numeric(df[lon_col], errors='coerce')

    coordinate_cleaner_log.info(
        f"Coordinate Cleaner:     ✅ Split '{original_col}' into '{lat_col}' and '{lon_col}', converted to numeric.")

    df.drop(columns=[original_col], inplace=True, errors='ignore')
    coordinate_cleaner_log.info(
        f"Coordinate Cleaner:     ✅ Dropped original column '{original_col}'.")

    return df
