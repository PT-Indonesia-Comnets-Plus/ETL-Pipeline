
from airflow.utils.log.logging_mixin import LoggingMixin
import pandas as pd
import numpy as np
import re
from typing import List, Dict, Any, Optional, Tuple


class UserTransformer:
    """
    Cleanses user data by removing duplicates and irrelevant information.
    """

    def __init__(self):
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
        print("  Pipeline Step: Dropping irrelevant columns...")
        return df.drop(columns=self.columns_to_drop, errors="ignore") if not df.empty else df

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Renames columns based on predefined mappings."""
        print("  Pipeline Step: Renaming columns...")
        return df.rename(columns=self.column_rename_map, errors="ignore") if not df.empty else df

    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Removes duplicate rows based on 'id_permohonan'."""
        print("  Pipeline Step: Removing duplicates...")
        return df.drop_duplicates(subset=['id_permohonan']) if not df.empty else df

    def _capitalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Capitalizes specific columns."""
        print("  Pipeline Step: Capitalizing specific columns...")
        for col in self.colom_capitalize:
            if col in df.columns:
                df[col] = df[col].str.capitalize()
        return df if not df.empty else df

        # --- Coordinate Cleaning Methods ---

    def _clean_comma_separated(self, coord: Any) -> Optional[str]:
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

    def _clean_degree_as_separator(self, coord: Any) -> Optional[str]:
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
                return self._clean_comma_separated(f"{lat_part},{lon_part}")
        except Exception:
            pass
        return None

    def _clean_two_commas_with_space(self, coord: Any) -> Optional[str]:
        """Handles formats like 'lat, lon' or 'lat  lon' by replacing separators with a single comma."""
        if pd.isna(coord):
            return None
        coord_str = str(coord).strip()
        # Replace space-comma or multiple spaces with a single comma
        coord_str_standardized = coord_str.replace(', ', ',').replace(' ', ',')
        return self._clean_comma_separated(coord_str_standardized)

    def _clean_dot_space_separated(self, coord: Any) -> Optional[str]:
        """Handles format like '-7.90845. 113.35127' -> '-7.90845,113.35127'."""
        if pd.isna(coord):
            return None
        coord_str = str(coord).strip()
        if not coord_str:
            return None
        # Regex to match number, dot, space(s), number
        match = re.match(r'^(-?\d+\.?\d*)\.\s+(-?\d+\.?\d*)$', coord_str)
        if match:
            lat_part = match.group(1)
            lon_part = match.group(2)
            return f"{lat_part},{lon_part}"  # Already clean format
        return None

    def _clean_with_e_separator(self, coord: Any) -> Optional[str]:
        """Handles coordinates separated by 'E', potentially with 'S' prefix and scaling."""
        if pd.isna(coord):
            return None
        coord_str = str(coord).strip()
        if 'E' not in coord_str.upper():
            return None  # Case-insensitive check
        try:
            parts = re.split('E', coord_str, maxsplit=1, flags=re.IGNORECASE)
            if len(parts) == 2:
                lat_part = parts[0].strip()
                lon_part = parts[1].strip()
                is_south = False
                if lat_part.upper().startswith('S'):
                    is_south = True
                    lat_part = lat_part[1:].strip()  # Remove 'S'

                lat_float = float(lat_part)
                lon_float = float(lon_part)

                # Apply scaling logic (adjust thresholds if needed)
                lat_final = lat_float / \
                    100 if abs(lat_float) > 90 else lat_float
                lon_final = lon_float / \
                    100 if abs(lon_float) > 180 else lon_float

                if is_south:
                    lat_final = -abs(lat_final)  # Ensure negative for South

                return f"{lat_final},{lon_final}"
        except (ValueError, TypeError, IndexError):
            pass  # Ignore conversion/split errors
        return None

    def _clean_dot_separated_no_comma(self, coord: Any) -> Optional[str]:
        """Handles formats like 'X.Y.A.B' -> 'X.Y,A.B' where dot acts as separator."""
        if pd.isna(coord):
            return None
        coord_str = str(coord).strip()
        if ',' in coord_str:
            return None  # Skip if already has comma

        # Try matching pattern like -7.12345.112.67890
        match_simple_dot_sep = re.match(
            r'^(-?\d+\.\d+)\.(\d+\.?\d*)$', coord_str)
        if match_simple_dot_sep:
            lat_part = match_simple_dot_sep.group(1)
            lon_part = match_simple_dot_sep.group(2)
            # Pass through standard comma cleaner for final validation
            return self._clean_comma_separated(f"{lat_part},{lon_part}")
        return None

    def _clean_merged_coordinates(self, coord: Any) -> Optional[str]:
        """Handles specific merged formats like '-7362714112732918' -> '-7.362714,112.732918'."""
        if pd.isna(coord):
            return None
        coord_str = str(coord).strip().replace(" ", "").replace(",", "")

        # Example pattern: -7 followed by 6 digits, then 112 followed by 6 digits
        match_specific_merged = re.match(
            r'^(-?\d)(\d{6})(\d{3})(\d{6})$', coord_str)
        if match_specific_merged:
            lat_sign = match_specific_merged.group(1)
            lat_dec = match_specific_merged.group(2)
            lon_int = match_specific_merged.group(3)
            lon_dec = match_specific_merged.group(4)
            lat = f"{lat_sign}.{lat_dec}"
            lon = f"{lon_int}.{lon_dec}"
            return f"{lat},{lon}"
        return None

    def _clean_split_from_long_float(self, coord: Any) -> Optional[str]:
        """Handles large raw float/int numbers by splitting them based on assumed digit counts."""
        try:
            if not isinstance(coord, (float, int)) or pd.isna(coord):
                return None

            # Format, remove sign and decimal for splitting, keep original sign
            coord_sign = "-" if coord < 0 else ""
            coord_str_raw = "{:.10f}".format(abs(coord)).replace(
                '.', '')  # Use absolute value
            coord_str = coord_str_raw.rstrip('0')

            # Heuristic: Check if length suggests a merged lat/lon
            if len(coord_str) < 10:
                return None  # Too short

            # Example split logic (adjust indices based on typical data)
            # Assumes: 1 digit int lat, 6 digits dec lat, 3 digits int lon, rest dec lon
            lat_int_digit = coord_str[0]
            lat_dec_digits = coord_str[1:7]
            lon_int_digits = coord_str[7:10]
            lon_dec_digits = coord_str[10:]

            lat = f"{coord_sign}{lat_int_digit}.{lat_dec_digits}"
            lon = f"{lon_int_digits}.{lon_dec_digits}"

            # Validate and return using the standard comma cleaner
            return self._clean_comma_separated(f"{lat},{lon}")

        except (IndexError, ValueError, TypeError):
            pass  # Ignore errors during splitting/conversion
        return None

    def _apply_coordinate_cleaning(self, coord: Any) -> Optional[str]:
        """Applies various cleaning functions in order of precedence."""
        if pd.isna(coord):
            return None

        # Try cleaning float first if applicable
        cleaned = self._clean_split_from_long_float(coord)
        if cleaned:
            return cleaned

        # Process as string
        coord_str_raw = str(coord)
        # Basic cleanup of common problematic characters
        coord_str = coord_str_raw.replace(
            'Â', '').replace('\u00A0', ' ').strip()

        if not coord_str or coord_str.lower() in ['none', 'nan', '<na>']:
            return None

        # Apply string-based cleaners in order
        cleaned = (
            self._clean_degree_as_separator(coord_str) or
            self._clean_two_commas_with_space(coord_str) or
            self._clean_dot_space_separated(coord_str) or
            self._clean_with_e_separator(coord_str) or
            self._clean_dot_separated_no_comma(coord_str) or
            self._clean_merged_coordinates(coord_str)
            # Add other specific cleaners here if needed
        )
        if cleaned:
            return cleaned

        # Final attempt with the standard comma separator cleaner
        if ',' in coord_str:
            return self._clean_comma_separated(coord_str)

        return None  # Return None if no method could clean it

    def _clean_invalid_characters(self, value: Any) -> Optional[str]:
        """Removes characters not allowed in numeric coordinates (digits, dot, comma, minus)."""
        if pd.isna(value):
            return None
        try:
            # Keep only digits, dot, comma, minus sign
            cleaned = re.sub(r'[^\d\.,-]', '', str(value))
            return cleaned if cleaned else None  # Return None if empty after cleaning
        except Exception:
            return None

    def _process_coordinate_column(self, df: pd.DataFrame, base_name: str) -> pd.DataFrame:
        """Cleans, splits, and converts a single coordinate column (e.g., 'koordinat_olt')."""
        original_col = f"koordinat_{base_name}"
        lat_col = f"latitude_{base_name}"
        lon_col = f"longitude_{base_name}"

        if original_col not in df.columns:
            print(
                f"  -> Column '{original_col}' not found. Skipping processing.")
            # Ensure target columns exist even if source is missing
            if lat_col not in df.columns:
                df[lat_col] = np.nan
            if lon_col not in df.columns:
                df[lon_col] = np.nan
            return df

        print(f"  -> Processing column '{original_col}'...")
        # Apply the cleaning chain
        cleaned_coords = df[original_col].apply(
            self._apply_coordinate_cleaning)

        # Split into temporary lat/lon series
        split_data = cleaned_coords.str.split(',', expand=True, n=1)

        # Assign to final columns, cleaning invalid chars and converting
        temp_lat = split_data[0] if split_data.shape[1] > 0 else pd.Series([
                                                                           np.nan] * len(df))
        temp_lon = split_data[1] if split_data.shape[1] > 1 else pd.Series([
                                                                           np.nan] * len(df))

        df[lat_col] = temp_lat.apply(self._clean_invalid_characters)
        df[lon_col] = temp_lon.apply(self._clean_invalid_characters)

        df[lat_col] = pd.to_numeric(df[lat_col], errors='coerce')
        df[lon_col] = pd.to_numeric(df[lon_col], errors='coerce')

        print(
            f"     ✅ Split into '{lat_col}' and '{lon_col}', converted to numeric.")

        # Drop the original coordinate column after processing
        df.drop(columns=[original_col], inplace=True, errors='ignore')
        print(f"     ✅ Dropped original column '{original_col}'.")

        return df

    def run(self, df: pd.DataFrame) -> dict:
        """Runs the cleansing pipeline on the provided DataFrame."""
        print("Running User Cleansing Pipeline...")
        df = self._drop_columns(df)
        df = self._rename_columns(df)
        df = self._remove_duplicates(df)
        df = self._capitalize_columns(df)
        df = self._process_coordinate_column(df, 'pelanggan')

        # Return the cleaned DataFrame as a dictionary with a single key
        return {"user": df} if not df.empty else {}
