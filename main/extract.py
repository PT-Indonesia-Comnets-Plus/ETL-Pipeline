import os
import json
import re
from typing import Tuple, Optional, List, Any, Callable, Dict

import pandas as pd
import gspread
from google.oauth2 import service_account
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.sdk import Variable
from psycopg2 import Error as Psycopg2Error

# --- Constants for Column Names ---
# Raw names from Google Sheets
RAW_ASSET_SHEET_FATID_COL = "FATID"
RAW_USER_SHEET_ID_COL = "ID Permohonan"
RAW_USER_SHEET_FATID_COL = "ID FAT"

# Standardized/Database column names
DB_ASSET_TABLE_FATID_COL = "fat_id"
DB_USER_TABLE_ID_COL = "id_permohonan"

try:
    from main.utils.config_database import initialize_database_connections
except ImportError:
    def initialize_database_connections():
        print("WARNING: Using mock initialize_database_connections. No DB operations will work.")
        return None
# --- End Mock ---


class Extractor:
    """
    Extracts data from Google Sheets, performs initial cleaning, filtering against
    existing database records, and prepares data for the transformation stage.
    """

    def __init__(self):
        self.log = LoggingMixin().log
        self.gspread_client: Optional[gspread.Client] = None
        self.db_pool = initialize_database_connections()

        # DataFrames to store IDs fetched from the database
        self.db_fat_ids_df: pd.DataFrame = pd.DataFrame(
            columns=[DB_ASSET_TABLE_FATID_COL])
        self.db_user_ids_df: pd.DataFrame = pd.DataFrame(
            columns=[DB_USER_TABLE_ID_COL])

        # DataFrame to store all unique FAT IDs (cleaned and expanded) from the asset sheet.
        # Used as a fallback for validating user data if DB FAT IDs are unavailable.
        self.all_sheet_asset_fat_ids_df: pd.DataFrame = pd.DataFrame(
            columns=[DB_ASSET_TABLE_FATID_COL])

        try:
            spreadsheet_id_json = Variable.get("Spreadsheet_ID")
            spreadsheet_id_details = json.loads(spreadsheet_id_json)
            self.aset_spreadsheet_id: str = spreadsheet_id_details["SPREADSHEET_ID_ASET"]
            self.user_spreadsheet_id: str = spreadsheet_id_details["SPREADSHEET_ID_USER"]
        except Exception as e:
            self.log.error(
                f"Failed to load Spreadsheet IDs from Airflow Variable: {e}")
            raise ValueError(
                "Critical configuration Spreadsheet_ID is missing or invalid.") from e

        if not self.db_pool:
            self.log.error(
                "Extractor did not receive a database pool. SQL operations will fail.")

    def _authenticate_gspread(self) -> None:
        """
        Authenticates with the Google Sheets API using credentials from Airflow Variables
        and stores the client in self.gspread_client.
        """
        if self.gspread_client:
            return
        try:
            self.log.info("Authenticating with Google Sheets API...")
            creds_json = Variable.get("Google_Credentials_Key")
            creds_dict = json.loads(creds_json)
            scope = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]
            credentials = service_account.Credentials.from_service_account_info(
                creds_dict, scopes=scope)
            self.gspread_client = gspread.authorize(credentials)
            self.log.info("Google Sheets API authentication successful.")
        except Exception as e:
            self.log.error(f"Google Sheets API authentication failed: {e}")
            raise

    def _load_sheet_to_dataframe(self, spreadsheet_id: str, sheet_name: str) -> pd.DataFrame:
        """
        Loads data from a specific Google Sheet into a pandas DataFrame.

        Args:
            spreadsheet_id: The ID of the Google Spreadsheet.
            sheet_name: The name of the sheet to load.

        Returns:
            A DataFrame containing the sheet data, or an empty DataFrame if no data or error.
        """
        self._authenticate_gspread()  # Ensures client is authenticated
        if not self.gspread_client:  # Should not happen if _authenticate_gspread raises on failure
            self.log.error("gspread client not available for loading sheet.")
            return pd.DataFrame()

        try:
            sheet = self.gspread_client.open_by_key(
                spreadsheet_id).worksheet(sheet_name)
            data = sheet.get_all_values()
            if not data:
                self.log.warning(
                    f"No data found in sheet: {spreadsheet_id} - {sheet_name}")
                return pd.DataFrame()
            if len(data) == 1:  # Only header row
                self.log.warning(
                    f"Sheet '{sheet_name}' in {spreadsheet_id} contains only a header row.")
                return pd.DataFrame(columns=data[0])

            self.log.info(
                f"Successfully loaded {len(data)-1} rows from sheet: {sheet_name} (Spreadsheet ID: {spreadsheet_id})")
            return pd.DataFrame(data[1:], columns=data[0])
        except Exception as e:
            self.log.error(
                f"Failed to load sheet '{sheet_name}' (Spreadsheet ID: {spreadsheet_id}): {e}")
            # Depending on desired behavior, you might want to return an empty DF or raise
            return pd.DataFrame()  # Return empty DF on error to allow potential partial processing

    def _execute_query(self, query: str, params: Optional[tuple] = None, fetch: str = "all") -> Tuple[List[Any], List[str], Optional[str]]:
        """
        Executes a SQL query using the initialized database connection pool.

        Args:
            query: The SQL query to execute.
            params: Parameters for the query.
            fetch: Determines how to fetch results: "all", "one", or "none" (for DML/DDL).

        Returns:
            A tuple (data_rows, column_names, error_message).
            `data_rows` and `column_names` are empty lists if no data or on error.
            `error_message` is None on success, otherwise an error string.
        """
        if not self.db_pool:
            return [], [], "Database pool not initialized"

        conn = None
        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as cur:
                self.log.debug(
                    f"Executing query: {query[:100]}... with params: {params}")
                cur.execute(query, params)
                columns = [desc[0]
                           for desc in cur.description] if cur.description else []

                if fetch == "all" and cur.description:
                    data = cur.fetchall()
                    self.log.info(f"Query fetched {len(data)} rows.")
                    return data, columns, None
                elif fetch == "one" and cur.description:
                    data = cur.fetchone()
                    self.log.info(
                        f"Query fetched {'1 row' if data else '0 rows'}.")
                    # Return as list for consistency
                    return [data] if data else [], columns, None
                elif fetch == "none":  # For DML statements that don't return rows but may affect them
                    conn.commit()
                    self.log.info(
                        f"DML query executed. Rows affected: {cur.rowcount}")
                    return [], [], None
                else:  # For DDL or other non-returning queries that might need commit
                    conn.commit()  # Assuming DDL statements are auto-committed by some drivers but explicit is safer
                    self.log.info(
                        f"Non-SELECT/DML query executed. Status: {cur.statusmessage}, Rows affected: {cur.rowcount}")
                    return [], [], None
        except Psycopg2Error as db_err:
            self.log.error(f"Database Error: {db_err}")
            if conn:
                conn.rollback()
            return [], [], f"Database Error: {db_err}"
        except Exception as e:
            self.log.error(f"Query Execution Error: {e}")
            if conn:
                conn.rollback()
            return [], [], f"Query Execution Error: {e}"
        finally:
            if conn and self.db_pool:
                self.db_pool.putconn(conn)

    def _fetch_db_ids_as_dataframe(self, query: str, id_column_name: str,
                                   cleaning_func: Optional[Callable[[
                                       Any], str]] = None
                                   ) -> Tuple[pd.DataFrame, Optional[str]]:
        """
        Fetches IDs from the database using a query and returns them as a DataFrame.

        Args:
            query: SQL query to fetch IDs. Expected to return a single column.
            id_column_name: The name of the ID column in the query result and target DataFrame.
            cleaning_func: Optional function to clean each ID value.

        Returns:
            A tuple (DataFrame, error_message). DataFrame is empty if error or no data.
        """
        data, columns, error = self._execute_query(query, fetch="all")
        if error:
            return pd.DataFrame(columns=[id_column_name]), error
        if not data:
            return pd.DataFrame(columns=[id_column_name]), None

        # Ensure the column name from query matches expected, or use the first column
        actual_col_name = columns[0] if columns else id_column_name
        df = pd.DataFrame(data, columns=[actual_col_name])
        if actual_col_name != id_column_name:
            df.rename(columns={actual_col_name: id_column_name}, inplace=True)

        if cleaning_func and id_column_name in df.columns:
            df[id_column_name] = df[id_column_name].apply(
                lambda x: cleaning_func(x) if pd.notna(x) else None)
            # Remove rows where ID became None after cleaning
            df.dropna(subset=[id_column_name], inplace=True)
            # Filter out empty strings after cleaning
            df = df[df[id_column_name] != ""]

        df.drop_duplicates(subset=[id_column_name], inplace=True)
        return df, None

    def _prepare_db_fat_ids(self) -> Optional[str]:
        """
        Fetches and prepares distinct FAT IDs from the 'user_terminals' table.
        Stores the result in self.db_fat_ids_df.
        """
        self.log.info(
            "Fetching and preparing existing FAT IDs from database...")
        query = f"SELECT DISTINCT {DB_ASSET_TABLE_FATID_COL} FROM user_terminals;"
        df, error = self._fetch_db_ids_as_dataframe(
            query, DB_ASSET_TABLE_FATID_COL, self._clean_fat_id_text_value)

        if error:
            # Ensure db_fat_ids_df is an empty DataFrame with the correct column on error
            self.db_fat_ids_df = pd.DataFrame(
                columns=[DB_ASSET_TABLE_FATID_COL])
            return f"Failed to fetch existing FAT IDs: {error}"

        self.db_fat_ids_df = df
        self.log.info(
            f"Found {len(self.db_fat_ids_df)} unique existing FAT IDs in database.")
        return None

    def _prepare_db_user_ids(self) -> Optional[str]:
        """
        Fetches and prepares distinct 'id_permohonan' from the 'pelanggans' table.
        Stores the result in self.db_user_ids_df.
        """
        self.log.info(
            "Fetching and preparing existing User IDs (id_permohonan) from database...")
        query = f"SELECT DISTINCT {DB_USER_TABLE_ID_COL} FROM pelanggans;"

        def clean_raw_user_id(value: Any) -> str:
            return str(value).strip() if pd.notna(value) else ""

        df, error = self._fetch_db_ids_as_dataframe(
            query, DB_USER_TABLE_ID_COL, clean_raw_user_id)

        if error:
            self.db_user_ids_df = pd.DataFrame(columns=[DB_USER_TABLE_ID_COL])
            return f"Failed to fetch existing User IDs: {error}"

        self.db_user_ids_df = df
        self.log.info(
            f"Found {len(self.db_user_ids_df)} unique existing User IDs in database.")
        return None

    def _clean_fat_id_text_value(self, value: Any) -> str:
        """Cleans a single FAT ID text value (from sheet or DB)."""
        if pd.isna(value):
            return ""
        cleaned_value = str(value).strip().upper()
        # Remove leading/trailing hyphens that might be surrounded by spaces
        cleaned_value = re.sub(r"^\s*-\s*|\s*-\s*$", "", cleaned_value)
        return cleaned_value.strip()  # Final strip after regex

    def _get_expanded_asset_sheet_fat_ids_for_fallback(self, df_asset_sheet_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Extracts, cleans, and expands FAT ID ranges from the raw asset sheet.
        The resulting DataFrame has a single column DB_ASSET_TABLE_FATID_COL.
        Used for populating self.all_sheet_asset_fat_ids_df for user validation fallback.
        """
        if RAW_ASSET_SHEET_FATID_COL not in df_asset_sheet_raw.columns or df_asset_sheet_raw.empty:
            self.log.info(
                "No 'FATID' column in raw asset sheet or DataFrame empty. Cannot extract FAT IDs for fallback.")
            return pd.DataFrame(columns=[DB_ASSET_TABLE_FATID_COL])

        self.log.info(
            "Extracting and expanding FAT IDs from raw asset sheet for user validation fallback...")
        expanded_fat_ids_list = []
        for _, row in df_asset_sheet_raw.iterrows():
            fat_id_text = row[RAW_ASSET_SHEET_FATID_COL]
            fat_id_text_cleaned = self._clean_fat_id_text_value(fat_id_text)

            if not fat_id_text_cleaned:
                continue

            parts = re.split(r'\s*-\s*', fat_id_text_cleaned)
            # Valid two-part range
            if len(parts) == 2 and parts[0] and parts[1]:
                # Clean parts again, though likely already clean
                expanded_fat_ids_list.append(
                    self._clean_fat_id_text_value(parts[0]))
                expanded_fat_ids_list.append(
                    self._clean_fat_id_text_value(parts[1]))
            else:  # Not a range or an invalid range format
                expanded_fat_ids_list.append(fat_id_text_cleaned)

        if not expanded_fat_ids_list:
            return pd.DataFrame(columns=[DB_ASSET_TABLE_FATID_COL])

        expanded_df = pd.DataFrame(expanded_fat_ids_list, columns=[
                                   DB_ASSET_TABLE_FATID_COL])
        expanded_df.drop_duplicates(
            subset=[DB_ASSET_TABLE_FATID_COL], inplace=True)
        self.log.info(
            f"Found {len(expanded_df)} unique expanded FAT IDs from raw asset sheet for fallback.")
        return expanded_df

    def _clean_and_expand_asset_df(self, df_assets: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans FATID column values and expands FAT ID ranges within the main asset DataFrame.
        This prepares the asset data for filtering and transformation.
        The column name remains RAW_ASSET_SHEET_FATID_COL after this step, but values are cleaned/expanded.
        """
        if df_assets.empty or RAW_ASSET_SHEET_FATID_COL not in df_assets.columns:
            self.log.info(
                "Asset DataFrame is empty or 'FATID' column missing. Skipping cleaning and expansion.")
            return df_assets

        self.log.info(
            "Cleaning 'FATID' column values and expanding ranges for main asset data...")
        # 1. Clean FATID values
        df_assets[RAW_ASSET_SHEET_FATID_COL] = df_assets[RAW_ASSET_SHEET_FATID_COL].apply(
            self._clean_fat_id_text_value)

        # Deduplicate based on cleaned FATID values before expansion to avoid redundant expansion
        # This assumes other row data is identical or 'first' is the desired one for duplicates.
        df_assets.drop_duplicates(
            subset=[RAW_ASSET_SHEET_FATID_COL], keep='first', inplace=True)
        self.log.info(
            f"Rows after initial FATID cleaning & deduplication: {len(df_assets)}")

        # 2. Expand FAT ID ranges
        expanded_rows = []
        for _, row in df_assets.iterrows():
            fat_id_text = row[RAW_ASSET_SHEET_FATID_COL]  # Already cleaned
            if not fat_id_text:  # Preserve rows with empty FATID after cleaning
                expanded_rows.append(row)
                continue

            parts = re.split(r'\s*-\s*', fat_id_text)
            if len(parts) == 2 and parts[0] and parts[1]:
                row_part1 = row.copy()
                # parts are already cleaned
                row_part1[RAW_ASSET_SHEET_FATID_COL] = parts[0]
                expanded_rows.append(row_part1)

                row_part2 = row.copy()
                row_part2[RAW_ASSET_SHEET_FATID_COL] = parts[1]
                expanded_rows.append(row_part2)
            else:
                expanded_rows.append(row)

        result_df = pd.DataFrame(expanded_rows).reset_index(drop=True)
        self.log.info(
            f"Asset DataFrame after FAT ID value cleaning and range expansion: {len(result_df)} rows.")
        return result_df

    def _filter_df_against_db_ids(self, source_df: pd.DataFrame, source_id_column: str,
                                  db_ids_df: pd.DataFrame, db_id_column: str,
                                  data_label: str) -> pd.DataFrame:
        """
        Filters a source DataFrame to keep only records with IDs not present in a db_ids_df.
        """
        if source_df.empty or source_id_column not in source_df.columns:
            self.log.warning(
                f"'{source_id_column}' not in {data_label} source_df or df empty. Skipping filtering.")
            return source_df
        if db_ids_df.empty:
            self.log.info(
                f"No existing {data_label} IDs in database. Keeping all {len(source_df)} source {data_label} records.")
            return source_df

        self.log.info(
            f"Filtering {data_label} data for new records against {len(db_ids_df)} existing DB IDs...")
        original_count = len(source_df)

        filtered_df = source_df[~source_df[source_id_column].isin(
            db_ids_df[db_id_column])].copy()
        self.log.info(
            f"{data_label} data filtering complete. Original: {original_count}, New: {len(filtered_df)}.")
        return filtered_df

    def _clean_asset_df_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans column names of the asset DataFrame: strips whitespace,
        and handles 'Status OSP AMARTA' duplicates by appending a counter.
        """
        if df.empty:
            return df

        self.log.info("Cleaning asset DataFrame column names...")
        target_col_base_name = "Status OSP AMARTA"
        new_cols = []
        osp_amarta_count = 1
        for col_idx, original_col_name in enumerate(df.columns):
            current_col_name_str = str(original_col_name).strip()
            # Check if the original column name (stripped) is the target base name
            if current_col_name_str == target_col_base_name:
                # Check if this is a duplicate by looking at previous new_cols or if others exist
                # A simpler approach for this specific problem is often to rely on position or prior knowledge
                # This implementation renames all occurrences with a counter.
                final_col_name = f"{target_col_base_name} {osp_amarta_count}"
                osp_amarta_count += 1
            else:
                final_col_name = current_col_name_str  # Already stripped

            # General cleaning for all column names (stripping multiple spaces)
            final_col_name = re.sub(r"\s+", " ", final_col_name)
            new_cols.append(final_col_name)

        df.columns = new_cols
        self.log.info(f"Asset DataFrame columns cleaned: {list(df.columns)}")
        return df

    def _prepare_asset_data_for_loading(self, raw_asset_df: pd.DataFrame, db_fat_ids_fetch_error: Optional[str]) -> pd.DataFrame:
        """Orchestrates the processing of asset data from sheet to filtered DataFrame."""
        if raw_asset_df.empty:
            self.log.warning(
                "Raw asset DataFrame is empty. No asset data to process for loading.")
            self.all_sheet_asset_fat_ids_df = pd.DataFrame(
                columns=[DB_ASSET_TABLE_FATID_COL])  # Ensure empty
            return pd.DataFrame()

        # 1. For user validation fallback: extract and expand FAT IDs from the *raw* asset sheet.
        #    This DataFrame (self.all_sheet_asset_fat_ids_df) has one column: DB_ASSET_TABLE_FATID_COL.
        self.all_sheet_asset_fat_ids_df = self._get_expanded_asset_sheet_fat_ids_for_fallback(
            raw_asset_df.copy())

        # 2. Process the main asset DataFrame for actual loading
        # 2a. Clean column names (e.g., "Status OSP AMARTA 1", "Status OSP AMARTA 2")
        processed_asset_df = self._clean_asset_df_column_names(
            raw_asset_df.copy())

        # 2b. Clean FATID values and expand ranges. Column name is still RAW_ASSET_SHEET_FATID_COL.
        processed_asset_df = self._clean_and_expand_asset_df(
            processed_asset_df)
        self.log.info(
            f"Asset data after value cleaning and range expansion: {len(processed_asset_df)} rows.")

        # 2c. Filter against existing DB FAT IDs
        if db_fat_ids_fetch_error:
            self.log.error(f"Cannot filter asset data due to DB FAT ID fetch error: {db_fat_ids_fetch_error}. "
                           f"Proceeding with all {len(processed_asset_df)} processed sheet asset records.")
            asset_df_for_loading = processed_asset_df
        else:
            # RAW_ASSET_SHEET_FATID_COL contains cleaned, individual FAT IDs after _clean_and_expand_asset_df
            asset_df_for_loading = self._filter_df_against_db_ids(
                source_df=processed_asset_df,
                # This column has the cleaned/expanded IDs
                source_id_column=RAW_ASSET_SHEET_FATID_COL,
                db_ids_df=self.db_fat_ids_df,
                db_id_column=DB_ASSET_TABLE_FATID_COL,
                data_label="asset"
            )
        return asset_df_for_loading

    def _determine_authoritative_fat_ids_for_user_validation(self, db_fat_ids_fetch_error: Optional[str]) -> set:
        """Determines the set of FAT IDs to use for validating user records."""
        authoritative_fat_ids = set()
        log_source_message = ""

        if not db_fat_ids_fetch_error:  # No error fetching DB FAT IDs
            if not self.db_fat_ids_df.empty:
                authoritative_fat_ids = set(
                    self.db_fat_ids_df[DB_ASSET_TABLE_FATID_COL].unique())
                log_source_message = (f"Using {len(authoritative_fat_ids)} FAT IDs from DB "
                                      f"(table user_terminals) for user 'ID FAT' validation.")
            else:  # DB FAT IDs fetched successfully but table was empty
                log_source_message = "DB user_terminals is empty. "
                if not self.all_sheet_asset_fat_ids_df.empty:
                    authoritative_fat_ids = set(
                        self.all_sheet_asset_fat_ids_df[DB_ASSET_TABLE_FATID_COL].unique())
                    log_source_message += (f"Falling back to {len(authoritative_fat_ids)} FAT IDs from asset sheet "
                                           "for user 'ID FAT' validation.")
                    self.log.warning(log_source_message)
                else:
                    log_source_message += ("Asset sheet also provided no FAT IDs. "
                                           "User 'ID FAT' validation against a list will be skipped.")
                    self.log.warning(log_source_message)
        else:  # Error fetching DB FAT IDs
            log_source_message = f"Error fetching DB FAT IDs ({db_fat_ids_fetch_error}). "
            self.log.error(log_source_message)  # Log the error part first
            if not self.all_sheet_asset_fat_ids_df.empty:
                authoritative_fat_ids = set(
                    self.all_sheet_asset_fat_ids_df[DB_ASSET_TABLE_FATID_COL].unique())
                log_source_message = (f"Falling back to {len(authoritative_fat_ids)} FAT IDs from asset sheet "
                                      "for user 'ID FAT' validation due to DB error.")
                # Info for the fallback action
                self.log.info(log_source_message)
            else:
                log_source_message = ("Asset sheet also provided no FAT IDs. "
                                      "User 'ID FAT' validation against a list will be skipped due to DB error and no fallback.")
                self.log.warning(log_source_message)

        if log_source_message and not ("Falling back" in log_source_message or "skipped" in log_source_message or "empty" in log_source_message):
            # Log if it was a straightforward "Using DB FAT IDs"
            self.log.info(log_source_message)

        return authoritative_fat_ids

    def _prepare_user_data_for_loading(self, raw_user_df: pd.DataFrame,
                                       db_user_ids_fetch_error: Optional[str],
                                       authoritative_fat_ids_for_validation: set) -> pd.DataFrame:
        """Orchestrates the processing and validation of user data."""
        if raw_user_df.empty:
            self.log.info(
                "Raw user DataFrame is empty. No user data to process.")
            return pd.DataFrame()

        if RAW_USER_SHEET_ID_COL not in raw_user_df.columns:
            self.log.warning(f"Key column '{RAW_USER_SHEET_ID_COL}' not found in user data. "
                             "Cannot perform primary filtering or deduplication. Returning raw data.")
            return raw_user_df  # Or an empty DF if this is a critical error

        # 1. Initial cleaning and deduplication based on user ID from sheet
        user_df_processed = raw_user_df.copy()
        user_df_processed[RAW_USER_SHEET_ID_COL] = user_df_processed[RAW_USER_SHEET_ID_COL].astype(
            str).str.strip()
        # Drop if ID became NaN
        user_df_processed.dropna(subset=[RAW_USER_SHEET_ID_COL], inplace=True)
        # Drop if ID became empty string
        user_df_processed = user_df_processed[user_df_processed[RAW_USER_SHEET_ID_COL] != ""]
        user_df_processed.drop_duplicates(
            subset=[RAW_USER_SHEET_ID_COL], keep='first', inplace=True)
        self.log.info(f"User data after initial cleaning & deduplication on '{RAW_USER_SHEET_ID_COL}': "
                      f"{len(user_df_processed)} rows.")

        # 2. Filter against existing DB User IDs
        if db_user_ids_fetch_error:
            self.log.error(f"Cannot filter user data against DB due to User ID fetch error: {db_user_ids_fetch_error}. "
                           f"Proceeding with all {len(user_df_processed)} initially processed user records.")
        else:
            user_df_processed = self._filter_df_against_db_ids(
                source_df=user_df_processed,
                source_id_column=RAW_USER_SHEET_ID_COL,
                db_ids_df=self.db_user_ids_df,
                db_id_column=DB_USER_TABLE_ID_COL,
                data_label="user"
            )

        # 3. Validate 'ID FAT' in user data against authoritative FAT ID list
        if RAW_USER_SHEET_FATID_COL not in user_df_processed.columns:
            self.log.warning(
                f"Column '{RAW_USER_SHEET_FATID_COL}' not found in user data. Skipping 'ID FAT' validation.")
        elif not authoritative_fat_ids_for_validation:
            self.log.warning(
                "No authoritative FAT ID list available. Skipping user 'ID FAT' validation.")
        else:
            self.log.info(f"Validating '{RAW_USER_SHEET_FATID_COL}' for {len(user_df_processed)} user records "
                          f"against {len(authoritative_fat_ids_for_validation)} authoritative FAT IDs.")

            # Clean the user's FAT ID column before comparison
            user_df_processed[RAW_USER_SHEET_FATID_COL] = user_df_processed[RAW_USER_SHEET_FATID_COL].apply(
                self._clean_fat_id_text_value)

            original_count_before_fat_val = len(user_df_processed)
            user_df_processed = user_df_processed[
                user_df_processed[RAW_USER_SHEET_FATID_COL].isin(
                    authoritative_fat_ids_for_validation)
            ].copy()
            dropped_count = original_count_before_fat_val - \
                len(user_df_processed)
            if dropped_count > 0:
                self.log.warning(f"{dropped_count} user records dropped due to invalid/missing '{RAW_USER_SHEET_FATID_COL}' "
                                 "after validation against authoritative FAT ID list.")
            self.log.info(
                f"User data after '{RAW_USER_SHEET_FATID_COL}' validation: {len(user_df_processed)} rows.")

        return user_df_processed

    def _save_df_to_parquet_and_log(self, df: pd.DataFrame, base_filename: str, run_id: str, temp_dir: str) -> Optional[str]:
        """Saves a DataFrame to a Parquet file in the temp_dir and logs the action."""
        if df.empty:
            self.log.info(
                f"DataFrame for '{base_filename}' is empty. Nothing to save.")
            return None

        file_path = os.path.join(temp_dir, f"{base_filename}_{run_id}.parquet")
        try:
            df.to_parquet(file_path, index=False)
            self.log.info(
                f"Data for '{base_filename}' saved to: {file_path} ({len(df)} rows)")
            return file_path
        except Exception as e:
            self.log.error(
                f"Failed to save DataFrame '{base_filename}' to Parquet at {file_path}: {e}")
            return None

    def run(self, ti):
        """
        Orchestrates the extraction process:
        1. Fetches existing IDs from the database.
        2. Loads data from Google Sheets.
        3. Processes asset data (clean, expand, filter).
        4. Processes user data (clean, filter, validate FAT IDs).
        5. Saves processed data to Parquet files and pushes paths to XCom.
        """
        temp_dir = "/opt/airflow/temp"  # Consider making this configurable
        os.makedirs(temp_dir, exist_ok=True)
        run_id = ti.run_id  # Assumes ti is passed from Airflow task instance

        self.log.info(f"--- Starting Extractor Task for run_id: {run_id} ---")

        # 1. Fetch existing IDs from database
        db_fat_ids_fetch_error = self._prepare_db_fat_ids()
        db_user_ids_fetch_error = self._prepare_db_user_ids()

        # 2. Load data from Google Sheets
        # Error handling within _load_sheet_to_dataframe returns empty DF, allowing flow to continue.
        raw_asset_df = self._load_sheet_to_dataframe(
            self.aset_spreadsheet_id, "Datek Aset All")
        raw_user_df = self._load_sheet_to_dataframe(
            self.user_spreadsheet_id, "Data All")
        self.log.info(f"Initial rows from asset sheet: {len(raw_asset_df)}")
        self.log.info(f"Initial rows from user sheet: {len(raw_user_df)}")

        # 3. Process Asset Data
        asset_df_for_loading = self._prepare_asset_data_for_loading(
            raw_asset_df, db_fat_ids_fetch_error)

        # 4. Process User Data
        # Determine authoritative FAT IDs for user validation (uses self.db_fat_ids_df and self.all_sheet_asset_fat_ids_df)
        authoritative_fat_ids = self._determine_authoritative_fat_ids_for_user_validation(
            db_fat_ids_fetch_error)
        user_df_for_loading = self._prepare_user_data_for_loading(
            raw_user_df, db_user_ids_fetch_error, authoritative_fat_ids)

        # 5. Save processed data and push paths to XCom
        asset_output_path = self._save_df_to_parquet_and_log(
            asset_df_for_loading, "aset_data_extracted", run_id, temp_dir)
        user_output_path = self._save_df_to_parquet_and_log(
            user_df_for_loading, "user_data_extracted", run_id, temp_dir)
        db_fat_ids_output_path = self._save_df_to_parquet_and_log(
            self.db_fat_ids_df, "db_fat_ids_snapshot", run_id, temp_dir)

        ti.xcom_push(key="aset_data_extracted_path", value=asset_output_path)
        ti.xcom_push(key="user_data_extracted_path", value=user_output_path)
        # Path to the snapshot of DB FAT IDs used
        ti.xcom_push(key="db_fat_ids_snapshot_path",
                     value=db_fat_ids_output_path)

        self.log.info(f"--- Extractor Task for run_id: {run_id} Finished ---")
