import os
import pandas as pd
from airflow.utils.log.logging_mixin import LoggingMixin
from google.oauth2 import service_account
import gspread
from airflow.sdk import Variable
import json
import re
from typing import Tuple, Optional, Set, List, Any
from main.utils.config_database import initialize_database_connections
from psycopg2 import Error as Psycopg2Error


class Extractor:
    """
    Extracts data from Google Sheets, performs initial cleaning and filtering
    against existing database records, and prepares data for the transform stage.
    """

    def __init__(self):
        self.log = LoggingMixin().log
        self.client: Optional[gspread.Client] = None
        self.db_pool = initialize_database_connections()
        Spreadsheet_ID = Variable.get(
            "Spreadsheet_ID")
        # Parse string JSON menjadi dictionary Python
        Spreadsheet_ID_details = json.loads(Spreadsheet_ID)
        self.aset_id = Spreadsheet_ID_details["SPREADSHEET_ID_ASET"]
        self.user_id = Spreadsheet_ID_details["SPREADSHEET_ID_USER"]

        if not self.db_pool:
            self.log.error(
                "Extractor tidak mendapatkan database pool. Operasi SQL akan gagal.")

    def _authenticate(self):
        """
        Authenticates with the Google Sheets API using credentials from Airflow Variables.

        Returns:
            gspread.Client: An authorized gspread client.
        """
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
            client = gspread.authorize(credentials)
            self.log.info("Google Sheets API authentication successful.")
            return client
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
            pd.DataFrame: A DataFrame containing the sheet data.
        """
        if self.client is None:
            self.client = self._authenticate()

        try:
            sheet = self.client.open_by_key(
                spreadsheet_id).worksheet(sheet_name)
            data = sheet.get_all_values()
            if not data:
                self.log.warning(f"No data found in sheet: {sheet_name}")
                return pd.DataFrame()
            if len(data) == 1:
                self.log.warning(
                    f"Sheet '{sheet_name}' contains only a header row.")
                return pd.DataFrame(columns=data[0])
            self.log.info(
                f"Successfully loaded {len(data)-1} rows from sheet: {sheet_name}")
            return pd.DataFrame(data[1:], columns=data[0])
        except Exception as e:
            self.log.error(f"Failed to load sheet '{sheet_name}': {e}")
            raise

    def _execute_query(self, query: str, params: Optional[tuple] = None, fetch: str = "all") -> Tuple[List[Any], List[str], Optional[str]]:
        """
        Executes a SQL query using the initialized database connection pool.

        Args:
            query: The SQL query to execute.
            params: Parameters for the query.
            fetch: Determines how to fetch results: "all", "one", or "none" (for DML).

        Returns:
            A tuple (data_rows, column_names, error_message).
            `data_rows` and `column_names` are empty lists if no data or on error for "all"/"one".
            `error_message` is None on success, otherwise an error string.
        """
        if not self.db_pool:
            self.log.error(
                "Database pool tidak tersedia. Tidak bisa menjalankan query.")
            return None, None, "Database pool not initialized"

        conn = None
        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as cur:
                self.log.debug(
                    f"Executing query: {query} with params: {params}")
                cur.execute(query, params)
                columns = [desc[0]
                           for desc in cur.description] if cur.description else []

                if fetch == "all" and cur.description:
                    data = cur.fetchall()
                    self.log.info(
                        f"Query fetched {len(data)} rows.")
                    return data, columns, None
                elif fetch == "one" and cur.description:
                    data = cur.fetchone()
                    self.log.info(
                        f"Query fetched {'1 row' if data else '0 rows'}.")
                    return [data] if data else [], columns, None
                elif fetch == "none":  # For DML statements
                    conn.commit()
                    self.log.info(
                        f"DML query executed. Rows affected: {cur.rowcount}")
                    return [], [], None
                else:  # For DDL or other non-returning queries that might need commit
                    conn.commit()
                    self.log.info(
                        f"Non-SELECT/DML query executed. Rows affected: {cur.rowcount}")
                    return [], [], None
        except Psycopg2Error as db_err:
            self.log.error(f"Database Error: {db_err}")
            if conn:
                conn.rollback()
            return [], [], f"Database Error: {db_err}"
        except Exception as e:
            self.log.error(f"Execution Error: {e}")
            if conn:
                conn.rollback()
            return [], [], f"Execution Error: {e}"
        finally:
            if conn and self.db_pool:
                self.db_pool.putconn(conn)

    def _get_existing_fat_ids(self) -> Tuple[Optional[Set[str]], Optional[str]]:
        """
        Fetches distinct, cleaned FAT IDs from the 'user_terminals' table.

        Returns:
            A tuple: (set of existing FAT IDs, error message).
            The set is None if an error occurs. Error message is None on success.
        """
        self.log.info("Fetching existing FAT IDs from database...")
        query_existing = "SELECT DISTINCT fat_id FROM user_terminals;"
        existing_data, _, db_error = self._execute_query(
            query_existing, fetch="all")

        if db_error:
            self.log.error(f"Failed to fetch existing FAT IDs: {db_error}")
            return None, f"Failed to fetch existing FAT IDs: {db_error}"

        db_fat_id_set = set()
        if existing_data:
            db_fat_id_set = {self._clean_fat_id_value(item[0])
                             for item in existing_data if item[0] is not None}
            self.log.info(
                f"Found {len(db_fat_id_set)} unique existing FAT IDs in database.")
        else:
            self.log.info("No existing FAT IDs found in database.")
        return db_fat_id_set, None

    def _get_existing_user_ids(self) -> Tuple[Optional[Set[str]], Optional[str]]:
        """
        Fetches distinct, cleaned 'id_permohonan' from the 'pelanggans' table.
        """
        self.log.info(
            "Fetching existing User IDs (id_permohonan) from database...")
        # Sesuaikan nama tabel jika berbeda
        query_existing = "SELECT DISTINCT id_permohonan FROM pelanggans;"
        existing_data, _, db_error = self._execute_query(
            query_existing, fetch="all")

        if db_error:
            self.log.error(f"Failed to fetch existing User IDs: {db_error}")
            return None, f"Failed to fetch existing User IDs: {db_error}"

        db_user_id_set = set()
        if existing_data:
            # Asumsikan id_permohonan adalah string dan perlu di-strip
            db_user_id_set = {str(item[0]).strip(
            ) for item in existing_data if item[0] is not None and str(item[0]).strip()}
            self.log.info(
                f"Found {len(db_user_id_set)} unique existing User IDs in database.")
        else:
            self.log.info("No existing User IDs found in database.")
        return db_user_id_set, None

    def _clean_fat_id_value(self, value: Any) -> str:
        """
        Cleans a single FAT ID value: converts to string, strips whitespace, and uppercases.
        """
        return str(value).strip().upper()

    def _expand_fat_id_ranges_in_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Expands rows where 'fat_id' represents a simple range (e.g., "ID1-ID2").
        Standardizes 'fat_id' values and deduplicates after expansion.

        Args:
            df: DataFrame with a 'fat_id' column.
        """
        if 'fat_id' not in df.columns or df.empty:  # Ensure 'fat_id' column exists
            self.log.info(
                "No 'fat_id' column or empty DataFrame, skipping FAT ID range expansion.")
            return df

        self.log.info("Cleaning and expanding FAT ID ranges if any...")
        expanded_rows = []
        initial_row_count = len(df)
        expansion_count = 0

        for _, row in df.iterrows():
            fat_id_val = self._clean_fat_id_value(
                row['fat_id']) if pd.notna(row['fat_id']) else ''

            # Check for a simple two-part range "PREFIX1-PREFIX2NUM" or "ID1-ID2"
            # Avoids splitting on negative numbers or hyphens within an ID part.
            if '-' in fat_id_val and not fat_id_val.startswith('-'):
                parts = [part.strip() for part in fat_id_val.split('-')]
                # Both parts must be non-empty
                if len(parts) == 2 and parts[0] and parts[1]:
                    row1 = row.copy()
                    row1['fat_id'] = self._clean_fat_id_value(parts[0])
                    expanded_rows.append(row1)

                    row2 = row.copy()
                    row2['fat_id'] = self._clean_fat_id_value(parts[1])
                    expanded_rows.append(row2)
                    expansion_count += 1
                else:  # Not a valid two-part range, treat as single ID
                    row_copy = row.copy()
                    row_copy['fat_id'] = fat_id_val  # Already cleaned
                    expanded_rows.append(row_copy)
            else:  # Not a range, or an empty/invalid string
                row_copy = row.copy()
                row_copy['fat_id'] = self._clean_fat_id_value(
                    fat_id_val)  # Gunakan metode yang sudah ada
                expanded_rows.append(row_copy)

        result_df = pd.DataFrame(
            expanded_rows, columns=df.columns) if expanded_rows else df.iloc[0:0].copy()  # Empty df with same columns

        self.log.info(
            f"FAT ID range expansion: {initial_row_count} rows -> {len(result_df)} rows. {expansion_count} ranges processed.")

        if not result_df.empty:
            result_df.drop_duplicates(
                subset=['fat_id'], keep='first', inplace=True)
            self.log.info(
                f"Rows after deduplication on 'fat_id' post-expansion: {len(result_df)}.")
        return result_df

    def _filter_new_records_by_id(self, df: pd.DataFrame, id_column_name: str, existing_ids: Set[str]) -> pd.DataFrame:
        """
        Filters DataFrame to keep only rows with ID in `id_column_name` not in `existing_ids`.
        Assumes `id_column_name` in `df` is cleaned.

        Args:
            id_column_name: The name of the ID column to filter on (e.g., 'fat_id', 'id_permohonan').
            df: DataFrame to filter.
            existing_ids: Set of FAT IDs already in the database.
        """
        if id_column_name not in df.columns or df.empty:
            self.log.warning(
                f"Column '{id_column_name}' not found or DataFrame empty. Skipping filtering.")
            return df  # Kembalikan df asli jika kolom ID tidak ada
        if not existing_ids:
            self.log.info(
                f"No existing IDs in database for column '{id_column_name}'. Keeping all source records.")
            return df

        self.log.info(
            f"Filtering DataFrame on column '{id_column_name}' for new records against {len(existing_ids)} existing IDs...")
        original_count = len(df)
        filtered_df = df[~df[id_column_name].isin(existing_ids)].copy()
        self.log.info(
            f"Filtering complete. Original records: {original_count}, New records: {len(filtered_df)}.")
        return filtered_df

    def _clean_aset_df_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans column names of the aset DataFrame.
        Specifically handles duplicate "Status OSP AMARTA" and general cleaning.
        """
        self.log.info("Cleaning column names for aset_df...")
        target_col_name = "Status OSP AMARTA"
        new_cols = []
        count = 1
        for col_name_original in df.columns:
            current_col_name = str(col_name_original)  # Ensure string
            if current_col_name == target_col_name:
                current_col_name = f"{target_col_name} {count}"
                count += 1
            cleaned_name = re.sub(r"\s+", " ", current_col_name).strip()
            new_cols.append(cleaned_name)
        df.columns = new_cols
        self.log.info(f"Aset_df columns after cleaning: {df.columns.tolist()}")
        return df

    def _standardize_fat_id_column(self, df: pd.DataFrame, target_name: str = "fat_id", alternatives: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Ensures a DataFrame has a 'fat_id' column, renaming from alternatives if necessary.
        """
        if alternatives is None:
            alternatives = ["FATID"]

        if target_name in df.columns:
            self.log.info(f"Column '{target_name}' already exists.")
            return df

        for alt_name in alternatives:
            if alt_name in df.columns:
                self.log.info(
                    f"Renaming column '{alt_name}' to '{target_name}'.")
                df.rename(columns={alt_name: target_name}, inplace=True)
                return df

        self.log.warning(
            f"Could not find or standardize FAT ID column (target: '{target_name}', alternatives: {alternatives}). Filtering may be skipped.")
        return df

    def run(self, ti):
        """
        Orchestrates the extraction process: load, clean, filter, and save data.
        Pushes paths of saved data (aset_data_path, user_data_path) to XCom.
        """
        temp_dir = "/opt/airflow/temp"
        os.makedirs(temp_dir, exist_ok=True)
        run_id = ti.run_id

        if self.client is None:
            self.client = self._authenticate()

        aset_df = self._load_sheet_to_dataframe(self.aset_id, "Datek Aset All")
        user_df = self._load_sheet_to_dataframe(self.user_id, "Data All")

        if aset_df.empty:
            self.log.warning(  # Ubah ke warning jika user_df masih bisa diproses
                "Aset DataFrame is empty. Aborting extraction for aset data.")
            # Decide if this is a critical error for the whole task
            # For now, we'll let user_df proceed if it's not empty.
            # If both must exist, raise ValueError here.

        # Preprocess aset_df
        # Use .copy() to avoid SettingWithCopyWarning
        aset_df = self._clean_aset_df_column_names(aset_df.copy())
        aset_df = self._standardize_fat_id_column(
            aset_df, target_name="fat_id", alternatives=["FATID"])

        if 'fat_id' in aset_df.columns:
            aset_df = self._expand_fat_id_ranges_in_df(aset_df)
        else:
            self.log.info(
                "Skipping FAT ID expansion as 'fat_id' column is not present in aset_df after cleaning.")

        if 'fat_id' in aset_df.columns and not aset_df.empty:
            self.log.info(
                "Attempting to filter aset_df based on existing FAT IDs in the database.")
            existing_fat_ids, error_msg = self._get_existing_fat_ids()

            if error_msg:
                self.log.error(
                    f"Could not filter FAT IDs due to database error: {error_msg}. Proceeding with current aset_df records ({len(aset_df)}).")
            elif existing_fat_ids is not None:  # Can be an empty set
                aset_df = self._filter_new_records_by_id(
                    aset_df, "fat_id", existing_fat_ids)
                self.log.info(
                    f"aset_df filtered. Remaining records: {len(aset_df)}.")
        elif 'fat_id' not in aset_df.columns:
            self.log.info(
                "Skipping FAT ID filtering as 'fat_id' column is not present in aset_df.")
        elif aset_df.empty:
            self.log.info(
                "Skipping FAT ID filtering as aset_df is empty after expansion/cleaning.")

        # Preprocess dan filter user_df (jika perlu logika inkremental)
        # Asumsikan 'ID Permohonan' adalah PK untuk user_df
        # Sesuaikan jika nama kolom berbeda setelah cleaning awal
        user_id_column = "ID Permohonan"
        if user_id_column in user_df.columns and not user_df.empty:
            # Bersihkan kolom ID Permohonan sebelum filtering
            user_df[user_id_column] = user_df[user_id_column].astype(
                str).str.strip()
            user_df.drop_duplicates(
                subset=[user_id_column], keep='first', inplace=True)  # Deduplikasi awal

            self.log.info(
                f"Attempting to filter user_df based on existing User IDs ('{user_id_column}') in the database.")
            existing_user_ids, error_msg_user = self._get_existing_user_ids()

            if error_msg_user:
                self.log.error(
                    f"Could not filter User IDs due to database error: {error_msg_user}. Proceeding with current user_df records ({len(user_df)}).")
            elif existing_user_ids is not None:
                user_df = self._filter_new_records_by_id(
                    user_df, user_id_column, existing_user_ids)
                self.log.info(
                    f"user_df filtered. Remaining records: {len(user_df)}.")
        elif user_id_column not in user_df.columns:
            self.log.info(
                f"Skipping User ID filtering as column '{user_id_column}' is not present in user_df.")
        elif user_df.empty:
            self.log.info(
                "Skipping User ID filtering as user_df is empty.")

        aset_new_path = os.path.join(
            temp_dir, f"aset_data_new_{run_id}.parquet")
        user_new_path = os.path.join(
            temp_dir, f"user_data_new_{run_id}.parquet")

        # Simpan aset_df yang mungkin sudah difilter
        aset_df.to_parquet(aset_new_path, index=False)
        # Simpan user_df yang mungkin sudah difilter
        user_df.to_parquet(user_new_path, index=False)

        self.log.info(
            f"Data aset baru disimpan ke: {aset_new_path} ({len(aset_df)} rows)")
        self.log.info(
            f"Data user baru disimpan ke: {user_new_path} ({len(user_df)} rows)")

        ti.xcom_push(key="aset_data_new_path", value=aset_new_path)
        ti.xcom_push(key="user_data_new_path", value=user_new_path)
