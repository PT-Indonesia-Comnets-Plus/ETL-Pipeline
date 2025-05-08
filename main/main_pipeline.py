import os
import json
import logging
from datetime import datetime
import pandas as pd
import numpy as np
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from main.utils.cleansing_asset import AssetPipeline

# --- Konfigurasi Logging ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# --- Fungsi Helper ---


def get_google_credentials_from_env():
    try:
        credentials = service_account.Credentials.from_service_account_info(
            "credential.json",
            scopes=[
                'https://www.googleapis.com/auth/spreadsheets.readonly',
                'https://www.googleapis.com/auth/drive'
            ]
        )
        return credentials
    except json.JSONDecodeError:
        logging.error("Format JSON di GOOGLE_CREDENTIALS_JSON tidak valid.")
        raise
    except Exception as e:
        logging.error(f"Error saat memproses kredensial: {e}")
        raise


def extract_from_sheets(credentials, spreadsheet_id, sheet_name):
    try:
        service = build('sheets', 'v4', credentials=credentials)
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=spreadsheet_id,
                                    range=f"'{sheet_name}'!A1:Z").execute()
        values = result.get('values', [])

        if not values:
            logging.warning(f"Tidak ada data ditemukan di Sheet: {sheet_name}")
            return pd.DataFrame()
        else:
            header = values[0]
            data = values[1:]
            df = pd.DataFrame(data, columns=header)
            logging.info(
                f"Berhasil mengekstrak {len(df)} baris dari Sheet: {sheet_name}")
            return df
    except Exception as e:
        logging.error(f"Gagal mengekstrak data dari Sheet '{sheet_name}': {e}")
        raise


def clean_data(df, data_type):
    logging.info(f"Memulai pembersihan data untuk {data_type}...")
    if df is None or df.empty:
        logging.warning(f"DataFrame {data_type} kosong.")
        return df

    if data_type == 'aset':
        try:
            pipeline = AssetPipeline()
            # Asumsikan pipeline.run() mengembalikan dictionary of DataFrames
            cleaned_data_dict = pipeline.run(df)
            logging.info(
                f"✅ Pembersihan data aset menggunakan AssetPipeline selesai. Hasil: {list(cleaned_data_dict.keys())}")
            # Mengembalikan dictionary hasil pemisahan
            return cleaned_data_dict
        except Exception as e:
            logging.error(
                f"❌ Gagal menjalankan AssetPipeline pada data aset: {e}")
            raise  # Atau return None/empty dict tergantung penanganan error yg diinginkan
    elif data_type == 'user':
        # Untuk data user, mungkin perlu pipeline/logika pembersihan berbeda
        # Untuk saat ini, kita kembalikan apa adanya atau lakukan pembersihan dasar jika ada
        logging.info(
            "Pembersihan data user (saat ini mengembalikan data asli)...")
        return df  # Kembalikan DataFrame user asli
    else:
        logging.warning(
            f"Tipe data tidak dikenal: {data_type}. Mengembalikan data asli.")
        return df


def save_to_csv(df, filename):
    os.makedirs("output", exist_ok=True)
    path = os.path.join("output", filename)
    df.to_csv(path, index=False)
    logging.info(f"DataFrame disimpan ke {path}")
    return path


def upload_to_drive(credentials, file_path, folder_id):
    """Uploads a file to a specific Google Drive folder."""
    try:
        service = build('drive', 'v3', credentials=credentials)
        file_metadata = {
            'name': os.path.basename(file_path),
            'parents': [folder_id]
        }
        media = MediaFileUpload(file_path, resumable=True)
        file = service.files().create(body=file_metadata,
                                      media_body=media, fields='id').execute()
        logging.info(
            f"✅ Berhasil upload '{os.path.basename(file_path)}' ke Drive. File ID: {file.get('id')}")
    except Exception as e:
        logging.error(
            f"❌ Gagal upload '{os.path.basename(file_path)}' ke Drive: {e}")
        # Pertimbangkan untuk raise error jika upload gagal adalah kritikal

# --- Pipeline Eksekusi ---


if __name__ == "__main__":
    logging.info("Memulai pipeline ETL...")

    try:
        creds = get_google_credentials_from_env()
        aset_spreadsheet_id = os.getenv('ASET_SPREADSHEET_ID')
        user_spreadsheet_id = os.getenv('USER_SPREADSHEET_ID')
        drive_folder_id = os.getenv('GOOGLE_DRIVE_TARGET_FOLDER_ID')

        if not aset_spreadsheet_id or not user_spreadsheet_id or not drive_folder_id:
            raise ValueError("ID spreadsheet/folder Drive tidak ditemukan!")

        aset_sheet_name = 'Datek Aset All'
        user_sheet_name = 'Data All'

        # --- Extract ---
        logging.info("=== Tahap Ekstraksi ===")
        df_aset = extract_from_sheets(
            creds, aset_spreadsheet_id, aset_sheet_name)
        df_user = extract_from_sheets(
            creds, user_spreadsheet_id, user_sheet_name)

        # --- Clean/Transform ---
        logging.info("=== Tahap Pembersihan ===")
        # clean_data untuk 'aset' sekarang mengembalikan dictionary
        cleaned_aset_data_dict = clean_data(df_aset, 'aset')
        df_user_clean = clean_data(df_user, 'user')

        # --- Save local CSV & Upload ---
        logging.info("=== Tahap Penyimpanan Lokal & Upload ke Drive ===")
        uploaded_files = []
        timestamp_str = datetime.now().strftime(
            '%Y%m%d_%H%M%S')  # Tambahkan waktu agar lebih unik

        # Proses dictionary hasil pembersihan aset
        if isinstance(cleaned_aset_data_dict, dict):
            for table_name, df_split in cleaned_aset_data_dict.items():
                if not df_split.empty:
                    file_path = save_to_csv(
                        df_split, f"{table_name}_clean_{timestamp_str}.csv")
                    upload_to_drive(creds, file_path, drive_folder_id)
                    uploaded_files.append(file_path)

        # Proses DataFrame user (jika tidak kosong)
        user_file_path = save_to_csv(
            df_user_clean, f"user_clean_{timestamp_str}.csv")
        upload_to_drive(creds, user_file_path, drive_folder_id)
        uploaded_files.append(user_file_path)

        logging.info("Pipeline ETL selesai dengan sukses.")

    except Exception as e:
        logging.error(f"Pipeline ETL gagal: {e}")
