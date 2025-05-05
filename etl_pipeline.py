from __future__ import annotations
import os
import re
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np
import json
import gspread
from google.oauth2 import service_account
from airflow import DAG
from airflow.decorators import task
import pandas as pd
from sqlalchemy import create_engine
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.hooks.postgres import PostgresHook  # Konsisten pakai ini
from airflow.utils.log.logging_mixin import LoggingMixin
import hashlib
from datetime import datetime, timedelta
from .utils.data_cleansing import (
    clean_column_names,
    clean_dataframe
)
from main.utils.proces_clean_coor import (
    clean_coordinate_column)
from helper.load_sheet import load_sheet_as_dataframe


t_log = LoggingMixin().log

# ========================= DEFINISI DAG AIRFLOW =========================
# Menggunakan @dag decorator
GOOGLE_CREDENTIALS_ENV_VAR_NAME = "GOOGLE_SHEET_CREDENTIALS"
POSTGRES_CONN_ID = "postgres_default"
SQL_CREATE_TABLES_FILE_NAME = "schema.sql"
SQL_DIR_PATH = os.path.join(os.path.dirname(__file__), "sql")
TEMP_DATA_DIR = "/temp"


# Task ID sudah didefinisikan di decorator
@task(task_id='create_database_tables')
def create_table_if_not_exists(postgres_conn_id: str, sql_dir: str, sql_file_name: str):
    """
    Eksekusi script DDL (CREATE TABLE IF NOT EXISTS, dll.) dari file SQL.

    Menggunakan PostgresHook.run() dengan path file SQL.
    Hook akan membaca file, memecah statement (jika ada semicolon),
    mengeksekusinya, dan melakukan commit.

    Args:
        postgres_conn_id (str): Airflow Connection ID untuk database PostgreSQL.
        sql_dir (str): Direktori tempat file SQL disimpan di lingkungan Airflow worker.
        sql_file_name (str): Nama file SQL (misal: 'init.sql').
    """
    t_log.info(
        f"Task DDL: Menggunakan koneksi '{postgres_conn_id}', mengeksekusi file '{sql_file_name}' di direktori '{sql_dir}'")

    # Buat instance Hook
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    # Buat path lengkap ke file SQL
    sql_file_path = os.path.join(sql_dir, sql_file_name)

    try:
        # Memeriksa apakah file SQL ada sebelum mencoba membacanya
        if not os.path.exists(sql_file_path):
            error_msg = f"File SQL DDL '{sql_file_name}' tidak ditemukan di lokasi: {sql_dir} (Path lengkap: {sql_file_path})"
            t_log.error(error_msg)
            raise FileNotFoundError(error_msg)
        t_log.info(f"Mengeksekusi script DDL dari file: '{sql_file_path}'...")
        hook.run(sql=sql_file_path)

        t_log.info(f"✅ Eksekusi DDL dari '{sql_file_path}' selesai.")
    except Exception as e:
        t_log.error(f"❌ Gagal eksekusi DDL dari '{sql_file_path}': {e}")
        raise


@task(task_id='load_and_initial_clean')  # <--- task_id diperbarui
# Hapus parameter temp_dir karena Airflow menyediakannya via ti
def load_and_initial_clean_func(
    aset_spreadsheet_id: str,
    aset_sheet_name: str,
    user_spreadsheet_id: str,
    user_sheet_name: str,
    google_credentials_env_var_name: str,
    # Hapus temp_dir: str,
    ti=None
) -> dict:
    """
    Task: Load data dari Sheets, autentikasi, bersihkan awal kolom,
    standarisasi ID kunci, lakukan deduplikasi awal, dan simpan ke Parquet sementara.
    """
    t_log.info("Memulai task: Load dan Pembersihan Awal.")

    # ========================= Autentikasi Google Sheets =========================
    t_log.info("--- Memulai Autentikasi Google Sheets ---")
    try:
        credentials_json_string = os.environ.get(
            google_credentials_env_var_name)

        if not credentials_json_string:
            raise ValueError(
                f"ENV var '{google_credentials_env_var_name}' tidak diset atau kosong.")

        credentials_dict = json.loads(credentials_json_string)
        scope = ["https://www.googleapis.com/auth/spreadsheets",
                 "https://www.googleapis.com/auth/drive"]
        credentials = service_account.Credentials.from_service_account_info(
            credentials_dict, scopes=scope)
        client = gspread.authorize(credentials)
        t_log.info("✅ Autentikasi Google Sheets berhasil.")
    except Exception as e:
        t_log.error(f"❌ Gagal autentikasi Google Sheets: {e}")
        raise  # Penting: Raise exception agar task gagal di Airflow

    # ========================= Load Data dari Google Sheets =========================
    t_log.info("\n--- Memulai Load Data dari Google Sheets ---")
    try:
        aset_data = load_sheet_as_dataframe(
            aset_spreadsheet_id, aset_sheet_name, client=client)
        user_data = load_sheet_as_dataframe(
            user_spreadsheet_id, user_sheet_name, client=client)

        if aset_data is None or aset_data.empty or user_data is None or user_data.empty:
            t_log.error("❌ DataFrame sumber kosong setelah load.")
            if aset_data is None or aset_data.empty:
                t_log.error(
                    f"DataFrame aset ('{aset_spreadsheet_id}/{aset_sheet_name}') kosong atau gagal dimuat.")
            if user_data is None or user_data.empty:
                t_log.error(
                    f"DataFrame user ('{user_spreadsheet_id}/{user_sheet_name}') kosong atau gagal dimuat.")
            raise ValueError(
                "DataFrame sumber (aset_data atau user_data) kosong.")

        t_log.info(
            f"✅ Load Data dari Google Sheets berhasil. Aset: {len(aset_data)} baris, User: {len(user_data)} baris.")

    except Exception as e:
        t_log.error(f"❌ Gagal Load Data dari Google Sheets: {e}")
        raise  # Raise exception agar task gagal

    # ========================= Pembersihan Kolom Awal & Rename =========================
    t_log.info("\n--- Memulai Pembersihan Kolom Awal & Rename ---")
    try:
        # 2a. Rename Kolom Duplikat "Status OSP AMARTA" akan ditangani oleh AssetPipeline
        # 2b. Bersihkan Nama Kolom Umum (menggunakan fungsi clean_column_names)
        # Tetap lakukan pembersihan dasar di sini
        t_log.info("-> Membersihkan nama kolom umum (basic)...")

        # 2b. Bersihkan Nama Kolom Umum (menggunakan fungsi clean_column_names)
        # Asumsikan clean_column_names mengembalikan DataFrame baru atau memodifikasi inplace
        t_log.info("-> Membersihkan nama kolom umum...")
        aset_data = clean_column_names(aset_data)
        user_data = clean_column_names(user_data)
        t_log.info("✅ Bersihkan Nama Kolom Umum selesai.")

    except Exception as e:
        t_log.error(f"❌ Error saat Pembersihan Kolom Awal & Rename: {e}")
        raise  # Raise exception

    # ========================= Pembersihan & Standarisasi Kolom ID Kunci =========================
    t_log.info("\n--- Memulai Pembersihan & Standarisasi Kolom ID Kunci ---")

    # Standarisasi FATID di aset_data
    if 'FATID' in aset_data.columns:
        # Count non-null sebelum proses
        initial_fatid_count = aset_data['FATID'].count()
        aset_data['FATID'] = aset_data['FATID'].astype(
            str).str.strip().fillna('')
        aset_data['FATID'] = aset_data['FATID'].str.upper()
        final_fatid_count = (aset_data['FATID'] != '').sum()
        t_log.info(
            f"✅ Kolom 'FATID' di aset_data distandarisasi. Non-empty: {initial_fatid_count} -> {final_fatid_count}.")
    else:
        t_log.warning(
            "⚠️ Kolom 'FATID' tidak ditemukan di aset_data untuk standarisasi.")

    # Standarisasi ID Permohonan di user_data
    if 'ID Permohonan' in user_data.columns:
        initial_id_permohonan_count = user_data['ID Permohonan'].count()
        user_data['ID Permohonan'] = user_data['ID Permohonan'].astype(
            str).str.strip().fillna('')
        # user_data['ID Permohonan'] = user_data['ID Permohonan'].str.upper() # Contoh: standarisasi ke uppercase
        final_id_permohonan_count = (user_data['ID Permohonan'] != '').sum()
        t_log.info(
            f"✅ Kolom 'ID Permohonan' di user_data distandarisasi. Non-empty: {initial_id_permohonan_count} -> {final_id_permohonan_count}.")
    else:
        t_log.warning(
            "⚠️ Kolom 'ID Permohonan' tidak ditemukan di user_data untuk standarisasi.")

    # Standarisasi ID FAT di user_data (kolom Foreign Key pelanggan)
    if 'ID FAT' in user_data.columns:
        initial_id_fat_count = user_data['ID FAT'].count()
        user_data['ID FAT'] = user_data['ID FAT'].astype(
            str).str.strip().fillna('')
        # user_data['ID FAT'] = user_data['ID FAT'].str.upper() # Contoh: standarisasi ke uppercase (HARUS KONSISTEN DENGAN FATID)
        final_id_fat_count = (user_data['ID FAT'] != '').sum()
        t_log.info(
            f"✅ Kolom 'ID FAT' di user_data distandarisasi. Non-empty: {initial_id_fat_count} -> {final_id_fat_count}.")
    else:
        t_log.warning(
            "⚠️ Kolom 'ID FAT' tidak ditemukan di user_data untuk standarisasi.")

    t_log.info("✅ Pembersihan & Standarisasi Kolom ID Kunci selesai.")

    # ========================= Deduplikasi Awal berdasarkan ID Kunci =========================
    t_log.info("\n--- Memulai Deduplikasi Awal berdasarkan ID Kunci ---")

    # Deduplikasi aset_data berdasarkan 'FATID'
    if 'FATID' in aset_data.columns and not aset_data.empty:
        initial_aset_count = len(aset_data)
        # Hapus baris dengan FATID kosong sebelum deduplikasi jika itu dianggap invalid
        aset_data_dedup = aset_data[aset_data['FATID'] != ''].copy()
        rows_with_empty_fatid_aset = initial_aset_count - len(aset_data_dedup)
        if rows_with_empty_fatid_aset > 0:
            t_log.warning(
                f"⚠️ Menghapus {rows_with_empty_fatid_aset} baris di aset_data dengan 'FATID' kosong sebelum deduplikasi.")

        # Lakukan deduplikasi pada data yang tidak memiliki FATID kosong
        initial_count_before_dedup = len(aset_data_dedup)
        aset_data_dedup.drop_duplicates(
            subset=["FATID"], keep="first", inplace=True)
        deduplicated_count = len(aset_data_dedup)
        t_log.info(
            f"✅ Deduplikasi awal aset_data berdasarkan 'FATID': {initial_count_before_dedup} -> {deduplicated_count} baris. ({initial_count_before_dedup - deduplicated_count} duplikat dihapus).")
        aset_data = aset_data_dedup  # Ganti aset_data dengan hasil deduplikasi

    elif 'FATID' in aset_data.columns and aset_data.empty:
        t_log.info("ℹ️ aset_data kosong, deduplikasi dilewati.")

    else:
        t_log.warning(
            "⚠️ Kolom 'FATID' tidak ditemukan di aset_data. Melewatkan deduplikasi awal aset.")

    # Deduplikasi user_data berdasarkan 'ID Permohonan'
    if 'ID Permohonan' in user_data.columns and not user_data.empty:
        initial_user_count = len(user_data)
        # Hapus baris dengan ID Permohonan kosong sebelum deduplikasi jika itu dianggap invalid PK
        user_data_dedup = user_data[user_data['ID Permohonan'] != ''].copy()
        rows_with_empty_id_permohonan_user = initial_user_count - \
            len(user_data_dedup)
        if rows_with_empty_id_permohonan_user > 0:
            t_log.warning(
                f"⚠️ Menghapus {rows_with_empty_id_permohonan_user} baris di user_data dengan 'ID Permohonan' kosong sebelum deduplikasi.")

        # Lakukan deduplikasi
        initial_count_before_dedup_user = len(user_data_dedup)
        user_data_dedup = (
            user_data_dedup.sort_values(
                by=["SID", "Cust Name", "Koodinat Pelanggan"],
                ascending=False,
                na_position="last"
            )
            .drop_duplicates(subset=["ID Permohonan"], keep="first")
        )
        deduplicated_count_user = len(user_data_dedup)
        t_log.info(
            f"✅ Deduplikasi awal user_data berdasarkan 'ID Permohonan': {initial_count_before_dedup_user} -> {deduplicated_count_user} baris. ({initial_count_before_dedup_user - deduplicated_count_user} duplikat dihapus).")
        user_data = user_data_dedup

    elif 'ID Permohonan' in user_data.columns and user_data.empty:
        t_log.info("ℹ️ user_data kosong, deduplikasi dilewati.")
    else:
        t_log.warning(
            "⚠️ Kolom 'ID Permohonan' tidak ditemukan di user_data. Melewatkan deduplikasi awal user.")

    t_log.info("✅ Deduplikasi Awal selesai.")

    # ========================= Dropping Kolom Tertentu di user_data =========================
    t_log.info("\n--- Menghapus Kolom Tertentu di user_data ---")
    kolom_yang_dihapus = [
        # Asumsikan ini nama setelah clean_column_names
        'Koordinat FAT', 'Hostname OLT', 'FDT'
    ]
    # Cek apakah kolom ada sebelum dihapus
    cols_to_drop_exist = [
        col for col in kolom_yang_dihapus if col in user_data.columns]
    if cols_to_drop_exist:
        user_data.drop(columns=cols_to_drop_exist,
                       inplace=True, errors='ignore')
        t_log.info(f"✅ Kolom {cols_to_drop_exist} dihapus dari user_data.")
    else:
        t_log.info("ℹ️ Tidak ada kolom spesifik untuk dihapus di user_data.")

    t_log.info("✅ Proses Pembersihan Awal Selesai.")
    # ========================= Menyimpan DataFrame Hasil Pembersihan Awal ke File Sementara (Parquet) =========================
    t_log.info(
        "\n--- Menyimpan DataFrame Hasil Pembersihan Awal ke File Sementara ---")

    output_temp_dir = ti.temp_dir
    dfs_to_save = {
        "aset_data_initial_clean.parquet": aset_data,
        "user_data_initial_clean.parquet": user_data,
    }

    saved_file_paths = {}
    for file_name, df in dfs_to_save.items():
        # Periksa apakah DataFrame valid sebelum disimpan
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            t_log.warning(
                f"⚠️ DataFrame '{file_name}' kosong atau invalid. Tidak disimpan.")
            # Opsional: simpan file kosong sebagai placeholder jika task downstream memerlukannya ada
            try:
                empty_df = pd.DataFrame(
                    columns=df.columns) if df is not None else pd.DataFrame()
                file_path = os.path.join(output_temp_dir, file_name)
                empty_df.to_parquet(file_path, index=False)
                saved_file_paths[file_name] = file_path
                t_log.info(
                    f"✅ DataFrame kosong '{file_name}' disimpan sebagai placeholder ke {file_path}")
            except Exception as e:
                t_log.error(
                    f"❌ Gagal menyimpan placeholder untuk '{file_name}' ke {file_path}: {e}")
            continue

        # Buat path lengkap file output
        file_path = os.path.join(output_temp_dir, file_name)

        try:
            # Simpan DataFrame ke Parquet
            df.to_parquet(file_path, index=False)
            saved_file_paths[file_name] = file_path
            t_log.info(
                f"✅ DataFrame '{file_name}' ({len(df)} baris) disimpan ke {file_path}")
        except Exception as e:
            t_log.error(
                f"❌ Gagal menyimpan '{file_name}' ke {file_path}: {e}")
            raise  # Penting: Raise exception jika gagal menyimpan
    t_log.info(
        f"Task Load dan Pembersihan Awal selesai. Path file disimpan ke XCom: {saved_file_paths}")
    # Kembalikan dictionary path file yang disimpan
    return saved_file_paths


def transform_data_func():
    # 2d. Rename Kolom ke Format Final (snake_case)
    aset_data.rename(columns={
        # Kolom dari Tabel OLT
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

        # Kolom dari Tabel FDT
        "FDT ID": "fdt_id",
        "Status OSP AMARTA 1": "status_osp_amarta_fdt",
        "Jumlah Splitter FDT": "jumlah_splitter_fdt",
        "Kapasitas Splitter FDT": "kapasitas_splitter_fdt",
        "FDT New/Existing": "fdt_new_existing",
        "Port FDT": "port_fdt",
        "Koodinat FDT": "koordinat_fdt",

        # Kolom dari Tabel FAT
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
        "Status OSP AMARTA 2": "status_osp_amarta_fat",

        # Kolom dari Tabel Cluster
        "Cluster": "cluster",
        "Koordinat Cluster": "koordinat_cluster",
        "Area KP": "area_kp",
        "Kota/Kab": "kota_kab",
        "Kecamatan": "kecamatan",
        "Kelurahan": "kelurahan",
        "UP3": "up3",
        "ULP": "ulp",

        # Kolom dari Tabel Dokumentasi
        "LINK DOKUMEN FEEDER": "link_dokumen_feeder",
        "KETERANGAN DOKUMEN": "keterangan_dokumen",
        "LINK DATA ASET": "link_data_aset",
        "KETERANGAN DATA ASET": "keterangan_data_aset",
        "LINK MAPS": "link_maps",
        "UPDATE ASET": "update_aset",
        "AMARTA UPDATE": "amarta_update",

        # Kolom dari Tabel HomeConnected
        "HC OLD": "hc_old",
        "HC iCRM+": "hc_icrm",
        "TOTAL HC": "total_hc",
        "CLEANSING HP": "cleansing_hp",

        # Kolom dari Tabel AdditionalInformation
        "PA": "pa",
        "Tanggal RFS": "tanggal_rfs",
        "Mitra": "mitra",
        "Kategori": "kategori",
        "Sumber Datek": "sumber_datek"
    }, inplace=True)

    user_data.rename(columns={
        "SID": "sid",
        "ID Permohonan": "id_permohonan",
        "Koodinat Pelanggan": "koordinat_pelanggan",
        "Cust Name": "cust_name",
        "Telpn": "telpn",
        "ID FAT": "fat_id",
        "NOTES": "notes"
    }, inplace=True)

    t_log.info("✅ Rename kolom ke snake_case selesai.")
    if not aset_data.empty:  # Check added for safety
        try:
            if 'tanggal_rfs' in aset_data.columns:
                print(
                    "  -> Memproses kolom 'tanggal_rfs': Mengganti '0203' dengan '2023'.")
                aset_data['tanggal_rfs'] = aset_data['tanggal_rfs'].astype(
                    str)
                aset_data['tanggal_rfs'] = aset_data['tanggal_rfs'].str.replace(
                    '0203', '2023', regex=False)
                print("    ✅ Pembersihan string '0203' di 'tanggal_rfs' selesai.")
                # 2. Define the original dictionary with all intended types
            original_astype_dict_aset = {
                # Tabel OLT
                "hostname_olt": "string", "Koordinat_olt": "string", "brand_olt": "string", "type_olt": "string",
                "kapasitas_olt": "Int64",
                "kapasitas_port_olt": "Int64",
                "olt_port": "Int64",
                "olt": "string", "interface_olt": "string",
                # Tabel FDT
                "fdt_id": "string", "status_osp_amarta_fdt": "string", "jumlah_splitter_fdt": "Int64",
                "kapasitas_splitter_fdt": "Int64",
                "fdt_new_existing": "string", "port_fdt": "Int64",
                "koordinat_fdt": "string",
                # Tabel FAT
                "fat_id": "string", "jumlah_splitter_fat": "Int64",
                "kapasitas_splitter_fat": "Int64",
                "koordinat_fat": "string",
                "status_osp_amarta_fat": "string", "fat_kondisi": "string", "fat_filter_pemakaian": "string", "keterangan_full": "string",
                "fat_id_x": "string", "filter_fat_cap": "string",
                # Tabel Cluster
                "cluster": "string", "koordinat_cluster": "string", "area_kp": "string", "kota_kab": "string", "kecamatan": "string", "kelurahan": "string",
                "up3": "string", "ulp": "string",
                # Dokumentasi
                "link_dokumen_feeder": "string", "keterangan_dokumen": "string", "link_data_aset": "string", "keterangan_data_aset": "string",
                "link_maps": "string", "update_aset": "string", "amarta_update": "string",
                # HomeConnected
                "hc_old": "Int64",
                "hc_icrm": "Int64",
                "total_hc": "Int64",
                "cleansing_hp": "string",
                # Additional Info
                "pa": "string",
                "tanggal_rfs": "datetime64[ns]",
                "mitra": "string", "kategori": "string", "sumber_datek": "string"
            }
            # Identifikasi kolom-kolom yang ditargetkan untuk Int64
            int64_cols_to_process = [
                col for col, dtype in original_astype_dict_aset.items() if dtype == 'Int64']
            other_cols_to_astype = {
                col: dtype for col, dtype in original_astype_dict_aset.items() if dtype != 'Int64'}

            print("\n  --- Memproses Kolom Target Int64 ---")
            processed_int64_cols = []  # Untuk melacak kolom yang berhasil diproses
            for col in int64_cols_to_process:
                if col in aset_data.columns:
                    t_log.info(
                        f"  -> Memproses kolom '{col}': Membersihkan dan konversi ke Int64.")
                    try:
                        cleaned_series = aset_data[col].astype(str).copy()
                        cleaned_series = cleaned_series.str.replace(
                            '[., ]', '', regex=True).str.strip()

                        # Konversi ke Int64 (ini juga menangani NaNs dari coerce)
                        numeric_series = pd.to_numeric(
                            cleaned_series, errors='coerce')
                        aset_data[col] = numeric_series.astype('Int64')

                        print(
                            f"    ✅ Pembersihan dan konversi '{col}' ke Int64 selesai.")
                        # Tandai kolom ini berhasil diproses
                        processed_int64_cols.append(col)

                    except Exception as col_e:
                        t_log.error(
                            f"    ❌ Gagal memproses kolom '{col}' secara spesifik: {col_e}")
                        t_log.info(
                            f"    ℹ️ Kolom '{col}' mungkin tidak dikonversi ke Int64 karena error.")

            t_log.info("\n  --- Mengonversi Kolom Lainnya ---")
            valid_astype_dict_aset = {}
            missing_cols_aset = []
            for col, dtype in other_cols_to_astype.items():
                if col in aset_data.columns:
                    valid_astype_dict_aset[col] = dtype
                else:
                    missing_cols_aset.append(col)

            if missing_cols_aset:
                print(
                    f"    ⚠️ Kolom berikut tidak ditemukan di aset_data dan dilewati dalam konversi tipe data umum: {missing_cols_aset}")

            if valid_astype_dict_aset:
                print(
                    "  -> Mengonversi tipe data kolom aset_data lainnya menggunakan astype...")
                try:
                    aset_data = aset_data.astype(valid_astype_dict_aset)
                    print(
                        "  ✅ Konversi tipe data aset_data (selain Int64 yang sudah ditangani) selesai.")
                except Exception as astype_e:
                    # Menangkap error spesifik yang mungkin terjadi di langkah astype()
                    print(
                        f"❌ Error saat konversi tipe data aset_data pada langkah astype() untuk kolom-kolom tersisa: {astype_e}")
                    print(
                        "    ℹ️ Pastikan format data di kolom yang tersisa sesuai dengan tipe data target di dictionary.")
            else:
                print(
                    "  ℹ️ Tidak ada kolom lain di aset_data yang perlu dikonversi via astype.")
            t_log.info("✅ Konversi tipe data aset_data selesai.")
        except Exception as e:
            t_log.error(f"Error konversi tipe data aset_data: {e}")
            raise

    try:
        if 'user_data' in locals() and isinstance(user_data, pd.DataFrame) and not user_data.empty:
            t_log.info("  -> Mengonversi kolom user_data...")
            try:
                astype_dict_user = {
                    "sid": "string", "id_permohonan": "string", "koordinat_pelanggan": "string",
                    "cust_name": "string", "telpn": "string", "fat_id": "string",
                    "fdt": "string", "notes": "string"
                }
                valid_astype_dict_user = {}
                missing_cols_user = []
                for col, dtype in astype_dict_user.items():
                    if col in user_data.columns:
                        valid_astype_dict_user[col] = dtype
                    else:
                        missing_cols_user.append(col)

                if missing_cols_user:
                    print(
                        f"    ⚠️ Kolom berikut tidak ditemukan di user_data dan dilewati dalam konversi tipe data: {missing_cols_user}")

                if valid_astype_dict_user:
                    user_data = user_data.astype(valid_astype_dict_user)
                    print("  ✅ Konversi tipe data user_data selesai.")
                else:
                    print(
                        "  ℹ️ Tidak ada kolom di user_data yang perlu dikonversi via astype.")

            except Exception as e:
                print(f"❌ Error saat konversi tipe data user_data: {e}")

        elif 'user_data' in locals() and isinstance(user_data, pd.DataFrame) and user_data.empty:
            print("\n⚠️ DataFrame user_data kosong, konversi tipe data dilewati.")

    except NameError:
        print(
            "\nℹ️ Variabel user_data tidak terdefinisi. Konversi tipe data untuk user_data dilewati.")

    t_log.info("✅ Proses Konversi Tipe Data Selesai.")
    t_log.info("  -> Memproses kolom koordinat OLT, FDT, Pelanggan...")
    try:
        coordinate_cols_map = {
            'koordinat_olt': ('latitude_olt', 'longitude_olt'),
            'koordinat_fdt': ('latitude_fdt', 'longitude_fdt'),
            'koordinat_fat': ('latitude_fat', 'longitude_fat'),
            'koordinat_cluster': ('latitude_cluster', 'longitude_cluster'),
        }
        for coord_col, (lat_col, lon_col) in coordinate_cols_map.items():
            if coord_col == 'koordinat_pelanggan':
                continue

            # Panggil fungsi reusable untuk membersihkan kolom koordinat di aset_data
            if not aset_data.empty:
                aset_data = clean_coordinate_column(
                    aset_data, coord_col, lat_col, lon_col, t_log)

        # Memproses user_data khusus untuk kolom koordinat pelanggan
        if 'koordinat_pelanggan' in user_data.columns and not user_data.empty:
            t_log.info("Memproses koordinat pelanggan di user_data...")
            try:
                user_data = clean_coordinate_column(
                    user_data, 'koordinat_pelanggan', 'latitude_pelanggan', 'longitude_pelanggan', t_log)
                t_log.info("✅ Cleansing koordinat pelanggan selesai.")
            except Exception as e:
                t_log.error(f"❌ Gagal memproses koordinat pelanggan: {e}")
                raise

    except Exception as e:
        t_log.error(f"Error saat memproses kolom koordinat: {e}")
        raise
    t_log.info("✅ Proses Cleansing Koordinat Selesai.")


def split_validate_data_func(ti):
    t_log.info("\n--- Memulai Proses Splitting dan Validasi ---")
    # ========================= Split Data dari aset_data =========================
    print("\n=== Splitting Data from aset_data ===")

    def clean_fat_id(df):
        """Fungsi untuk membersihkan kolom fat_id"""
        if 'fat_id' in df.columns:
            df['fat_id'] = (
                df['fat_id']
                .astype(str)
                .str.strip()
                .str.upper()
                .str.replace(r'[\s\t\-]+', '', regex=True)
            )
        return df

    # Fungsi untuk memproses kolom fat_id yang memiliki format range
    def expand_fat_id_ranges(df):
        # Buat list untuk menyimpan baris yang sudah di-expand
        expanded_rows = []

        # Iterasi melalui setiap baris di DataFrame
        for _, row in df.iterrows():
            fat_id = str(row['fat_id']).strip(
            ) if pd.notna(row['fat_id']) else ''

            # Jika fat_id memiliki format range (mengandung '-')
            if '-' in fat_id:
                # Split menjadi dua bagian
                id_parts = [part.strip()
                            for part in fat_id.split('-') if part.strip()]

                # Jika format range valid (ada dua bagian setelah split)
                if len(id_parts) == 2:
                    # Buat dua baris baru dengan fat_id yang berbeda
                    row1 = row.copy()
                    row1['fat_id'] = id_parts[0]
                    expanded_rows.append(row1)

                    row2 = row.copy()
                    row2['fat_id'] = id_parts[1]
                    expanded_rows.append(row2)
                else:
                    # Jika format tidak valid, simpan baris asli
                    expanded_rows.append(row)
            else:
                # Jika bukan format range, simpan baris asli
                expanded_rows.append(row)

        # Buat DataFrame baru dari baris yang sudah di-expand
        return pd.DataFrame(expanded_rows)

    # Bersihkan dan expand fat_id di aset_data
    aset_data = clean_fat_id(aset_data)
    aset_data = expand_fat_id_ranges(aset_data)
    aset_data.drop_duplicates(subset='fat_id', keep="first", inplace=True)
    print("✅ Split user_terminals from aset_data (including expanding fat_id ranges).")

    # 1. Split Data untuk Tabel user_terminals
    user_terminals = aset_data[[
        "hostname_olt", "latitude_olt", "longitude_olt", "brand_olt", "type_olt",
        "kapasitas_olt", "kapasitas_port_olt", "olt_port", "olt", "interface_olt",
        "fdt_id", "status_osp_amarta_fdt", "jumlah_splitter_fdt", "kapasitas_splitter_fdt",
        "fdt_new_existing", "port_fdt", "latitude_fdt", "longitude_fdt",
        "fat_id", "jumlah_splitter_fat", "kapasitas_splitter_fat", "latitude_fat", "longitude_fat",
        "status_osp_amarta_fat", "fat_kondisi", "fat_filter_pemakaian", "keterangan_full",
        "fat_id_x", "filter_fat_cap"
    ]].copy()
    user_terminals = clean_fat_id(user_terminals)
    print("✅ Split user_terminals from aset_data.")

    # 2-5. Split Data untuk tabel lainnya
    tables = {
        "cluster_data": ["latitude_cluster", "longitude_cluster", "area_kp", "kota_kab", "kecamatan", "kelurahan", "up3", "ulp", "fat_id"],
        "home_connected_data": ["hc_old", "hc_icrm", "total_hc", "cleansing_hp", "fat_id"],
        "dokumentasi_data": ["status_osp_amarta_fat", "link_dokumen_feeder", "keterangan_dokumen", "link_data_aset", "keterangan_data_aset", "link_maps", "update_aset", "amarta_update", "fat_id"],
        "additional_info_data": ["pa", "tanggal_rfs", "mitra", "kategori", "sumber_datek", "fat_id"]
    }

    for name, cols in tables.items():
        globals()[name] = aset_data[cols].copy()
        globals()[name] = clean_fat_id(globals()[name])
        print(f"✅ Split {name} from aset_data.")

    # ========================= Split dan Filter Data dari user_data =========================
    print("\n=== Splitting dan Filtering Data from user_data ===")

    pelanggan_data = user_data[[
        "id_permohonan", "sid", "cust_name", "telpn",
        "latitude_pelanggan", "longitude_pelanggan", "fat_id", "notes"
    ]].copy()
    pelanggan_data = clean_fat_id(pelanggan_data)
    print("✅ Split pelanggan_data from user_data.")

    # Validasi fat_id
    if 'fat_id' in user_terminals.columns:
        # Dapatkan semua fat_id valid dari user_terminals
        valid_fat_ids = set(user_terminals['fat_id'].unique())
        print(
            f"ℹ️ Ditemukan {len(valid_fat_ids)} unique fat_ids di user_terminals.")

        # Filter pelanggan_data
        valid_pelanggan_data = pelanggan_data[pelanggan_data['fat_id'].isin(
            valid_fat_ids)].copy()
        invalid_pelanggan_data = pelanggan_data[~pelanggan_data['fat_id'].isin(
            valid_fat_ids)].copy()

        print(
            f"✅ {len(valid_pelanggan_data)} baris valid, {len(invalid_pelanggan_data)} baris invalid.")

        if not invalid_pelanggan_data.empty:
            print("⚠️ Contoh fat_id tidak valid:",
                  invalid_pelanggan_data['fat_id'].unique()[:10])
    else:
        print("❌ Kolom 'fat_id' tidak ditemukan di user_terminals")
        valid_pelanggan_data = pd.DataFrame(columns=pelanggan_data.columns)
        invalid_pelanggan_data = pelanggan_data.copy()

    # Cleaning tambahan untuk pelanggan
    valid_pelanggan_data['id_permohonan'] = valid_pelanggan_data['id_permohonan'].astype(
        str).str.strip()
    valid_pelanggan_data.drop_duplicates(
        subset='id_permohonan', keep="first", inplace=True)

    # ========================= Final Validation =========================
    print("\n=== Final Validation ===")

    # Pastikan tidak ada fat_id yang tidak valid
    missing_fat_ids = set(
        valid_pelanggan_data['fat_id']) - set(user_terminals['fat_id'])
    if missing_fat_ids:
        print(f"❌ Masih ada {len(missing_fat_ids)} fat_id yang tidak valid!")
        print("Contoh:", list(missing_fat_ids)[:5])
    else:
        print("✅ Semua fat_id di valid_pelanggan_data valid!")

    run_id = ti.run_id
    output_temp_dir = os.path.join(temp_dir, run_id)
    os.makedirs(output_temp_dir, exist_ok=True)
    t_log.info(f"\nMenyimpan file sementara ke: {output_temp_dir}")
    dfs_to_save = {
        "user_terminals.parquet": user_terminals,
        "clusters.parquet": cluster_data,
        "home_connecteds.parquet": home_connected_data,
        "dokumentasis.parquet": dokumentasi_data,
        "additional_informations.parquet": additional_info_data,
        "pelanggans.parquet": valid_pelanggan_data,
    }

    saved_file_paths = {}
    for file_name, df in dfs_to_save.items():
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            t_log.warning(
                f"DataFrame '{file_name}' kosong/invalid. Tidak disimpan.")
            continue

        file_path = os.path.join(output_temp_dir, file_name)
        try:
            df.to_parquet(file_path, index=False)
            saved_file_paths[file_name] = file_path
            t_log.info(
                f"DataFrame '{file_name}' ({len(df)} baris) disimpan ke {file_path}")
        except Exception as e:
            t_log.error(
                f"Gagal menyimpan '{file_name}' ke {file_path}: {e}")
            raise
    t_log.info(f"Path file disimpan ke XCom: {saved_file_paths}")
    return saved_file_paths


@task(task_id='load_to_supabase')
def load_to_supabase_func(
    postgres_conn_id: str,
    saved_dataframe_paths: dict  # Menerima dari XCom task sebelumnya
):
    """Membaca Parquet sementara dan mengimpor ke database Supabase."""
    t_log.info(
        f"Memulai task Load to Supabase. Koneksi='{postgres_conn_id}'.")

    if not saved_dataframe_paths or not isinstance(saved_dataframe_paths, dict):
        t_log.error("Tidak ada path file DataFrame yang diterima.")
        raise ValueError("Tidak ada path file DataFrame yang diterima.")

    t_log.info(f"Path file untuk diimpor: {saved_dataframe_paths}")

    try:
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        engine = hook.get_sqlalchemy_engine()
        t_log.info("SQLAlchemy engine dibuat.")
    except Exception as e:
        t_log.error(f"Gagal membuat SQLAlchemy engine: {e}")
        raise

    import_order_files = [
        {"file_name": "user_terminals.parquet",
            "table_name": "user_terminals"},
        {"file_name": "clusters.parquet", "table_name": "clusters"},
        {"file_name": "home_connecteds.parquet",
            "table_name": "home_connecteds"},
        {"file_name": "dokumentasis.parquet", "table_name": "dokumentasis"},
        {"file_name": "additional_informations.parquet",
            "table_name": "additional_informations"},
        {"file_name": "pelanggans.parquet", "table_name": "pelanggans"},
    ]

    if_exists_behavior = 'append'  # Atau 'replace'

    t_log.info(
        f"\n--- Memulai Impor DataFrames ke Database (if_exists='{if_exists_behavior}') ---")

    for item in import_order_files:
        file_name = item["file_name"]
        table_name = item["table_name"]
        file_path = saved_dataframe_paths.get(file_name)

        if not file_path or not os.path.exists(file_path):
            t_log.warning(
                f"File '{file_name}' tidak ada. Melewatkan impor ke '{table_name}'.")
            continue

        t_log.info(
            f"Membaca '{file_name}' dan mengimpor ke '{table_name}'...")

        try:
            df_to_import = pd.read_parquet(file_path)
            t_log.info(f"DataFrame dibaca ({len(df_to_import)} baris).")

            if df_to_import.empty:
                t_log.warning(
                    f"DataFrame '{file_name}' kosong. Melewatkan impor.")
                continue

            # --- Impor menggunakan to_sql ---
            df_to_import.to_sql(
                name=table_name,
                con=engine,
                if_exists=if_exists_behavior,
                index=False,
                # dtype: Opsional, mapping tipe data eksplisit
            )
            t_log.info(
                f"Berhasil mengimpor {len(df_to_import)} baris ke '{table_name}'.")

        except Exception as e:
            t_log.error(
                f"Gagal mengimpor '{file_name}' ke '{table_name}': {e}")
            # import traceback; traceback.print_exc() # Opsional: Log traceback
            # raise # Uncomment jika *setiap* kegagalan impor harus menggagalkan task

    t_log.info("\n=== Proses Impor Selesai ===")

# ========================= MEMANGGIL TASKS DAN DEFINISI DEPENDENSI =========================
# Panggil fungsi @task yang sudah didefinisikan.


extract_clean_split_save_instance = extract_clean_split_save_task(
    aset_spreadsheet_id="1bbXV377Gu4MxJzbRWxFr_kbSZyzyP80kCyhR80TDWzQ",  # GANTI
    aset_sheet_name="Datek Aset All",
    user_spreadsheet_id="1LMyZprJ_w3X6DC7Jqu0CpJaVQ3u8VUEMvSEADv1EMjc",  # GANTI
    user_sheet_name="Data All",
    google_credentials_env_var_name=GOOGLE_CREDENTIALS_ENV_VAR_NAME,
    temp_dir=TEMP_DATA_DIR,
)

# Panggil Task Create Database Tables (Jika DDL di DAG)
create_db_tables_instance = create_table_if_not_exists(
    postgres_conn_id=POSTGRES_CONN_ID,
    sql_dir=SQL_DIR_PATH,
    sql_file_name=SQL_CREATE_TABLES_FILE_NAME,
)

# Panggil Task Load to Supabase
load_to_supabase_instance = load_to_supabase_task(
    postgres_conn_id=POSTGRES_CONN_ID,
    # saved_dataframe_paths=extract_clean_split_save_instance # TaskFlow otomatis meneruskan output
)

# Panggil Task Cleanup (Opsional)
# cleanup_instance = cleanup_temp_files_task(...)

# --- Definisikan Dependencies ---
# >> cleanup_instance # Tambahkan jika ada cleanup
extract_clean_split_save_instance >> create_db_tables_instance >> load_to_supabase_instance

# Jika tanpa Task DDL:
# extract_clean_split_save_instance >> load_to_supabase_instance # >> cleanup_instance


# ========================= INSTANTIATE DAG =========================
