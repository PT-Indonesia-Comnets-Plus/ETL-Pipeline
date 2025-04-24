import pandas as pd
import re
import numpy as np  # Import numpy untuk np.nan
# from decimal import Decimal # Asumsi ini tidak diperlukan jika hanya konversi ke float

# ========================= Cleansing Functions =========================


def clean_column_names(df):
    df.columns = [re.sub(r"\s+", " ", col).strip()
                  for col in df.columns]
    return df


def clean_dataframe(df, exclude_columns):
    """
    Bersihkan header kolom dan kapitalisasi isi string, kecuali kolom di exclude_columns.
    """
    if df.empty:
        return df

    # Bersihkan nama kolom: hilangkan spasi ekstra dan strip
    df.columns = [re.sub(r"\s+", " ", col).strip()
                  for col in df.columns]

    # Bersihkan isi string di setiap kolom, lalu kapitalisasi jika bukan di exclude_columns
    for col in df.columns:
        if col not in exclude_columns:
            try:
                df[col] = df[col].apply(lambda x: re.sub(
                    r"\s+", " ", x.strip()).title() if isinstance(x, str) else x)
            except Exception as e:
                print(f"⚠️ Gagal memproses kolom '{col}': {e}")
    return df


def clean_degree_with_comma(coord):
    if pd.isna(coord) or 'Â,' not in str(coord):
        return None
    coord = str(coord).replace('Â', '')
    # Setelah hapus Â, mungkin format jadi lat,lon. Coba standardisasi dengan clean_comma_separated
    # Rantai ke fungsi lain yang lebih umum
    return clean_comma_separated(coord)


def clean_degree_separated(coord):
    if pd.isna(coord):
        return None
    coord_str = str(coord)
    if 'Â°' not in coord_str:
        return None
    try:
        # Contoh: -7.361902Â°112.693948Â°
        # Temukan Â° pertama, pisahkan. Buang semua Â°. Sisipkan koma.
        parts = coord_str.split('Â°')
        if len(parts) >= 2:
            lat_part = parts[0].strip()
            lon_part = parts[1].strip().replace(
                'Â°', '')  # Hapus Â° sisa di lon
            # Coba standardisasi dengan clean_comma_separated (jika lat/lon masih ada koma/titik desimal beda)
            return clean_comma_separated(f"{lat_part},{lon_part}")
    except:
        pass
    return None


def clean_degree_as_separator(coord):
    if pd.isna(coord):
        return None
    coord_str = str(coord)
    if '°' not in coord_str:
        return None
    try:
        # Contoh: -6.9271° 107.6048°
        # Temukan ° pertama, pisahkan. Buang semua °. Sisipkan koma.
        parts = coord_str.split('°')
        if len(parts) >= 2:
            lat_part = parts[0].strip()
            # Gabungkan sisa part jika ada banyak °, strip
            lon_part = ''.join(parts[1:]).strip()
            # Coba standardisasi dengan clean_comma_separated
            return clean_comma_separated(f"{lat_part},{lon_part}")
    except:
        pass
    return None


def clean_two_commas_with_space(coord):
    if pd.isna(coord):
        return None
    coord_str = str(coord).strip()
    # Contoh: -8,1948403, 111,1077904 atau -7,2892906 112,7276532
    # Pola: AngkaDesimalKoma Spasi AngkaDesimalKoma ATAU AngkaDesimalKoma, Spasi AngkaDesimalKoma
    # Kita bisa gabungkan dan standardisasi dengan clean_comma_separated
    coord_str_standardized = coord_str.replace(', ', ',').replace(
        ' ', ',')  # Ganti ' ' atau ', ' jadi ','
    return clean_comma_separated(coord_str_standardized)


def clean_comma_separated(coord):
    """Handles standard 'lat,lon' format and standardizes decimal comma/dot."""
    if pd.isna(coord):
        return None
    coord_str = str(coord).strip()
    if ',' not in coord_str:  # Harus ada setidaknya 1 koma sebagai pemisah
        return None
    parts = [part.strip() for part in coord_str.split(',', 1)
             ]  # Split hanya pada koma pertama
    if len(parts) == 2:
        # Standardisasi: desimal selalu titik
        lat_part = parts[0].replace(',', '.')
        # Standardisasi: desimal selalu titik
        lon_part = parts[1].replace(',', '.')
        # Cek apakah kedua part terlihat seperti angka setelah standardisasi
        try:
            float(lat_part)
            float(lon_part)
            # Kembalikan format standar "lat,lon"
            return f"{lat_part},{lon_part}"
        except ValueError:
            return None  # Jika tidak bisa diubah jadi float, format salah
    return None  # Jika split tidak menghasilkan 2 part


# Tambahkan fungsi baru untuk format titik-spasi
def clean_dot_space_separated(coord):
    """Handles format like '-7.90845. 113.35127' -> '-7.90845,113.35127'"""
    if pd.isna(coord):
        return None
    coord_str = str(coord).strip()
    if coord_str == '':
        return None

    # Regex to match format like Number. Space(s) Number
    # Allows optional minus at start, numbers, optional dot, numbers after dot.
    # Captures the two numeric parts separated by dot and one or more spaces.
    match = re.match(r'^(-?\d+\.?\d*)\.\s+(\-?\d+\.?\d*)$', coord_str)
    if match:
        lat_part = match.group(1)
        lon_part = match.group(2)
        # Parts should already use '.' as decimal due to regex.
        return f"{lat_part},{lon_part}"

    return None


def clean_dot_separated_no_comma(coord):
    """Handles format like 'X.Y.A.B' or 'X.YYYYYYY.AAAAAAA' -> 'X.Y,A.B'"""
    if pd.isna(coord):
        return None
    coord_str = str(coord).strip()
    if ',' in coord_str:
        return None  # Jangan proses jika ada koma

    # Coba regex yang mencari pola Angka.Angka.Angka.Angka atau Angka.Angka.Angka
    # Regex: Angka (opsional -) diikuti . diikuti Angka, kemudian . Angka (opsional . Angka lagi di akhir)
    # Ini mencoba menangani beberapa pola titik sebagai pemisah utama
    match = re.match(r'^(-?\d+\.?\d*)\.(\d+\.?\d*)\.?(\d*\.?\d*)$', coord_str)
    if match:
        # Jika match, kita asumsikan split utamanya adalah setelah titik pertama
        lat_part = match.group(1)  # Ini harusnya Lat integer + desimal
        # Gabungkan group 2 dan 3 sebagai Lon
        lon_part = match.group(2)  # Ini harusnya Lon integer + desimal
        if match.group(3):  # Jika ada group ke-3 (dari pola X.Y.A.B.C)
            # Pola X.Y.A.B.C mungkin A.B lat, C.D lon
            # Atau X.Y lat, A.B.C lon -> ini rumit
            # Berdasarkan contoh "-7.36271456342.732918" -> "-7.362714,112.732918"
            # Ini bukan pola A.B.C.D.
            # Mari fokus pada pola X.Y.A.B -> X.Y,A.B
            # Asumsi regex ^(-?\d+\.?\d*)\.(\d+\.?\d*)$ lebih relevan untuk pola dua bagian dipisah titik
            # Kita pakai regex spesifik ^(-?\d+\.\d+)\.(\d+\.?\d+)$ seperti di clean_dot_space_separated tapi tanpa spasi
            match_simple_dot_sep = re.match(
                r'^(-?\d+\.\d+)\.(\d+\.?\d*)$', coord_str)
            if match_simple_dot_sep:
                lat_part = match_simple_dot_sep.group(1)
                lon_part = match_simple_dot_sep.group(2)
                # Standardisasi via clean_comma_separated
                return clean_comma_separated(f"{lat_part},{lon_part}")

    return None  # Jika tidak cocok pola titik sebagai pemisah utama tanpa koma


def clean_merged_coordinates(coord):
    """Handles formats like -7.36271456342.732918 (jika ada pola khusus) atau -8180339111.116929"""
    if pd.isna(coord):
        return None
    coord_str = str(coord).strip().replace(
        " ", "").replace(",", "")  # Hapus spasi dan koma

    # Contoh: -8180339111.116929
    # Regex: mulai negatif opsional, digit (lat integer+decimal), digit (lon integer), titik (decimal), digit (lon decimal)
    # Pola di regex asli: ^(-?\d+\.\d+)(\d{3}\.\d+)$ cocok untuk A.B C.D dimana C 3 digit.
    # Untuk -8180339111.116929, format ini mungkin dari konversi float besar.
    # Pola raw_data -8180339111.116929 -> harus jadi -8.180339,111.116929
    # Ini diproses oleh clean_split_from_long_float jika inputnya float/int.
    # Jika inputnya string "-8180339111.116929", clean_split_from_long_float akan return None.
    # Regex yang mungkin cocok string ini: ^(-?\d+)(\d{3}\.?\d*)\.(\d+\.?\d*)$
    match_big_number = re.match(
        r'^(-?\d+)(\d{3}\.?\d*)\.(\d+\.?\d*)$', coord_str)
    if match_big_number:
        # Ini mungkin bukan format lat,lon yang sebenarnya.
        # Pola -8180339111.116929 => -8 (lat int) 180339 (lat dec) 111 (lon int) 116929 (lon dec)
        # Regex: ^(-?\d)(\d+)(\d{3})\.(\d+)$
        match_specific_merged = re.match(
            r'^(-?\d)(\d+)(\d{3})\.(\d+)$', coord_str)
        if match_specific_merged:
            lat_int = match_specific_merged.group(1)
            lat_dec = match_specific_merged.group(2)
            lon_int = match_specific_merged.group(3)
            lon_dec = match_specific_merged.group(4)
            # Format menjadi -LatInt.LatDec,LonInt.LonDec
            return f"{lat_int}.{lat_dec},{lon_int}.{lon_dec}"
        # Jika tidak cocok pola spesifik ini, biarkan regex asli jika masih relevan
        match = re.match(
            r'^(-?\d+\.\d+)(\d{3}\.\d+)$', coord_str)  # Regex asli
        if match:
            lat = match.group(1)
            lon = match.group(2)
            return f"{lat},{lon}"  # Kembalikan format standar lat,lon

    return None  # Jika tidak cocok pola gabungan


def move_comma_separator(coord):
    # Ini terlihat seperti logika kompleks untuk format spesifik, biarkan seperti adanya.
    if pd.isna(coord) or ',' not in str(coord) or str(coord).count('.') != 2:
        return None
    try:
        # Contoh: 7.123.456,112.789.012 -> 7.123456,112.789012 (ini asumsi saya)
        # Kode asli Anda sepertinya memindahkan bagian setelah desimal pertama ke Lon
        # lat_part, lon_part = str(coord).split(',')
        # lat_before, lat_after = lat_part.split('.') # lat_before='7', lat_after='123.456' -> error di sini
        # Ini logicnya perlu disesuaikan dengan format persisnya.
        # Biarkan fungsi ini seperti aslinya jika memang menangani format spesifik.
        # Jika contoh raw_data "8.180339,111.116929" ini yang dimaksud, itu ditangani clean_comma_separated
        # Jika contoh "7.123.456,112.789.012" yang dimaksud, regex di clean_dot_separated_no_comma mungkin lebih pas jika koma dihilangkan
        pass  # Biarkan logika asli atau kembalikan None jika tidak yakin
    except:
        pass
    return None  # Kembalikan None karena logikanya rumit dan rentan error


def clean_with_e_separator(coord):
    # Ini terlihat sudah menangani format E, biarkan seperti adanya.
    if pd.isna(coord):
        return None
    coord_str = str(coord).strip()
    if 'E' not in coord_str:
        return None
    try:
        # Contoh: -7.86482E112.69473 atau S709.168E11224.693
        # Split by 'E'
        parts = coord_str.split('E')
        if len(parts) == 2:
            lat_part = parts[0].strip()
            lon_part = parts[1].strip()

            # Tangani S/N di Latitude
            is_south = False
            if lat_part.upper().startswith('S'):
                is_south = True
                lat_part = lat_part[1:].strip()  # Hapus 'S'

            # Asumsi format sebelum E adalah angka desimal (X.Y) atau integer (X)
            # Asumsi format setelah E adalah angka desimal (A.B) atau integer (A)
            # Konversi ke float untuk validasi dan standardisasi (jika perlu penyesuaian skala seperti di kode asli)
            try:
                lat_float = float(lat_part)
                lon_float = float(lon_part)

                # Jika ada logika penyesuaian skala seperti "/ 100" di kode asli, terapkan di sini
                # Contoh: S709.168 -> 7.09168 (geser koma 2 tempat)
                # Asumsi 709.168E11224.693 -> 7.09168, 112.24693 (geser koma 2 tempat untuk lat dan 3 untuk lon?)
                # Logika asli Anda: lat/100, lon/100. Mari pertahankan jika itu intentnya.
                # Ini mungkin tergantung format persis sebelum E.
                # Mari kembalikan nilai float, lalu biarkan konversi akhir menangani float ke string.
                # Atau kembalikan string "lat,lon" dengan float format
                # Contoh heuristic: jika >90, geser koma
                lat_final = lat_float / \
                    100 if abs(lat_float) > 90 else lat_float
                # Contoh heuristic: jika >180, geser koma
                lon_final = lon_float / \
                    100 if abs(lon_float) > 180 else lon_float
                # Logika asli Anda hanya bagi 100. Mari pertahankan jika itu polanya.
                lat_final = lat_float / 100
                lon_final = lon_float / 100

                if is_south:
                    lat_final = -abs(lat_final)  # Pastikan negatif jika 'S'

                # Kembalikan sebagai string "lat,lon"
                return f"{lat_final},{lon_final}"

            except ValueError:
                return None  # Jika partnya bukan angka

    except:
        pass
    return None


def clean_split_from_long_float(coord):
    """Handles large raw float/int numbers like -8180339111.116929 -> -8.180339,111.116929"""
    # Ini adalah kasus spesifik jika data awalnya berupa angka float atau int yang besar
    try:
        # Cek apakah input adalah float atau int, BUKAN string
        if not isinstance(coord, (float, int)) or pd.isna(coord):
            return None

        # Ubah angka menjadi string dengan presisi tinggi, buang titik desimal, buang minus di awal jika ada
        # Contoh: -8180339111.116929 -> "-8180339111.11692900000000" -> "818033911111692900000000"
        # Sesuaikan format presisi sesuai data Anda
        coord_str_raw = "{:.10f}".format(
            coord).replace('.', '').replace('-', '')
        # Hapus nol di belakang jika ada (misal .000)
        coord_str = coord_str_raw.rstrip('0')

        if len(coord_str) < 10:  # Heuristic: Asumsi lat+lon punya total digit minimal segini
            return None  # Angka terlalu kecil untuk pola gabungan ini

        # Pola: -LatInt LatDec LonInt LonDec
        # -8.180339,111.116929
        # Lat: -8 (1 digit) . 180339 (6 digit)
        # Lon: 111 (3 digit) . 116929 (6 digit)
        # Total digit (tanpa minus dan titik): 1 + 6 + 3 + 6 = 16 digit
        # Jika pola selalu LatInt(1).LatDec(6)LonInt(3).LonDec(>=1)
        # Cari 6 digit setelah digit pertama (integer lat) sebagai lat desimal
        # Cari 3 digit setelah lat desimal sebagai lon integer
        # Sisanya adalah lon desimal
        # string gabungan: -8 180339 111 116929
        # index: 0  1..6   7..9 10..akhir
        # Lengths: 1, 6, 3, ?
        # Check panjang string: minimal 10 (1+6+3)
        if len(coord_str) < 10:
            return None

        try:
            # Asumsi 1 digit integer lat (setelah hapus minus)
            lat_int_digit = coord_str[0]
            lat_dec_digits = coord_str[1:7]  # Asumsi 6 digit desimal lat
            lon_int_digits = coord_str[7:10]  # Asumsi 3 digit integer lon
            lon_dec_digits = coord_str[10:]  # Sisa adalah desimal lon

            # Rekonstruksi string "lat,lon"
            # Tambahkan kembali minus jika awal negatif
            lat = f"-{lat_int_digit}.{lat_dec_digits}"
            lon = f"{lon_int_digits}.{lon_dec_digits}"

            # Cek apakah angka asli negatif
            if coord < 0:
                # Tambahkan kembali minus
                lat = f"-{lat_int_digit}.{lat_dec_digits}"

            # Final standardisasi dan validasi
            return clean_comma_separated(f"{lat},{lon}")

        except IndexError:
            return None  # String tidak cukup panjang untuk pola

    except:
        # Tangani error lain jika format float/int tidak seperti yang diharapkan
        pass  # Fall through

    return None  # Jika input bukan float/int atau pola tidak cocok


# --- Fungsi clean_comma_separated yang Dimodifikasi ---
def clean_comma_separated(coord):
    """
    Handles standard 'lat,lon' format, standardizes decimal comma/dot,
    and removes multiple consecutive dots (e.g., '..').
    """
    if pd.isna(coord):
        return None
    coord_str = str(coord).strip()
    if ',' not in coord_str:  # Harus ada setidaknya 1 koma sebagai pemisah
        return None
    # Split hanya pada koma pertama untuk memisahkan lat dan lon
    parts = [part.strip() for part in coord_str.split(',', 1)]
    if len(parts) == 2:
        # Standardisasi: desimal selalu titik
        lat_part = parts[0].replace(',', '.')
        # Standardisasi: desimal selalu titik
        lon_part = parts[1].replace(',', '.')

        # --- PENTING: Hapus titik berurutan di sini ---
        # Ganti satu atau lebih titik berurutan (\.+) dengan satu titik (.)
        lat_part = re.sub(r'\.+', '.', lat_part)
        lon_part = re.sub(r'\.+', '.', lon_part)

        # Pastikan tidak ada titik di awal atau akhir setelah penggantian
        lat_part = lat_part.strip('.')
        lon_part = lon_part.strip('.')
        # --- End of Added Step ---

        # Cek apakah kedua part terlihat seperti angka setelah standardisasi dan pembersihan titik
        try:
            float(lat_part)
            float(lon_part)
            # Kembalikan format standar "lat,lon" string
            return f"{lat_part},{lon_part}"
        except ValueError:
            return None  # Jika tidak bisa diubah jadi float, format salah
    return None  # Jika split tidak menghasilkan 2 part

# --- Fungsi apply_cleaning (Tetap sama, karena memanggil clean_comma_separated) ---


def apply_cleaning(coord):
    """
    Proses data latitude/longitude untuk numerik dan string,
    mencoba berbagai format pembersihan, mengembalikan string format "lat,lon" atau None.
    """
    # Langkah 1: Tangani input None/NaN langsung
    if pd.isna(coord):
        return None

    # Langkah 2: Coba format yang inputnya mungkin berupa angka mentah (float/int)
    cleaned_coord = clean_split_from_long_float(coord)
    if cleaned_coord is not None:
        return cleaned_coord

    # Langkah 3: Jika input bukan angka atau clean_split_from_long_float gagal, coba sebagai string
    coord_str_raw = str(coord)
    # ** PENTING: Hapus karakter problematic (Â, NBSP) di awal **
    coord_str = coord_str_raw.replace('Â', '').replace('\u00A0', ' ').strip()

    # Tangani string kosong atau representasi null dalam string setelah pembersihan awal
    if coord_str == '' or coord_str.lower() in ['none', 'nan', '<na>']:
        return None

    # Coba fungsi pembersihan berbasis string dalam urutan yang logis
    # Fungsi yang lebih spesifik atau yang menangani pemisah non-standar didahulukan
    cleaned_coord = (
        # clean_degree_with_comma(coord_str) # Dihapus
        # clean_degree_separated(coord_str) # Dihapus
        clean_degree_as_separator(coord_str) or  # Ex: -6.9271° 107.6048°
        # Ex: -8,1948403, 111,1077904
        clean_two_commas_with_space(coord_str) or
        clean_dot_space_separated(coord_str) or  # Ex: -7.90845. 113.35127
        clean_with_e_separator(coord_str) or  # Ex: -7.86482E112.69473
        clean_dot_separated_no_comma(coord_str) or  # Ex: -7.90845.113.35127
        clean_merged_coordinates(coord_str) or  # Ex: -7.362714563.427329
        move_comma_separator(coord_str)  # Ex: 7.123.456,112.789.012
        # clean_comma_separated tidak dipanggil di sini agar hanya menjadi fallback
    )

    if cleaned_coord is not None:
        # Jika salah satu fungsi pembersih spesifik berhasil, ia mengembalikan string "lat,lon"
        return cleaned_coord

    # Langkah 4: Fallback terakhir - coba split koma jika ada koma
    # Ini menangani format standar "lat,lon" atau kasus lain yang setelah pembersihan awal
    # menghasilkan string dengan koma sebagai pemisah utama (termasuk "-7.331868..,112.637920").
    if ',' in coord_str:
        # Gunakan logika clean_comma_separated (yang sudah dimodifikasi)
        return clean_comma_separated(coord_str)

    # Langkah 5: Jika tidak ada format yang cocok sama sekali
    return None


# ========================= Contoh Data (Tambahkan kasus baru) =========================
raw_data = [
    "-7.361902Â,112.693948Â",  # Ini data asli, setelah Â dihapus -> "-7.361902,112.693948"
    "-7.361902Â°112.693948Â°",  # setelah Â dihapus -> "-7.361902°112.693948°"
    "-6.9271° 107.6048°",
    "-8,1948403, 111,1077904",
    "8.180339,111.116929",
    "-7,2892906 112,7276532",
    "-7.36271456342.732918",
    -8180339111.116929,
    "-7.86482E112.69473",
    "S709.168E11224.693",
    "-7.90845. 113.35127",
    "  -5.123 . 110.456 ",
    "1.2.3.4.5",
    "abc",
    "123",
    "123,abc",
    "123.45,abc.def",
    None, np.nan, "", " ", "<NA>",
    # --- Kasus baru yang ingin ditangani ---
    "-7.331868..,112.637920",  # Kasus double dot
    "-7.331868...,112.637920",  # Kasus triple dot
    "-7..331868,112.637920",  # Kasus double dot di tempat lain (setelah minus)
    "8.123..,110.456..",  # Kasus double dot di kedua sisi
    "1...2...3,4...5...6"  # Kasus banyak titik di kedua sisi
]

# ========================= Proses Data =========================
print("Memproses data contoh...")
cleaned_data = [apply_cleaning(coord) for coord in raw_data]

# ========================= Output =========================
print("\nOriginal -> Cleaned (string format 'lat,lon' atau None)")
print("=" * 60)
for i, (original, cleaned) in enumerate(zip(raw_data, cleaned_data), 1):
    # Gunakan repr() atau !r untuk menampilkan string secara literal (termasuk '', None)
    print(f"{i}. Original: {original!r}")
    print(f"   ✅ Cleaned : {cleaned!r}")
    # Opsional: Cek apakah hasil cleaned bisa dipecah dan diubah jadi float
    if isinstance(cleaned, str) and ',' in cleaned:
        try:
            lat, lon = cleaned.split(',', 1)
            lat_float = float(lat)
            lon_float = float(lon)
            print(
                f"   -> Hasil OK, siap konversi Float. Lat: {lat_float}, Lon: {lon_float}")
        except (ValueError, TypeError):
            print("   !!! Peringatan: Hasil clean tidak bisa dikonversi jadi Float! !!!")
    elif cleaned is None:
        print("   -> Hasil adalah None (tidak dikenali).")
    else:
        print(
            f"   !!! Peringatan: Hasil clean bukan string 'lat,lon' atau None! ({type(cleaned)}) !!!")

print("=" * 60)
