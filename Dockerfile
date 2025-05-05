# c:\Users\rizky\OneDrive\Dokumen\GitHub\ETL\Dockerfile

# Gunakan versi image Airflow yang sesuai dengan docker-compose.yml kamu
# Ganti 'apache/airflow:2.8.1' dengan versi yang kamu gunakan
FROM apache/airflow:3.0.0

# Ganti ke user root sementara untuk instalasi (jika diperlukan)
# USER root

# Salin file requirements.txt ke dalam image
# Pastikan path ini benar relatif terhadap build context di docker-compose.yml
COPY requirements.txt /requirements.txt

# Instal requirements menggunakan pip
# --user bisa digunakan jika tidak ingin jadi root, tapi pastikan pathnya dikenali Airflow
# --no-cache-dir untuk menjaga ukuran image tetap kecil
RUN pip install --no-cache-dir -r /requirements.txt

# (Opsional) Jika kamu berganti ke root, kembalikan ke user airflow
# USER airflow
