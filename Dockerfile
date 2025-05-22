# Gunakan base image Python yang sesuai dengan versi Python di pyproject.toml Anda
# Misalnya, jika Anda menggunakan Python 3.9
ARG PYTHON_VERSION=3.9
FROM python:${PYTHON_VERSION}-slim-buster

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    AIRFLOW_HOME=/opt/airflow \
    # Konfigurasi Poetry untuk tidak membuat virtual env di dalam image
    # dan tidak ada interaksi saat instalasi
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    # Atau jika Anda ingin Poetry menginstal ke site-packages sistem:
    # POETRY_INSTALL_NO_ROOT=1
    # POETRY_INSTALL_NO_DEV=1 # Opsional, jika tidak ingin dev dependencies
    PATH="/opt/airflow/.local/bin:${PATH}"

# Variabel untuk versi Airflow dan Poetry (bisa disesuaikan)
ARG AIRFLOW_VERSION=2.8.1
ARG POETRY_VERSION=1.7.1

# Persyaratan sistem yang mungkin dibutuhkan oleh Airflow atau dependensi Python lainnya
# Contoh: untuk postgres, mysql, ldap, dsb.
# Sesuaikan berdasarkan kebutuhan backend dan executor Airflow Anda.
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     libpq-dev \ # Untuk psycopg2 (PostgreSQL)
#     default-libmysqlclient-dev \ # Untuk mysqlclient (MySQL)
#     libsasl2-dev \ # Untuk LDAP
#     curl \
#     && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install "poetry==${POETRY_VERSION}"

# Set working directory
WORKDIR /opt/airflow

# Copy file dependensi Poetry
COPY pyproject.toml poetry.lock ./

# Install dependensi proyek menggunakan Poetry
# --no-dev direkomendasikan untuk image production
# Hapus --no-dev jika Anda membutuhkan dependensi development di image
RUN poetry install --no-dev --no-root

# Copy semua file proyek (DAGs, plugins, skrip, dll.)
# Pastikan .dockerignore sudah dikonfigurasi dengan benar
COPY . .

# Copy dan set permission untuk entrypoint script (jika menggunakan)
COPY scripts/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Inisialisasi database Airflow dan buat user admin
# Ini bisa juga dilakukan di entrypoint script untuk fleksibilitas lebih
# RUN airflow db init && \
#     airflow users create \
#     --username admin \
#     --password admin \
#     --firstname Admin \
#     --lastname User \
#     --role Admin \
#     --email admin@example.com

# Expose port untuk Airflow webserver
EXPOSE 8080

# Set user (opsional, tapi praktik yang baik untuk keamanan)
# USER airflow
# RUN mkdir -p ${AIRFLOW_HOME}/logs ${AIRFLOW_HOME}/dags ${AIRFLOW_HOME}/plugins
# Jika Anda membuat user baru, pastikan user tersebut memiliki hak tulis ke AIRFLOW_HOME

# Perintah default saat container dijalankan
# Bisa berupa "webserver", "scheduler", "worker", atau skrip entrypoint
# CMD ["airflow", "webserver"]
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"] # Default command untuk entrypoint.sh
