# c:\Users\rizky\OneDrive\Dokumen\GitHub\ETL\Dockerfile

FROM apache/airflow:3.0.0

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt
