FROM apache/airflow:3.0.0

COPY requirements.txt /requirements.txt

RUN uv pip install --no-cache-dir -r /requirements.txt