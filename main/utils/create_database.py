from .config_database import initialize_database_connections
from psycopg2 import Error as Psycopg2Error
from airflow.utils.log.logging_mixin import LoggingMixin
import os
SCHEMA_FILE_PATH = os.getenv(
    "AIRFLOW_VAR_SCHEMA_SQL_PATH", "/opt/airflow/config/schema.sql")
# Anda bisa juga set ini via Airflow Variable `SCHEMA_SQL_PATH`


def ensure_database_schema(**kwargs):
    """
    Ensures that the database schema (tables) exists in Supabase.
    Reads DDL statements from a .sql file and executes them.
    """
    ti = kwargs['ti']
    log = LoggingMixin().log

    log.info(
        f"Attempting to ensure database schema using SQL file: {SCHEMA_FILE_PATH}")
    db_pool = initialize_database_connections()

    if not db_pool:
        log.error("Database pool not initialized. Cannot ensure schema.")
        raise ValueError("Database pool not initialized for schema creation.")

    conn = None
    try:
        with open(SCHEMA_FILE_PATH, 'r', encoding='utf-8') as f:
            # Membaca seluruh isi file SQL
            sql_script = f.read()

        # Pisahkan skrip menjadi statement individual berdasarkan ';'
        # Hapus statement kosong yang mungkin muncul karena baris baru atau spasi
        sql_statements = [stmt.strip()
                          for stmt in sql_script.split(';') if stmt.strip()]

        if not sql_statements:
            log.warning(f"No SQL statements found in {SCHEMA_FILE_PATH}.")
            return

        conn = db_pool.getconn()
        with conn.cursor() as cur:
            log.info(
                f"Found {len(sql_statements)} SQL statement(s) to execute.")
            for i, statement in enumerate(sql_statements):
                # Log sebagian statement
                log.info(
                    f"Executing statement {i+1}/{len(sql_statements)}: {statement[:150]}...")
                cur.execute(statement)
            conn.commit()
        log.info("Database schema ensured successfully.")

    except FileNotFoundError:
        log.error(
            f"Schema file not found at {SCHEMA_FILE_PATH}. Please ensure the path is correct and accessible.")
        raise
    except Psycopg2Error as db_err:
        log.error(f"Database error during schema creation: {db_err}")
        if conn:
            conn.rollback()  # Rollback jika terjadi error
        raise
    except Exception as e:
        log.error(f"An unexpected error occurred during schema creation: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn and db_pool:
            db_pool.putconn(conn)
# --- Akhir fungsi schema ---
