from .config_database import initialize_database_connections
from psycopg2 import Error as Psycopg2Error
from airflow.utils.log.logging_mixin import LoggingMixin
import os

# --- Constants ---
# Path ke file SQL skema. Bisa di-override dengan Airflow Variable SCHEMA_SQL_PATH.
DEFAULT_SCHEMA_FILE_PATH = "/opt/airflow/config/schema.sql"
# Nama Airflow Variable jika ingin override
SCHEMA_SQL_PATH_VAR_NAME = "SCHEMA_SQL_PATH"


def ensure_database_schema(**kwargs):
    """
    Ensures that the database schema (tables) exists in Supabase.
    Reads DDL statements from a .sql file and executes them.
    """
    # ti = kwargs.get('ti') # ti tidak digunakan secara aktif di fungsi ini, bisa dihilangkan jika tidak ada XCom
    log = LoggingMixin().log

    # Dapatkan path file skema, utamakan dari Airflow Variable jika ada
    schema_file_path = os.getenv(
        f"AIRFLOW_VAR_{SCHEMA_SQL_PATH_VAR_NAME}", DEFAULT_SCHEMA_FILE_PATH
    )

    log.info(
        f"Attempting to ensure database schema using SQL file: {schema_file_path}")

    db_psycopg2_pool, _ = initialize_database_connections()
    if not db_psycopg2_pool:
        log.error(
            "Psycopg2 database pool not initialized from initialize_database_connections(). Cannot ensure schema.")
        raise ValueError("Database pool not initialized for schema creation.")

    conn = None
    try:
        with open(schema_file_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        sql_statements = [stmt.strip()
                          for stmt in sql_script.split(';') if stmt.strip()]

        if not sql_statements:
            log.warning(f"No SQL statements found in {schema_file_path}.")
            return

        conn = db_psycopg2_pool.getconn()
        with conn.cursor() as cur:
            log.info(
                f"Found {len(sql_statements)} SQL statement(s) to execute.")
            for i, statement in enumerate(sql_statements):
                log.info(
                    f"Executing statement {i+1}/{len(sql_statements)}: {statement[:150]}...")
                cur.execute(statement)
            conn.commit()
        log.info("Database schema ensured successfully.")
    except FileNotFoundError:
        log.error(
            f"Schema file not found at {schema_file_path}. Please ensure the path is correct and accessible.")
        raise
    except Psycopg2Error as db_err:
        log.error(f"Database error during schema creation: {db_err}")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        log.error(f"An unexpected error occurred during schema creation: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn and db_psycopg2_pool:
            db_psycopg2_pool.putconn(conn)
