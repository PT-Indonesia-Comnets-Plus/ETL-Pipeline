import json
from airflow.sdk import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from psycopg2 import pool as psycopg2_pool
from sqlalchemy import create_engine

# Global variables to store connections (lazy initialization)
_db_pool = None
_sqlalchemy_engine = None
_initialized = False


def initialize_database_connections():
    """
    Initialize database connections with lazy loading.
    Only connects when actually needed, not during import time.
    """
    global _db_pool, _sqlalchemy_engine, _initialized

    # Return cached connections if already initialized
    if _initialized:
        return _db_pool, _sqlalchemy_engine

    log = LoggingMixin().log

    try:
        log.info("🔌 Menginisialisasi database connection pool...")

        # Check if running in DAG parsing mode (skip database connections)
        import os
        if os.environ.get('AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION') == 'True':
            log.info(
                "⚠️ Running in DAG parsing mode, skipping database initialization")
            return None, None

        # Ambil string JSON dari Airflow Variable
        connection_details_str = Variable.get("SUPABASE_CONN")
        connection_details = json.loads(connection_details_str)

        db_host = connection_details["DB_HOST"]
        db_name = connection_details["DB_NAME"]
        db_user = connection_details["DB_USER"]
        db_password = connection_details["DB_PASSWORD"]
        db_port = connection_details["DB_PORT"]

        _db_pool = psycopg2_pool.SimpleConnectionPool(
            1, 5,  # minconn, maxconn
            host=db_host, database=db_name, user=db_user, password=db_password, port=db_port
        )
        log.info("✅ Psycopg2 connection pool berhasil diinisialisasi.")

        # Buat SQLAlchemy engine
        db_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        _sqlalchemy_engine = create_engine(db_url)
        log.info("✅ SQLAlchemy engine berhasil diinisialisasi.")

        _initialized = True

    except KeyError as e:
        log.error(
            f"❌ Kunci {e} tidak ditemukan dalam JSON SUPABASE_CONNECTION_DETAILS_JSON.")
        return None, None  # Kembalikan None jika gagal
    except json.JSONDecodeError:
        log.error("❌ Gagal mem-parsing JSON dari SUPABASE_CONNECTION_DETAILS_JSON.")
        return None, None  # Kembalikan None jika gagal
    except Exception as e:
        log.error(f"❌ Gagal menginisialisasi database connection pool: {e}")
        return None, None  # Kembalikan None jika gagal

    return _db_pool, _sqlalchemy_engine
