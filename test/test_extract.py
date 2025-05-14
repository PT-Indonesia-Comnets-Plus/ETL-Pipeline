import json
import logging
from unittest.mock import MagicMock
from airflow.sdk import Variable
import gspread
from google.oauth2 import service_account

# Setup logging
logging.basicConfig(level=logging.INFO)
t_log = logging.getLogger(__name__)

# Simulasi Variable.get di luar Airflow


class MockVariable:
    @staticmethod
    def get(key):
        if key == "GOOGLE_CREDENTIALS_JSON":
            # Ganti dengan isi string kredensial JSON kamu untuk lokal testing
            with open("google_credentials.json") as f:
                return f.read()
        return None


# Ganti Variable dengan versi mock
Variable.get = MockVariable.get


def extract(**kwargs):
    ti = kwargs["ti"]
    t_log.info("--- Memulai Autentikasi Google Sheets ---")

    try:
        credentials_json_string = Variable.get("GOOGLE_CREDENTIALS_JSON")
        if not credentials_json_string:
            raise ValueError("Variabel 'GOOGLE_CREDENTIALS_JSON' kosong.")

        credentials_dict = json.loads(credentials_json_string)
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        credentials = service_account.Credentials.from_service_account_info(
            credentials_dict, scopes=scope
        )
        client = gspread.authorize(credentials)

        t_log.info("✅ Autentikasi Google Sheets berhasil.")
    except Exception as e:
        t_log.error(f"❌ Gagal autentikasi Google Sheets: {e}")
        raise

    data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
    ti.xcom_push("order_data", data_string)

# Simulasi tugas Airflow


class MockTI:
    def xcom_push(self, key, value):
        print(f"✅ XCom Push: {key} = {value}")


if __name__ == "__main__":
    extract(ti=MockTI())
