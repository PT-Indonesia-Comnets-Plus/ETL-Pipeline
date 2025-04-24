import pandas as pd
from airflow.utils.log.logging_mixin import LoggingMixin
t_log = LoggingMixin().log


def load_sheet_as_dataframe(spreadsheet_id, sheet_name, client):
    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
        sheet = spreadsheet.worksheet(sheet_name)
        data = sheet.get_all_values()
        if not data:
            return pd.DataFrame()
        if len(data) == 1:
            return pd.DataFrame(columns=data[0])
        return pd.DataFrame(data[1:], columns=data[0])
    except Exception as e:
        t_log.error(f"Gagal memuat sheet '{sheet_name}': {e}")
        raise
