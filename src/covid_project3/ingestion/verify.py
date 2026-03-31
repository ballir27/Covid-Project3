# src/covid_project3/ingestion/verify.py
import httpx
from .uploader import get_connection

def get_api_total_count(endpoint: str) -> int:
    try:
        resp = httpx.get(f"{endpoint}?$limit=1")  # just fetch metadata
        resp.raise_for_status()
        data = resp.json()
        total = data.get("metadata", {}).get("total")  # adjust to your API
        if total is None:
            return -1
        return int(total)
    except Exception:
        return -1

def get_snowflake_row_count(table_name: str) -> int:
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
        rows = cursor.fetchone()[0]
        cursor.close()
        return rows
    except Exception:
        return -1

def verify_pipeline(endpoint: str, table_name: str):
    api_total = get_api_total_count(endpoint)
    sf_rows = get_snowflake_row_count(table_name)

    result = {
        "api_total": api_total,
        "sf_rows": sf_rows,
        "status": None
    }

    if api_total != -1:
        if api_total == sf_rows:
            result["status"] = "✅ All rows fetched!"
        elif sf_rows < api_total:
            result["status"] = f"⚠️ Missing {api_total - sf_rows} rows in Snowflake."
        else:
            result["status"] = f"⚠️ Snowflake has {sf_rows - api_total} extra rows."
    else:
        result["status"] = "⚠️ Cannot verify total rows from API."

    return result