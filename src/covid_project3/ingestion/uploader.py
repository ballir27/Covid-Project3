# src/covid_project3/ingestion/uploader.py

import snowflake.connector
import polars as pl
from covid_project3.config import config
from loguru import logger


# =========================
# CONNECTION
# =========================
def get_connection():
    return snowflake.connector.connect(
        user=config["SNOWFLAKE_USER"],
        password=config["SNOWFLAKE_PASSWORD"],
        account=config["SNOWFLAKE_ACCOUNT"],
        warehouse=config["SNOWFLAKE_WAREHOUSE"],
        database=config["SNOWFLAKE_DATABASE"],
        schema=config["SNOWFLAKE_SCHEMA"]
    )


# =========================
# CREATE TABLE (AUTO)
# =========================
def create_table_if_not_exists(cursor, df: pl.DataFrame, table_name: str):
    """
    Create Snowflake table based on Polars schema.
    """
    dtype_mapping = {
        pl.Int64: "NUMBER",
        pl.Int32: "NUMBER",
        pl.Float64: "FLOAT",
        pl.Float32: "FLOAT",
        pl.Utf8: "STRING",
        pl.Boolean: "BOOLEAN",
        pl.Date: "DATE",
        pl.Datetime: "TIMESTAMP_NTZ",
    }

    columns = []
    for col in df.columns:
        dtype = df[col].dtype
        snow_type = dtype_mapping.get(type(dtype), "STRING")
        columns.append(f'"{col.upper()}" {snow_type}')

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns)}
        )
    """

    cursor.execute(create_sql)


# =========================
# GET LATEST VALUE
# =========================
def get_latest_value(table_name: str, unique_key_cols: list):
    """
    Fetch max composite key safely.
    """
    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # IMPORTANT: quote column names
        composite_expr = " || '|' || ".join([f'"{col.upper()}"' for col in unique_key_cols])

        query = f"SELECT MAX({composite_expr}) FROM {table_name}"
        cursor.execute(query)

        return cursor.fetchone()[0]

    except Exception as e:
        logger.warning(f"No existing table or failed to fetch max value: {e}")
        return None

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# =========================
# GENERATE UNIQUE KEY
# =========================
def generate_unique_key(df: pl.DataFrame, unique_key_cols: list) -> pl.DataFrame:
    """
    Create composite key using Polars.
    """
    return df.with_columns(
        pl.concat_str(
            [pl.col(c).cast(pl.Utf8) for c in unique_key_cols],
            separator="|"
        ).alias("unique_key")
    )


# =========================
# UPLOAD DATA
# =========================
def upload_to_snowflake(df: pl.DataFrame, batch_id: int, table_name: str):
    """
    Insert data into Snowflake (auto-creates table).
    """
    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # ✅ Ensure table exists
        create_table_if_not_exists(cursor, df, table_name)

        # ✅ Convert Polars → Python rows (NO numpy dependency issue)
        records = df.rows()

        columns = ", ".join([f'"{c.upper()}"' for c in df.columns])
        placeholders = ", ".join(["%s"] * len(df.columns))

        insert_sql = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({placeholders})
        """

        cursor.executemany(insert_sql, records)
        conn.commit()

        logger.info(f"✅ Uploaded batch {batch_id}")

    except Exception as e:
        logger.error(f"❌ Error uploading batch {batch_id}: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()