# # src/covid_project3/ingestion/uploader.py
# import os
# import polars as pl
# import snowflake.connector
# from loguru import logger
# from covid_project3.config import config

# _connection = None
# TMP_DIR = "tmp"
# os.makedirs(TMP_DIR, exist_ok=True)

# def get_connection():
#     global _connection
#     if _connection is None:
#         _connection = snowflake.connector.connect(
#             user=config["SNOWFLAKE_USER"],
#             password=config["SNOWFLAKE_PASSWORD"],
#             account=config["SNOWFLAKE_ACCOUNT"],
#             warehouse=config["SNOWFLAKE_WAREHOUSE"],
#             database=config["SNOWFLAKE_DATABASE"],
#             schema=config["SNOWFLAKE_SCHEMA"],
#         )
#     return _connection


# def create_table_if_not_exists(cursor, df: pl.DataFrame, table_name: str):
#     dtype_mapping = {
#         pl.Int64: "NUMBER",
#         pl.Int32: "NUMBER",
#         pl.Float64: "FLOAT",
#         pl.Float32: "FLOAT",
#         pl.Utf8: "STRING",
#         pl.Boolean: "BOOLEAN",
#         pl.Date: "DATE",
#         pl.Datetime: "TIMESTAMP_NTZ",
#     }
#     columns = []
#     for col in df.columns:
#         dtype = df[col].dtype
#         snow_type = dtype_mapping.get(type(dtype), "STRING")
#         columns.append(f'"{col.upper()}" {snow_type}')

#     create_sql = f"""
#         CREATE TABLE IF NOT EXISTS {table_name} (
#             {', '.join(columns)}
#         )
#     """
#     cursor.execute(create_sql)


# def get_latest_value(table_name: str, unique_key_cols: list):
#     conn = None
#     cursor = None
#     try:
#         conn = get_connection()
#         cursor = conn.cursor()
#         composite_expr = " || '|' || ".join([f'"{col.upper()}"' for col in unique_key_cols])
#         query = f"SELECT MAX({composite_expr}) FROM {table_name}"
#         cursor.execute(query)
#         return cursor.fetchone()[0]
#     except Exception as e:
#         logger.warning(f"No existing table or failed to fetch max value: {e}")
#         return None
#     finally:
#         if cursor:
#             cursor.close()


# def generate_unique_key(df: pl.DataFrame, unique_key_cols: list) -> pl.DataFrame:
#     return df.with_columns(
#         pl.concat_str([pl.col(c).cast(pl.Utf8) for c in unique_key_cols], separator="|").alias("unique_key")
#     )


# def upload_to_snowflake(df: pl.DataFrame, batch_id: int, table_name: str):
#     conn = get_connection()
#     cursor = conn.cursor()
#     try:
#         create_table_if_not_exists(cursor, df, table_name)

#         # Save locally as Parquet
#         parquet_path = os.path.join(TMP_DIR, f"cdc_batch_{batch_id}.parquet")
#         df.write_parquet(parquet_path)

#         # PUT + COPY INTO (bulk load)
#         cursor.execute(f"PUT file://{parquet_path} @%{table_name} AUTO_COMPRESS=TRUE")
#         cursor.execute(f"""
#             COPY INTO {table_name}
#             FROM @%{table_name}
#             FILE_FORMAT = (TYPE = PARQUET)
#             MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
#         """)
#         logger.info(f"🚀 BULK loaded batch {batch_id}")
#     except Exception as e:
#         logger.error(f"❌ Batch {batch_id} failed: {e}")
#     finally:
#         if cursor:
#             cursor.close()
# src/covid_project3/ingestion/uploader.py
# src/covid_project3/ingestion/uploader.py
import snowflake.connector
import polars as pl
from loguru import logger
from covid_project3.config import config

_connection = None

def get_connection():
    global _connection
    if _connection is None:
        _connection = snowflake.connector.connect(
            user=config["SNOWFLAKE_USER"],
            password=config["SNOWFLAKE_PASSWORD"],
            account=config["SNOWFLAKE_ACCOUNT"],
            warehouse=config["SNOWFLAKE_WAREHOUSE"],
            database=config["SNOWFLAKE_DATABASE"],
            schema=config["SNOWFLAKE_SCHEMA"],
        )
    return _connection


def create_table_if_not_exists(cursor, df: pl.DataFrame, table_name: str):
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


def get_latest_value(table_name: str, unique_key_cols: list):
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
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


def generate_unique_key(df: pl.DataFrame, unique_key_cols: list) -> pl.DataFrame:
    return df.with_columns(
        pl.concat_str([pl.col(c).cast(pl.Utf8) for c in unique_key_cols], separator="|").alias("unique_key")
    )


def upload_to_snowflake(df: pl.DataFrame, batch_id: int, table_name: str):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        create_table_if_not_exists(cursor, df, table_name)

        parquet_path = f"tmp/cdc_batch_{batch_id}.parquet"
        df.write_parquet(parquet_path)

        cursor.execute(f"PUT file://{parquet_path} @%{table_name}")
        cursor.execute(f"""
            COPY INTO {table_name}
            FROM @%{table_name}
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        """)
        logger.info(f"🚀 BULK loaded batch {batch_id}")
    except Exception as e:
        logger.error(f"❌ Batch {batch_id} failed: {e}")
    finally:
        if cursor:
            cursor.close()

def upload_census_to_snowflake(df, table_name="US_CENSUS_2023"):
    """
    Upload the Census FIPS data to Snowflake using the existing uploader
    """
    from loguru import logger
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # Create table dynamically
        create_table_if_not_exists(cursor, df, table_name)

        parquet_path = f"tmp/census.parquet"
        df.write_parquet(parquet_path)

        cursor.execute(f"PUT file://{parquet_path} @%{table_name}")
        cursor.execute(f"""
            COPY INTO {table_name}
            FROM @%{table_name}
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        """)
        logger.info(f"🚀 Census data uploaded to {table_name}")
    finally:
        cursor.close()
        
        
# ✅ New function to count Snowflake rows
def count_snowflake_rows(table_name: str) -> int:
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        return cursor.fetchone()[0]
    except Exception as e:
        logger.warning(f"Failed to count rows in {table_name}: {e}")
        return -1
    finally:
        if cursor:
            cursor.close()