
import snowflake.connector
import polars as pl
from loguru import logger
from pathlib import Path
import tempfile
import threading
from covid_project3.config import config

# Thread-local storage so each thread gets its own Snowflake connection.
# The global singleton approach is not thread-safe: concurrent threads calling
# conn.cursor() on the same connection object cause failures in the executor.
_thread_local = threading.local()


def _staging_parquet_path(filename: str) -> Path:
    staging_dir = Path(tempfile.gettempdir()) / "covid_project3"
    staging_dir.mkdir(parents=True, exist_ok=True)
    return staging_dir / filename


def get_connection():
    """Return a Snowflake connection scoped to the current thread."""
    conn = getattr(_thread_local, "connection", None)
    if conn is None or conn.is_closed():
        conn = snowflake.connector.connect(
            user=config["SNOWFLAKE_USER"],
            password=config["SNOWFLAKE_PASSWORD"],
            account=config["SNOWFLAKE_ACCOUNT"],
            warehouse=config["SNOWFLAKE_WAREHOUSE"],
            database=config["SNOWFLAKE_DATABASE"],
            schema=config["SNOWFLAKE_SCHEMA"],
        )
        _thread_local.connection = conn
    return conn


def create_table_if_not_exists(cursor, dataset_df: pl.DataFrame, table_name: str):
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
    for col in dataset_df.columns:
        dtype = dataset_df[col].dtype
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
        query = f"SELECT MAX({composite_expr}) FROM {table_name}" # Max can be slow, order by desc limit 1 might be faster if you use a column that is indexed.
        cursor.execute(query)
        return cursor.fetchone()[0]
    except Exception as e:
        logger.warning(f"No existing table or failed to fetch max value: {e}")
        return None
    finally:
        if cursor:
            cursor.close()


def generate_unique_key(
    dataset_df: pl.DataFrame, unique_key_cols: list
) -> pl.DataFrame:
    return dataset_df.with_columns(
        pl.concat_str(
            [pl.col(c).cast(pl.Utf8) for c in unique_key_cols],
            separator="|",
        ).alias("unique_key")
    )


def upload_to_snowflake(
    cdc_cases_batch_df: pl.DataFrame, batch_id: int, table_name: str
):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        create_table_if_not_exists(cursor, cdc_cases_batch_df, table_name) # create table if not exists becomes dead code after first run. 

        parquet_path = _staging_parquet_path(f"cdc_batch_{batch_id}.parquet")
        cdc_cases_batch_df.write_parquet(str(parquet_path))

        cursor.execute(f"PUT file://{parquet_path} @%{table_name}")
        cursor.execute(f"""
            COPY INTO {table_name}
            FROM @%{table_name}
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        """)
        logger.info(f"🚀 BULK loaded batch {batch_id}")
    except Exception as e:
        logger.error(f"Batch {batch_id} failed: {e}") #Throw an exception, fail fast and loud
    finally:
        if cursor:
            cursor.close()


def upload_census_to_snowflake(
    census_fips_df: pl.DataFrame, table_name="US_CENSUS_2023"
):
    """
    Upload the Census FIPS data to Snowflake using the existing uploader.
    """
    conn = get_connection()
    cursor = conn.cursor()
    #Try withpout an exception handling here, if this fails we want to know about it and fix it, failing silently can hide issues.
    try:
        # Create table dynamically
        create_table_if_not_exists(cursor, census_fips_df, table_name)

        parquet_path = _staging_parquet_path("census.parquet")
        census_fips_df.write_parquet(str(parquet_path))

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
        
        
# New function to count Snowflake rows
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
        return -1 # Again, consider raising an exception here instead of returning -1 to fail fast and early, returning -1 can hide issues.
    finally:
        if cursor:
            cursor.close()