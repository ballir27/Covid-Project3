import threading

import polars as pl
import snowflake.connector
from loguru import logger
from snowflake.connector.errors import Error as SnowflakeError
from snowflake.connector.pandas_tools import write_pandas

from covid_project3.config import config

_thread_local = threading.local()


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


def _quoted_table_ref(table_name: str) -> str:
    return ".".join(
        f'"{p.strip()}"' for p in table_name.split(".") if p.strip()
    )


def _normalized_identifier(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = value.strip().strip('"')
    return cleaned.upper() if cleaned else None


def _as_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def get_latest_value(
    table_name: str,
    unique_key_cols: list[str],
) -> str | None:
    cursor = None
    try:
        cursor = get_connection().cursor()
        composite_expr = " || '|' || ".join(
            [f'"{col.upper()}"' for col in unique_key_cols]
        )
        query = (
            f"SELECT MAX({composite_expr}) FROM {_quoted_table_ref(table_name)}"  # noqa: S608
        )
        cursor.execute(query)
        return cursor.fetchone()[0]
    except SnowflakeError as e:
        logger.warning(f"No existing table or failed to fetch max value: {e}")
        return None
    finally:
        if cursor:
            cursor.close()


def count_snowflake_rows(table_name: str) -> int:
    cursor = None
    try:
        cursor = get_connection().cursor()
        cursor.execute(
            f"SELECT COUNT(*) FROM {_quoted_table_ref(table_name)}"  # noqa: S608
        )
        return cursor.fetchone()[0]
    except SnowflakeError as e:
        logger.warning(f"Failed to count rows in {table_name}: {e}")
        return -1
    finally:
        if cursor:
            cursor.close()


def upload_dataframe(df: pl.DataFrame, table_name: str) -> int:
    """Upload a Polars DataFrame directly to Snowflake via write_pandas."""
    conn = get_connection()
    pdf = df.to_pandas()
    target = _normalized_identifier(table_name.split(".")[-1])
    schema = _normalized_identifier(config.get("SNOWFLAKE_SCHEMA"))
    database = _normalized_identifier(config.get("SNOWFLAKE_DATABASE"))
    auto_create = _as_bool(config.get("SNOWFLAKE_AUTO_CREATE_TABLE"), default=True)

    if not target:
        raise ValueError(f"Invalid table name: {table_name}")

    success, _, nrows, _ = write_pandas(
        conn,
        pdf,
        target,
        schema=schema,
        database=database,
        auto_create_table=auto_create,
        overwrite=False,
    )
    if not success:
        raise RuntimeError(f"write_pandas failed for table {table_name}")
    return nrows
