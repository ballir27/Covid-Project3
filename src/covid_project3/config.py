import os

import snowflake.connector
from adbc_driver_snowflake import dbapi
from dotenv import load_dotenv
from loguru import logger


def snow_config():
    load_dotenv()

    # Connect to Snowflake
    logger.info("Connecting to Snowflake...")
    snow_conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )
    return snow_conn

def adbc_snowflake_config():
    load_dotenv()

    opts = {
        "username": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "adbc.snowflake.sql.account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "adbc.snowflake.sql.db": os.getenv("SNOWFLAKE_DATABASE"),
        "adbc.snowflake.sql.schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "adbc.snowflake.sql.warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "adbc.snowflake.sql.role": os.getenv("SNOWFLAKE_ROLE"),
    }

    adbc_conn = dbapi.connect(db_kwargs=opts)
    return adbc_conn
