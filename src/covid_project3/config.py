import os

import snowflake.connector
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
