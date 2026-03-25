import os

import snowflake.connector
from dotenv import load_dotenv
from loguru import logger


def snow_config():
    load_dotenv()

    # Connect to Snowflake
    logger.info("Connecting to Snowflake...")
    snow_conn = snowflake.connector.connect(
        user=os.getenv("snowflake_user"),
        password=os.getenv("snowflake_password"),
        account=os.getenv("snowflake_account"),
        warehouse=os.getenv("snowflake_warehouse"),
        database=os.getenv("snowflake_database"),
        schema=os.getenv("snowflake_schema"),
        role=os.getenv("snowflake_role")
    )
    return snow_conn
