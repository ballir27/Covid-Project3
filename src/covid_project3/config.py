import os

# import snowflake.connector
from dotenv import dotenv_values
# from loguru import logger


# def snow_config():
#     load_dotenv()

#     # Connect to Snowflake
#     logger.info("Connecting to Snowflake...")
#     snow_conn = snowflake.connector.connect(
#         user=os.getenv("snowflake_user"),
#         password=os.getenv("snowflake_password"),
#         account=os.getenv("snowflake_account"),
#         warehouse=os.getenv("snowflake_warehouse"),
#         database=os.getenv("snowflake_database"),
#         schema=os.getenv("snowflake_schema"),
#         role=os.getenv("snowflake_role")
#     )
#     return snow_conn
# src/covid_project3/config.py
# Get absolute path to project root
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Load .env from project root
config = dotenv_values(os.path.join(PROJECT_ROOT, ".env"))