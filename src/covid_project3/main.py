# import config
# from loguru import logger


# def main():
#     try:
#         snow_conn = config.snow_config()
#         snow_cursor = snow_conn.cursor()

#     except Exception as e:
#         logger.error(f"Error uploading to Snowflake: {e}")
#         raise e

#     finally:
#         snow_cursor.close()
#         snow_conn.close()

# if __name__ == "__main__":
#     main()

#      main()
# src/covid_project3/main.py
import asyncio
from covid_project3.ingestion.engine import run_pipeline
from covid_project3.config import config

ENDPOINT = config["CDC_COVID_API_ENDPOINT"]
TABLE_NAME = "CDC_RAW_CASES_1"

# Columns to generate unique key
UNIQUE_KEY_COLS = ["case_month", "res_state"]

# Optional: only keep these columns
SELECT_COLUMNS = [
    "case_month", "res_state", "state_fips_code", "res_county",
    "county_fips_code", "age_group", "sex", "race", "ethnicity",
    "case_onset_interval", "process", "exposure_yn", "current_status",
    "symptom_status", "hosp_yn", "icu_yn", "death_yn",
    "case_positive_specimen", "underlying_conditions_yn"
]

if __name__ == "__main__":
    asyncio.run(run_pipeline(
        ENDPOINT,
        TABLE_NAME,
        config,
        UNIQUE_KEY_COLS,
        select_columns=SELECT_COLUMNS
    ))
   