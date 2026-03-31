import asyncio

from covid_project3.config import config
from covid_project3.ingestion.engine import run_pipeline ,run_census_pipeline


ENDPOINT = config["CDC_COVID_API_ENDPOINT"]
TABLE_NAME = "CDC_RAW_CASES_1"

UNIQUE_KEY_COLS = ["case_month", "res_state"]

SELECT_COLUMNS = [
    "case_month",
    "res_state",
    "state_fips_code",
    "res_county",
    "county_fips_code",
    "age_group",
    "sex",
    "race",
    "ethnicity",
    "case_onset_interval",
    "process",
    "exposure_yn",
    "current_status",
    "symptom_status",
    "hosp_yn",
    "icu_yn",
    "death_yn",
    "case_positive_specimen",
    "underlying_conditions_yn",
]

if __name__ == "__main__":
    asyncio.run(
        run_pipeline(
            ENDPOINT,
            TABLE_NAME,
            config,
            UNIQUE_KEY_COLS,
            select_columns=SELECT_COLUMNS,
        )
    )
    asyncio.run(run_census_pipeline())
