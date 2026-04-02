import argparse
import asyncio
from collections.abc import Sequence

from covid_project3.config import config
from covid_project3.ingestion.engine import (
    run_census_pipeline,
    run_pipeline,
)

ENDPOINT = config["CDC_COVID_API_ENDPOINT"]
CDC_TABLE_NAME = "CDC_RAW_CASES_1"
CENSUS_TABLE_NAME = "US_CENSUS_2023"

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


async def _run_selected_pipeline(pipeline: str) -> None:
    if pipeline in {"cdc", "both"}:
        await run_pipeline(
            ENDPOINT,
            CDC_TABLE_NAME,
            dict(config),
            UNIQUE_KEY_COLS,
            select_columns=SELECT_COLUMNS,
        )

    if pipeline in {"census", "both"}:
        await run_census_pipeline()


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run ingestion locally (CDC, Census, or both)."
    )
    parser.add_argument(
        "--pipeline",
        choices=["cdc", "census", "both"],
        default="both",
        help="Which ingestion pipeline to run.",
    )
    args = parser.parse_args(argv)
    asyncio.run(_run_selected_pipeline(args.pipeline))


if __name__ == "__main__":
    main()
