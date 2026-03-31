# dags/covid_ingestion_dag.py
import asyncio
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from loguru import logger

from covid_project3.config import config
from covid_project3.ingestion.engine import run_census_pipeline, run_pipeline

# Redirect loguru to Airflow's stdlib logging so task logs are captured.
logger.remove()
logger.add(
    lambda msg: logging.getLogger("covid_project3").info(msg),
    format="{message}",
    colorize=False,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CDC_ENDPOINT = config.get("CDC_COVID_API_ENDPOINT")
CDC_TABLE_NAME = "CDC_RAW_CASES_1"
CDC_CONFIG = {
    "LIMIT": int(config.get("LIMIT", 10000)),
    "CONCURRENT_REQUESTS": int(config.get("CONCURRENT_REQUESTS", 10)),
    "CONCURRENT_UPLOADS": int(config.get("CONCURRENT_UPLOADS", 5)),
}
# Column names match the n8mc-b4w4 (case-level) endpoint schema.
CDC_UNIQUE_KEYS = ["case_month", "res_state", "county_fips_code"]

# ---------------------------------------------------------------------------
# Default DAG args
# ---------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    "covid_ingestion",
    default_args=default_args,
    description="CDC + Census ingestion pipeline",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["covid", "ingestion"],
) as dag:

    def run_cdc():
        """Run CDC ingestion pipeline asynchronously."""
        logger.info("Starting CDC ingestion pipeline...")
        asyncio.run(run_pipeline(
            endpoint=CDC_ENDPOINT,
            table_name=CDC_TABLE_NAME,
            config=CDC_CONFIG,
            unique_key_cols=CDC_UNIQUE_KEYS,
        ))
        logger.info("CDC ingestion completed!")

    def run_census():
        """Run Census ingestion."""
        logger.info("Starting Census ingestion pipeline...")
        asyncio.run(run_census_pipeline())
        logger.info("Census ingestion completed!")

    # Tasks
    cdc_task = PythonOperator(
        task_id="cdc_ingestion",
        python_callable=run_cdc,
    )

    census_task = PythonOperator(
        task_id="census_ingestion",
        python_callable=run_census,
    )

    # Task dependencies: CDC must complete before Census runs.
    cdc_task >> census_task

