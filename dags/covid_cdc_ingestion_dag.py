# dags/covid_cdc_ingestion_dag.py
import asyncio
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from loguru import logger

logger.remove()
logger.add(
    lambda msg: logging.getLogger("covid_project3").info(msg),
    format="{message}",
    colorize=False,
)

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
    "covid_cdc_ingestion",
    default_args=default_args,
    description="CDC case data ingestion pipeline",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["covid", "ingestion", "cdc"],
) as dag:

    def run_cdc():
        """Run CDC ingestion pipeline asynchronously."""
        from covid_project3.config import config
        from covid_project3.ingestion.engine import run_pipeline

        cdc_endpoint = config.get("CDC_COVID_API_ENDPOINT")
        cdc_table_name = "CDC_RAW_CASES_1"
        cdc_config = {
            "LIMIT": int(config.get("LIMIT", 10000)),
            "CONCURRENT_REQUESTS": int(config.get("CONCURRENT_REQUESTS", 10)),
        }
        cdc_unique_keys = ["case_month", "res_state", "county_fips_code"]

        logger.info("Starting CDC ingestion pipeline...")
        asyncio.run(
            run_pipeline(
                endpoint=cdc_endpoint,
                table_name=cdc_table_name,
                config=cdc_config,
                unique_key_cols=cdc_unique_keys,
            )
        )
        logger.info("CDC ingestion completed!")

    # Task
    cdc_task = PythonOperator(
        task_id="cdc_ingestion",
        python_callable=run_cdc,
    )