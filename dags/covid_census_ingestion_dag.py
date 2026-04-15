# dags/covid_census_ingestion_dag.py
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
    "covid_census_ingestion",
    default_args=default_args,
    description="US Census data ingestion pipeline (direct to Snowflake)",
    schedule_interval="@weekly",  # Census data changes less frequently
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["covid", "ingestion", "census"],
) as dag:

    def run_census():
        """Run Census ingestion pipeline asynchronously."""
        from covid_project3.ingestion.engine import run_census_pipeline

        logger.info("Starting Census ingestion pipeline...")
        asyncio.run(run_census_pipeline())
        logger.info("Census ingestion completed!")

    # Task
    census_task = PythonOperator(
        task_id="census_ingestion",
        python_callable=run_census,
    )