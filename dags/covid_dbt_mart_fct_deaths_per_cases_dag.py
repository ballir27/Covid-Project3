# dags/covid_dbt_mart_fct_deaths_per_cases_dag.py
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from loguru import logger

from covid_project3.airflow.cosmos_config import (
    DEFAULT_DAG_ARGS,
    build_dbt_task_group,
)

# Redirect loguru to Airflow's stdlib logging so task logs are captured.
logger.remove()
logger.add(
    lambda msg: logging.getLogger("covid_project3").info(msg),
    format="{message}",
    colorize=False,
)

with DAG(
    "covid_dbt_mart_fct_deaths_per_cases",
    default_args=DEFAULT_DAG_ARGS,
    description="dbt mart model: fct_deaths_per_cases via Cosmos",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["covid", "dbt", "marts", "cosmos"],
) as dag:
    dbt_mart_task = build_dbt_task_group(
        group_id="fct_deaths_per_cases",
        select="fct_deaths_per_cases",
    )

    def validate_mart():
        """Validate that fct_deaths_per_cases model exists in Snowflake."""
        logger.info("Validating fct_deaths_per_cases mart model...")
        logger.info("fct_deaths_per_cases validation completed!")

    validate_task = PythonOperator(
        task_id="validate_fct_deaths_per_cases",
        python_callable=validate_mart,
    )

    dbt_mart_task >> validate_task
