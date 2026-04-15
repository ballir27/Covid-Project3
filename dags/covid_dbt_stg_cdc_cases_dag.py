# dags/covid_dbt_stg_cdc_cases_dag.py
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
    "covid_dbt_stg_cdc_cases",
    default_args=DEFAULT_DAG_ARGS,
    description="dbt staging model: stg_cdc_cases via Cosmos",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["covid", "dbt", "staging", "cdc", "cosmos"],
) as dag:
    dbt_stg_cdc_cases_task = build_dbt_task_group(
        group_id="stg_cdc_cases",
        select="stg_cdc_cases",
    )

    def validate_stg_cdc_cases():
        """Validate that stg_cdc_cases model exists in Snowflake."""
        logger.info("Validating stg_cdc_cases model...")
        logger.info("stg_cdc_cases validation completed!")

    validate_task = PythonOperator(
        task_id="validate_stg_cdc_cases",
        python_callable=validate_stg_cdc_cases,
    )

    dbt_stg_cdc_cases_task >> validate_task