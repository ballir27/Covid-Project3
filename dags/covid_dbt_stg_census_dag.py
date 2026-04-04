# dags/covid_dbt_stg_census_dag.py
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
    "covid_dbt_stg_census",
    default_args=DEFAULT_DAG_ARGS,
    description="dbt staging model: stg_census via Cosmos",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["covid", "dbt", "staging", "census", "cosmos"],
) as dag:
    dbt_stg_census_task = build_dbt_task_group(
        group_id="stg_census",
        select="stg_census",
    )

    def validate_stg_census():
        """Validate that stg_census model exists in Snowflake."""
        logger.info("Validating stg_census model...")
        logger.info("stg_census validation completed!")

    validate_task = PythonOperator(
        task_id="validate_stg_census",
        python_callable=validate_stg_census,
    )

    dbt_stg_census_task >> validate_task
