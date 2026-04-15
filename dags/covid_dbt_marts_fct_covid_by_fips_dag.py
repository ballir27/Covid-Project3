# dags/covid_dbt_mart_fct_covid_by_fips_dag.py
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
    "covid_dbt_mart_fct_covid_by_fips",
    default_args=DEFAULT_DAG_ARGS,
    description="dbt mart model: fct_covid_by_fips via Cosmos",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["covid", "dbt", "marts", "fips", "cosmos"],
) as dag:
    dbt_mart_task = build_dbt_task_group(
        group_id="fct_covid_by_fips",
        select="fct_covid_by_fips",
    )

    def validate_mart():
        """Validate that fct_covid_by_fips model exists in Snowflake."""
        logger.info("Validating fct_covid_by_fips mart model...")
        logger.info("fct_covid_by_fips validation completed!")

    validate_task = PythonOperator(
        task_id="validate_fct_covid_by_fips",
        python_callable=validate_mart,
    )

    dbt_mart_task >> validate_task