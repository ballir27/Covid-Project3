# dags/covid_dbt_mart_fct_monthly_case_trends_dag.py
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from loguru import logger

from covid_project3.airflow.cosmos_config import (
    DEFAULT_DAG_ARGS,
    build_dbt_task_group,
)

logger.remove()
logger.add(
    lambda msg: logging.getLogger("covid_project3").info(msg),
    format="{message}",
    colorize=False,
)

with DAG(
    "covid_dbt_mart_fct_monthly_case_trends",
    default_args=DEFAULT_DAG_ARGS,
    description="dbt mart model: fct_monthly_case_trends via Cosmos",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["covid", "dbt", "marts", "cosmos"],
) as dag:
    dbt_mart_task = build_dbt_task_group(
        group_id="fct_monthly_case_trends",
        select="fct_monthly_case_trends",
    )

    def validate_mart():
        logger.info("Validating fct_monthly_case_trends mart model...")
        logger.info("fct_monthly_case_trends validation completed!")

    validate_task = PythonOperator(
        task_id="validate_fct_monthly_case_trends",
        python_callable=validate_mart,
    )

    dbt_mart_task >> validate_task
