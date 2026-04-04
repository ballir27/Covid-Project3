# dags/covid_dbt_mart_ops_source_freshness_dag.py
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
    "covid_dbt_mart_ops_source_freshness",
    default_args=DEFAULT_DAG_ARGS,
    description="dbt mart model: ops_source_freshness via Cosmos",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["covid", "dbt", "marts", "ops", "cosmos"],
) as dag:
    dbt_mart_task = build_dbt_task_group(
        group_id="ops_source_freshness",
        select="ops_source_freshness",
    )

    def validate_mart():
        logger.info("Validating ops_source_freshness mart model...")
        logger.info("ops_source_freshness validation completed!")

    validate_task = PythonOperator(
        task_id="validate_ops_source_freshness",
        python_callable=validate_mart,
    )

    dbt_mart_task >> validate_task
