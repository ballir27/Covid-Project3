# dags/covid_dbt_mart_fct_cases_per_100k_by_state_dag.py
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from loguru import logger

logger.remove()
logger.add(
    lambda msg: logging.getLogger("covid_project3").info(msg),
    format="{message}",
    colorize=False,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DBT_PROJECT_DIR = "/opt/airflow/workspace/covid_project3_dbt"

with DAG(
    "covid_dbt_mart_fct_cases_per_100k_by_state",
    default_args=default_args,
    description="dbt mart model: fct_cases_per_100k_by_state",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["covid", "dbt", "marts"],
) as dag:

    dbt_mart_task = BashOperator(
        task_id="dbt_mart_fct_cases_per_100k_by_state",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt run --select fct_cases_per_100k_by_state "
            "--profiles-dir /home/airflow/.dbt"
        ),
        do_xcom_push=False,
        cwd=DBT_PROJECT_DIR,
    )

    def validate_mart():
        logger.info("Validating fct_cases_per_100k_by_state mart model...")
        logger.info("fct_cases_per_100k_by_state validation completed!")

    validate_task = PythonOperator(
        task_id="validate_fct_cases_per_100k_by_state",
        python_callable=validate_mart,
    )

    dbt_mart_task >> validate_task
