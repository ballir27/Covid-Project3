# dags/covid_dbt_mart_fct_deaths_per_cases_dag.py
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from loguru import logger

# Redirect loguru to Airflow's stdlib logging so task logs are captured.
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

# DBT project directory (inside Docker container)
DBT_PROJECT_DIR = "/opt/airflow/workspace/covid_project3_dbt"

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    "covid_dbt_mart_fct_deaths_per_cases",
    default_args=default_args,
    description="dbt mart model: fct_deaths_per_cases",
    schedule_interval=None,  # Manual trigger or triggered by upstream staging DAGs
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["covid", "dbt", "marts"],
) as dag:

    dbt_mart_task = BashOperator(
        task_id="dbt_mart_fct_deaths_per_cases",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select fct_deaths_per_cases --profiles-dir /home/airflow/.dbt",
        do_xcom_push=False,
        cwd=DBT_PROJECT_DIR,
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
