# dags/covid_dbt_stg_census_dag.py
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
    "covid_dbt_stg_census",
    default_args=default_args,
    description="dbt staging model: stg_census",
    schedule_interval=None,  # Manual trigger or triggered by upstream DAGs
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["covid", "dbt", "staging", "census"],
) as dag:

    # Using BashOperator to run dbt
    dbt_stg_census_task = BashOperator(
        task_id="dbt_stg_census",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select stg_census --profiles-dir /home/airflow/.dbt",
        do_xcom_push=False,
        cwd=DBT_PROJECT_DIR,
    )

    # Optional: Add a Python task that validates the model was created
    def validate_stg_census():
        """Validate that stg_census model exists in Snowflake."""
        logger.info("Validating stg_census model...")
        # Add your validation logic here (e.g., row count check, schema validation)
        logger.info("stg_census validation completed!")

    validate_task = PythonOperator(
        task_id="validate_stg_census",
        python_callable=validate_stg_census,
    )

    # Task dependencies
    dbt_stg_census_task >> validate_task
