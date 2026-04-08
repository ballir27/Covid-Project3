# dags/covid_master_pipeline_dag.py
from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

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
# DAG definition - Orchestrates the entire COVID pipeline
# ---------------------------------------------------------------------------
with DAG(
    "covid_master_pipeline",
    default_args=default_args,
    description=(
        "Master orchestration DAG: runs ingestion, "
        "and mart DAGs"
    ),
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["covid", "orchestration", "master"],
) as dag:

    # Wait for CDC ingestion to complete
    wait_for_cdc = ExternalTaskSensor(
        task_id="wait_for_cdc_ingestion",
        external_dag_id="covid_cdc_ingestion",
        external_task_id="cdc_ingestion",
        poke_interval=60,  # Check every 60 seconds
        timeout=3600,  # Timeout after 1 hour
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
    )

    # Wait for Census ingestion to complete
    wait_for_census = ExternalTaskSensor(
        task_id="wait_for_census_ingestion",
        external_dag_id="covid_census_ingestion",
        external_task_id="census_ingestion",
        poke_interval=60,
        timeout=3600,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
    )

    # Wait for stg_cdc_cases transformation to complete
    wait_for_stg_cdc_cases = ExternalTaskSensor(
        task_id="wait_for_stg_cdc_cases",
        external_dag_id="covid_dbt_stg_cdc_cases",
        external_task_id="validate_stg_cdc_cases",
        poke_interval=60,  # Check every 60 seconds
        timeout=3600,  # Timeout after 1 hour
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
    )

    # Wait for stg_census transformation to complete
    wait_for_stg_census = ExternalTaskSensor(
        task_id="wait_for_stg_census",
        external_dag_id="covid_dbt_stg_census",
        external_task_id="validate_stg_census",
        poke_interval=60,
        timeout=3600,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
    )

    # Wait for current mart models to complete
    wait_for_mart_deaths = ExternalTaskSensor(
        task_id="wait_for_mart_fct_deaths_per_cases",
        external_dag_id="covid_dbt_mart_fct_deaths_per_cases",
        external_task_id="validate_fct_deaths_per_cases",
        poke_interval=60,
        timeout=3600,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
    )

    wait_for_mart_covid_by_fips = ExternalTaskSensor(
        task_id="wait_for_mart_fct_covid_by_fips",
        external_dag_id="covid_dbt_mart_fct_covid_by_fips",
        external_task_id="validate_fct_covid_by_fips",
        poke_interval=60,
        timeout=3600,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
    )

    # Set dependencies:
    # Ingestions -> staging models -> marts
    [wait_for_cdc, wait_for_census] >> wait_for_stg_cdc_cases
    [wait_for_cdc, wait_for_census] >> wait_for_stg_census

    [wait_for_stg_cdc_cases, wait_for_stg_census] >> wait_for_mart_deaths
    [wait_for_stg_cdc_cases, wait_for_stg_census] >> (
        wait_for_mart_covid_by_fips
    )
