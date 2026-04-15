from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "covid_master_pipeline",
    default_args=default_args,
    description=(
        "Master DAG for CDC/Census ingestion and downstream dbt staging and mart models"
    ),
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["covid", "master", "orchestration"],
) as dag:
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    trigger_cdc_ingestion = TriggerDagRunOperator(
        task_id="trigger_cdc_ingestion",
        trigger_dag_id="covid_cdc_ingestion",
        wait_for_completion=True,
        poke_interval=20,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_census_ingestion = TriggerDagRunOperator(
        task_id="trigger_census_ingestion",
        trigger_dag_id="covid_census_ingestion",
        wait_for_completion=True,
        poke_interval=20,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_stg_cdc_cases = TriggerDagRunOperator(
        task_id="trigger_stg_cdc_cases",
        trigger_dag_id="covid_dbt_stg_cdc_cases",
        wait_for_completion=True,
        poke_interval=20,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_stg_census = TriggerDagRunOperator(
        task_id="trigger_stg_census",
        trigger_dag_id="covid_dbt_stg_census",
        wait_for_completion=True,
        poke_interval=20,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_fct_deaths_per_cases = TriggerDagRunOperator(
        task_id="trigger_fct_deaths_per_cases",
        trigger_dag_id="covid_dbt_mart_fct_deaths_per_cases",
        wait_for_completion=True,
        poke_interval=20,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_fct_covid_by_fips = TriggerDagRunOperator(
        task_id="trigger_fct_covid_by_fips",
        trigger_dag_id="covid_dbt_mart_fct_covid_by_fips",
        wait_for_completion=True,
        poke_interval=20,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    start >> [trigger_cdc_ingestion, trigger_census_ingestion]
    trigger_cdc_ingestion >> trigger_stg_cdc_cases
    trigger_census_ingestion >> trigger_stg_census
    trigger_stg_cdc_cases >> trigger_fct_deaths_per_cases >> finish
    [trigger_stg_cdc_cases, trigger_stg_census] >> trigger_fct_covid_by_fips >> finish
