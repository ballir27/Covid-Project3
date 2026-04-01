# dags/covid_dbt_staging_dag.py
"""
DEPRECATED: This DAG has been replaced with separate DAGs for each model:
  - covid_dbt_stg_cdc_cases_dag.py: Runs stg_cdc_cases model only
  - covid_dbt_stg_census_dag.py: Runs stg_census model only

This allows each staging model to be triggered independently.
"""
