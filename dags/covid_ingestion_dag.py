# dags/covid_ingestion_dag.py
"""
DEPRECATED: This DAG has been refactored into separate, independent DAGs:
  - covid_cdc_ingestion_dag.py: Runs CDC ingestion only (daily)
  - covid_census_ingestion_dag.py: Runs Census ingestion only (weekly)
  - covid_dbt_staging_dag.py: Runs dbt staging models
  - covid_master_pipeline_dag.py: Orchestrates all three (optional)

This allows each ingestion to be triggered independently or together via the master DAG.

For backwards compatibility, this file now imports and reuses the separate DAG definitions.
"""

from covid_cdc_ingestion_dag import dag as cdc_dag
from covid_census_ingestion_dag import dag as census_dag

# Note: Individual DAGs are now in separate files for modularity and independent triggering.
# All three DAGs (CDC, Census, and dbt staging) can now be triggered separately.

