#!/usr/bin/env bash
set -euo pipefail

unset PYTHONPATH || true
export PYTHONNOUSERSITE=1

exec /opt/airflow/dbt_venv/bin/dbt "$@"
