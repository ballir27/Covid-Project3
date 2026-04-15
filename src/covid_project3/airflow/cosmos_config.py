from datetime import timedelta
from pathlib import Path

from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import LoadMode

from covid_project3.config import config

DBT_PROJECT_DIR = Path(
    config.get("DBT_PROJECT_DIR")
    or "/opt/airflow/workspace/covid_dbt"
)
DBT_PROFILES_FILE = Path(
    config.get("DBT_PROFILES_FILE") or "/home/airflow/.dbt/profiles.yml"
)
DBT_PROFILE_NAME = config.get("DBT_PROFILE_NAME") or "covid_project3_dbt"
DBT_TARGET_NAME = (
    config.get("DBT_TARGET_NAME") or config.get("DBT_TARGET") or "dev"
)
DEFAULT_DBT_EXECUTABLE_PATH = "/opt/airflow/workspace/scripts/dbt_safe.sh"
DBT_EXECUTABLE_PATH = config.get("DBT_EXECUTABLE_PATH") or (
    DEFAULT_DBT_EXECUTABLE_PATH
    if Path(DEFAULT_DBT_EXECUTABLE_PATH).exists()
    else "/opt/airflow/dbt_venv/bin/dbt"
)
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"

DEFAULT_DAG_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

PROJECT_CONFIG = ProjectConfig(
    dbt_project_path=DBT_PROJECT_DIR,
    manifest_path=DBT_MANIFEST_PATH if DBT_MANIFEST_PATH.exists() else None,
)
PROFILE_CONFIG = ProfileConfig(
    profile_name=DBT_PROFILE_NAME,
    target_name=DBT_TARGET_NAME,
    profiles_yml_filepath=DBT_PROFILES_FILE,
)
EXECUTION_CONFIG = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)


def build_dbt_task_group(group_id: str, select: str) -> DbtTaskGroup:
    """Create a Cosmos dbt task group for a specific model selection."""
    load_method = (
        LoadMode.DBT_MANIFEST
        if DBT_MANIFEST_PATH.exists()
        else LoadMode.DBT_LS
    )

    return DbtTaskGroup(
        group_id=group_id,
        project_config=PROJECT_CONFIG,
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG,
        render_config=RenderConfig(
            select=[select],
            load_method=load_method,
        ),
        operator_args={
            "install_deps": False,
        },
    )