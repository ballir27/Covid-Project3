FROM apache/airflow:2.10.5-python3.12

WORKDIR /opt/airflow/workspace

COPY --chown=airflow:root pyproject.toml uv.lock README.md ./
COPY --chown=airflow:root src ./src

USER root
ENV AIRFLOW_SITE_PACKAGES=/opt/airflow/.local/lib/python3.12/site-packages
ENV PATH="/opt/airflow/.local/bin:/opt/airflow/dbt_venv/bin:${PATH}"
ENV PYTHONPATH="${AIRFLOW_SITE_PACKAGES}:/opt/airflow/workspace/src"

RUN mkdir -p "${AIRFLOW_SITE_PACKAGES}" && chown -R airflow:root /opt/airflow

USER airflow

RUN python -m pip install --no-cache-dir --target "${AIRFLOW_SITE_PACKAGES}" --no-deps \
    "astronomer-cosmos>=1.10.2" && \
    python -m pip install --no-cache-dir --target "${AIRFLOW_SITE_PACKAGES}" \
    "aenum>=3.1.11" \
    "deprecation>=2.1.0" \
    "msgpack>=1.0.0" \
    "virtualenv>=20.21.0" \
    "httpx>=0.28.1" \
    "loguru>=0.7.3" \
    "polars>=1.33.1" \
    "python-dotenv>=1.2.2" \
    "snowflake-connector-python>=4.3.0" \
    "numpy>=1.26.4,<2.0" \
    "pandas>=2.2.2,<2.3" \
    "boto3>=1.42.75" \
    "pyarrow>=23.0.1" \
    "plotly>=6.0.0" \
    "tqdm>=4.67" \
    "streamlit>=1.56.0" && \
    python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir \
        "dbt-core>=1.11.7" \
        "dbt-snowflake>=1.11.3"
