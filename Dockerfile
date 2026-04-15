FROM apache/airflow:2.10.5-python3.12

WORKDIR /opt/airflow/workspace

COPY --chown=airflow:root pyproject.toml uv.lock README.md ./
COPY --chown=airflow:root src ./src

USER airflow

RUN python3 -m venv /opt/airflow/dbt_venv \
    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir \
        "dbt-core==1.11.7" \
        "dbt-snowflake==1.11.4" \
    && /home/airflow/.local/bin/pip install --no-cache-dir \
        "astronomer-cosmos==1.14.0" \
        httpx \
        python-dotenv \
        boto3 \
        snowflake-connector-python \
        loguru \
        polars \
        pyarrow \
        tqdm \
    && /home/airflow/.local/bin/pip install --no-cache-dir --no-deps -e .
