FROM apache/airflow:2.10.5-python3.12

WORKDIR /opt/airflow/workspace

COPY --chown=airflow:root pyproject.toml uv.lock README.md ./
COPY --chown=airflow:root src ./src

USER root

RUN python -m pip install --no-cache-dir -e .

USER airflow
