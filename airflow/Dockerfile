FROM apache/airflow:2.10.5-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir boto3 pandas pyarrow requests
# /home/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-athena-community : c'est pas du pip normale mais pip de l'env virt cree
RUN python -m venv /home/airflow/dbt_venv && /home/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-athena-community





