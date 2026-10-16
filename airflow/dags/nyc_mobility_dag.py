import sys
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

sys.path.append("/opt/airflow")      # pour retrouver le dossier "ingestion"

DBT = "cd /opt/airflow/dbt_project && /home/airflow/dbt_venv/bin/dbt"
START = "{{ logical_date.start_of('month').to_date_string() }}"
END = "{{ logical_date.end_of('month').to_date_string() }}"
VARS = f"--vars '{{start_date: {START}, end_date: {END}}}'"


@dag(
    dag_id="nyc_mobility",
    schedule="@monthly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    end_date=pendulum.datetime(2025, 3, 1, tz="UTC"),   # 3 runs : jan, fev, mars
    catchup=True,
    max_active_runs=1,                                   # un mois a la fois
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=45),
    },
    tags=["nyc"],
)
def nyc_mobility():

    @task
    def extract_trips(logical_date=None):
        from ingestion.extract import extract_trips as run
        run(logical_date.year, logical_date.month)

    @task
    def extract_weather(logical_date=None):
        from ingestion.extract import extract_weather as run
        run(logical_date.year, logical_date.month)

    @task
    def extract_zones():
        from ingestion.extract import extract_zones as run
        run()

    @task
    def to_silver(logical_date=None):
        from ingestion import to_silver as s
        s.trips_to_silver(logical_date.year, logical_date.month)
        s.weather_to_silver(logical_date.year, logical_date.month)
        s.zones_to_silver()

    dbt_run = BashOperator(task_id="dbt_run",     bash_command=f"{DBT} --log-path /tmp/dbt_logs run {VARS}")
    dbt_test = BashOperator(task_id="dbt_test", bash_command=f"{DBT} --log-path /tmp/dbt_logs test {VARS}")

    [extract_trips(), extract_weather(), extract_zones()] >> to_silver() >> dbt_run >> dbt_test


nyc_mobility()