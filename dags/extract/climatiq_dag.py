from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from dags.utils.task_groups import extract_group, transform_group, load_group
from datetime import timedelta

default_args = {
    "owner": "data-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": True,
    "email": ["gamhaif@gmail.com"],
}

CLIMATIQ_PARAMS = {
    "api_key": "{{ var.value.CLIMATIQ_API_KEY }}",  # via Airflow Variable
    "region_filter": "Tunisia",
    "category": "industry",
    "source_type": "api"
}

with DAG(
    dag_id="climatiq_extraction_pipeline",
    default_args=default_args,
    description="Extract-transform-load Climatiq data",
    schedule_interval="@yearly",
    start_date=days_ago(1),
    catchup=False,
    tags=["climatiq", "etl"],
    doc_md="""
### CLIMATIQ Pipeline

Pipeline ETL pour extraire les facteurs d'émissions depuis l'API Climatiq (scope 1, 2), filtrés par région.
"""
) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    extract = extract_group("climatiq", CLIMATIQ_PARAMS)
    transform = transform_group("climatiq")
    load = load_group("climatiq")

    start >> extract >> transform >> load >> end
