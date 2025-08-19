from airflow import DAG
from airflow.utils.dates import days_ago
#from airflow.operators.dummy import DummyOperator
from airflow.operators.empty import EmptyOperator
from dags.utils.task_groups import extract_group, transform_group, load_group
from datetime import timedelta

default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": True,
    "email": ["gamhaif@gmail.com"],
}

AGRIBALYSE_PARAMS = {
    "datasets": ["synthese", "detail_par_etape", "detail_par_ingredient"],
    "format": "csv",
    "lang": "fr",
    "source_type": "remote_csv"
}

with DAG(
    dag_id="agribalyse_extraction_pipeline",
    default_args=default_args,
    description="ETL Agribalyse ADEME - Food lifecycle emission factors",
    schedule_interval="@yearly",
    start_date=days_ago(1),
    catchup=False,
    tags=["agribalyse", "ademe", "lca"],
    doc_md="""
### AGRIBALYSE Pipeline

Extraction des données LCA alimentaires depuis les jeux de données Agribalyse (ADEME), décomposition par étape ou ingrédient.
"""
) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    extract = extract_group("agribalyse", AGRIBALYSE_PARAMS)
    transform = transform_group("agribalyse")
    load = load_group("agribalyse")

    start >> extract >> transform >> load >> end
