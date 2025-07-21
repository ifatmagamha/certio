from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import zipfile
import os
import pymrio
import pandas as pd

# Constants
ZENODO_URL = "https://zenodo.org/records/15689391/files/IOT_2011_pxp.zip"
ZIP_PATH = "/tmp/IOT_2011_pxp.zip"
EXTRACT_DIR = "/tmp/IOT_2011_pxp/"
PARQUET_OUTPUT_PATH = "/tmp/france_Y.parquet"

# DAG default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def download_zip():
    print("Downloading with streaming...")
    with requests.get(ZENODO_URL, stream=True) as r:
        r.raise_for_status()
        with open(ZIP_PATH, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    print("✅ Download complete.")

def extract_zip():
    with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
        zip_ref.extractall(EXTRACT_DIR)


def load_and_filter_france():
    exio = pymrio.parse_exiobase3(EXTRACT_DIR, system='pxp')
    france_Y = exio.Y.loc[:, exio.Y.columns.get_level_values('region') == 'FR']
    france_Y.to_parquet(PARQUET_OUTPUT_PATH)


with DAG("zenodo_france_parquet_pipeline",
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         description="ETL pipeline: download MRIO dataset, filter France, save as Parquet",
         tags=['certio', 'pymrio', 'parquet']) as dag:

    t1 = PythonOperator(
        task_id='download_zip',
        python_callable=download_zip
    )

    t2 = PythonOperator(
        task_id='extract_zip',
        python_callable=extract_zip
    )

    t3 = PythonOperator(
        task_id='load_filter_france_save',
        python_callable=load_and_filter_france
    )

    t1 >> t2 >> t3