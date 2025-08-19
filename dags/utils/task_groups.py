# dags/utils/task_groups.py

from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from dags.utils.shared_operators import extract_data, transform_data, load_data

def extract_group(source_name: str, params: dict):
    with TaskGroup(group_id=f"{source_name}_extract") as tg:
        extract = PythonOperator(
            task_id=f"extract_{source_name}",
            python_callable=extract_data,
            op_kwargs={"source": source_name, "params": params}
        )
    return tg

def transform_group(source_name: str):
    with TaskGroup(group_id=f"{source_name}_transform") as tg:
        transform = PythonOperator(
            task_id=f"transform_{source_name}",
            python_callable=transform_data,
            op_kwargs={"source": source_name}
        )
    return tg

def load_group(source_name: str):
    with TaskGroup(group_id=f"{source_name}_load") as tg:
        load = PythonOperator(
            task_id=f"load_{source_name}",
            python_callable=load_data,
            op_kwargs={"source": source_name}
        )
    return tg
