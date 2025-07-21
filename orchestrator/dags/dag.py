from airflow import dag, task
from datetime import datetime


@dag
def dag():

    @task
    def training_model_a():
        return 1
    
    @task
    def training_model_b():
        return 2
    
    @task
    def training_model_c(): 
        return 3
    
    @task.branch
    def choose_best_model():
        if accuracy > 2:
            return "accurate_model"
        return "inaccurate_model"
    
    @task.bash
    def accurate():
        return "echo 'accurate model'"
    
    @task.bash
    def inaccurate():
        return "echo 'inaccurate model'"
    
dag()