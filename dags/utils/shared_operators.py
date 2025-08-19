# dags/utils/shared_operators.py

def extract_data(source, params):
    print(f"[EXTRACT] from {source} with {params}")

def transform_data(source):
    print(f"[TRANSFORM] for {source}")

def load_data(source):
    print(f"[LOAD] to storage for {source}")
