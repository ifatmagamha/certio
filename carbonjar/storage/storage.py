import os
import pandas as pd
from sqlalchemy import create_engine

class StorageManager:
    def __init__(self, db_url):
        self.engine = create_engine(db_url)

    def load_csv(self, csv_path):
        df = pd.read_csv(csv_path)
        print(f"[INFO] Loaded {len(df)} rows from {csv_path}")
        return df

    def clean_columns(self, df):
        df.columns = [col.lower().strip().replace(" ", "_") for col in df.columns]
        return df

    def insert_to_db(self, df, table_name: str = "emission_factors"):
        df = self.clean_columns(df)
        df.to_sql(table_name, con=self.engine, if_exists="append", index=False)
        print(f"[INFO] Inserted {len(df)} rows into {table_name}")

if __name__ == "__main__":
    DB_URL = "sqlite:///data/emission_factors.db"
    # For PostgreSQL: 'postgresql://user:password@localhost:5432/yourdb'

    etl = StorageManager(db_url=DB_URL)
    
    # Example CSV paths (normalized output)
    csv_files = [
        "data/processed/agribalyse_normalized.csv",
        "data/processed/climatiq_estimate_TN.csv",
        "data/processed/agribalysesteps_normalized.csv"
    ]

    for file_path in csv_files:
        if os.path.exists(file_path):
            df = etl.load_csv(file_path)
            etl.insert_to_db(df)
        else:
            print(f"[WARNING] File not found: {file_path}")
