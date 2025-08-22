import os
import json
import time
import random
from datetime import datetime
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add project root to sys.path if needed
sys.path.append(str(Path(__file__).resolve().parents[1]))  # Go up to project root

# Custom modules
from config import PARAMETER_RULES
from climatiq.search import ClimatiqSearchClient
from climatiq.parameters_rules_generator import generate_parameter_rules
from climatiq.processing import filter_csv_by_region
from climatiq.estimate import EstimatorByRegion

class ClimatiqPipeline:
    def __init__(self, api_key: str, region: str):
        self.api_key = api_key
        self.region = region

        self.raw_dir = Path("data/raw")
        self.config_dir = Path("data/config")
        self.processed_dir = Path("data/processed")

        self.search_client = ClimatiqSearchClient(api_key)
        rules_output = self.config_dir / f"generated_parameter_rules_{self.region}.json"
        self.estimator = EstimatorByRegion(api_key, region, rules_output)


    def run(self):
        filters = {
            "data_version": "^0",
            "query": "",
            "activity_id": "",
            "category": "",
            "sector": "",
            "region": self.region,
            "source": "",
            "year": "",
            "unit_type": "",
        }

        # Step 1 – Search & Save
        df = self.search_client.fetch_all_pages(filters)
        self.search_client.save_results(df, self.raw_dir, self.region)
        search_csv = self.raw_dir / f"climatiq_search_{self.region}.csv"
        grouped_df = self.search_client.grouped_df(df)

        # Step 2 – Generate PARAMETER_RULES
        grouped_path = self.raw_dir / f"climatiq_grouped_{self.region}.csv"
        grouped_df.to_csv(grouped_path, index=False)
        generate_parameter_rules(grouped_path, self.config_dir / f"generated_parameter_rules_{self.region}.json")

        # Step 3 – Estimate emissions
        estimate_output = self.processed_dir / f"climatiq_estimates_grouped_{self.region}.csv"
        self.estimator.run(grouped_df, estimate_output)


if __name__ == "__main__":
    region = "FR"
    api_key = os.getenv("CLIMATIQ_API_KEY")

    pipeline = ClimatiqPipeline(api_key=api_key, region=region)
    pipeline.run()
