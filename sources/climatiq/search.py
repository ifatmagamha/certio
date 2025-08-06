import os
import json
import time
import random
from datetime import datetime
from pathlib import Path
from typing import Dict
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class ClimatiqSearchClient:
    """
    Client for interacting with the Climatiq /search API endpoint.

    Responsibilities:
    - Build query from filter dictionary
    - Handle pagination
    - Save full and grouped emission factor results
    """

    def __init__(self, api_key: str, base_url: str = "https://api.climatiq.io/data/v1/search"):
        """
        Initialize the client session with retry strategy.

        :param api_key: API key for Climatiq
        :param base_url: Base URL of the search endpoint
        """
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {"Authorization": f"Bearer {self.api_key}"}
        self.session = requests.Session()

        retries = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def build_query(self, filters: Dict) -> str:
        """
        Construct a query string from filters.

        :param filters: Dictionary of filters to apply
        :return: Encoded query string
        """
        query = "?results_per_page=500"
        for key, value in filters.items():
            if value:
                query += f"&{key}={value}"
        return query

    def fetch_all_pages(self, filters: Dict) -> pd.DataFrame:
        """
        Fetch all pages of results for a given filter query.

        :param filters: Dictionary of search filters
        :return: Concatenated DataFrame of all results
        """
        query = self.build_query(filters)
        current_page = 1
        no_of_pages = 1
        results = pd.DataFrame()

        while current_page <= no_of_pages:
            full_url = f"{self.base_url}{query}&page={current_page}"
            response = self.session.get(full_url, headers=self.headers)
            response.raise_for_status()
            data = response.json()

            current_page += 1
            no_of_pages = data.get("last_page", 1)
            df = pd.DataFrame.from_records(data.get("results", []))
            if not df.empty and not df.isna().all(axis=None):
                results = pd.concat([results, df], ignore_index=True)

        return results

    def save_results(self, df: pd.DataFrame, out_dir: Path, region: str):
        """
        Save raw and grouped results to specific files.

        :param df: DataFrame to save
        :param out_dir: Directory to save files to
        :param region: Region code used in file naming
        """
        out_dir.mkdir(parents=True, exist_ok=True)

        raw_path = out_dir / f"climatiq_search_{region}.csv"
        grouped_path = out_dir / f"climatiq_grouped_{region}.csv"


        df.to_csv(raw_path, index=False)

        short_df = df[['activity_id', 'name', 'category', 'sector', 'source', 'unit_type']].drop_duplicates()
        final = short_df.groupby(['activity_id', 'name', 'category', 'sector'])['source'].apply(','.join).reset_index()
        final["unit_type"] = short_df.groupby(['activity_id', 'name', 'category', 'sector'])['unit_type'].apply(','.join).reset_index()['unit_type']
        final.to_csv(grouped_path, index=False)

        print(f"[INFO] Saved raw data to {raw_path}")
        print(f"[INFO] Saved grouped data to {grouped_path}")

    def save_raw_search(df: pd.DataFrame, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)
        print(f"[INFO] Raw search saved to {path}")


    def grouped_df(df: pd.DataFrame) -> pd.DataFrame:
        short_df = df[['activity_id', 'name', 'category', 'sector', 'source', 'unit_type', 'unit']].drop_duplicates()
        grouped = short_df.groupby(['activity_id', 'name', 'category', 'sector'])['source'].apply(','.join).reset_index()
        grouped["unit_type"] = (
        short_df.groupby(['activity_id', 'name', 'category', 'sector'])['unit_type']
        .apply(','.join)
        .reset_index()['unit_type']
     )
        return grouped


if __name__ == "__main__":
    region = "TN"
    api_key = os.getenv("CLIMATIQ_API_KEY")

    filters = {
        "data_version": "^0",
        "query": "",
        "activity_id": "",
        "category": "",
        "sector": "",
        "region": region,
        "source": "",
        "year": "",
        "unit_type": "",
    }


    client = ClimatiqSearchClient(api_key)
    df = client.fetch_all_pages(filters)
    client.save_results(df, Path("data/raw"), region)
