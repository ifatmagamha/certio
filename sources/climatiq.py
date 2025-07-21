from base.emission_source import EmissionSource
from base.data_fetcher import DataFetcher
import pandas as pd
import os

class ClimatiqSource(EmissionSource):
    def __init__(self, api_url: str, api_key: str):
        super().__init__("CLIMATIQ")
        self.api_url = api_url
        self.api_key = api_key

    def fetch_data(self):
        headers = {"Authorization": f"Bearer {self.api_key}"}
        self.raw_data = DataFetcher.fetch_api_json(self.api_url, headers)
        return self.raw_data

    def normalize(self):
        df = self.raw_data
        df_normalized = pd.DataFrame()
        df_normalized["product"] = df["activity_name"]
        df_normalized["sector"] = df["sector"]
        df_normalized["country"] = df["region"]
        df_normalized["kgCO2e"] = df["co2e_total"]
        df_normalized["unit"] = df["unit"]
        df_normalized["source"] = "CLIMATIQ"
        self.normalized_data = df_normalized
        return df_normalized
