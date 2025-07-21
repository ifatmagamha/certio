import pandas as pd
import requests

class DataFetcher:
    @staticmethod
    def fetch_csv(path: str, sep=",", encoding="utf-8", **kwargs):
        if path.startswith("http"):
            return pd.read_csv(path, sep=sep, encoding=encoding, **kwargs)
        return pd.read_csv(path, sep=sep, encoding=encoding, **kwargs)

    @staticmethod
    def fetch_excel(path: str, **kwargs):
        return pd.read_excel(path, **kwargs)

    @staticmethod
    def fetch_api_json(url: str, headers: dict = None):
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return pd.json_normalize(response.json())

    @staticmethod
    def fetch_local_json(path: str):
        return pd.read_json(path)
