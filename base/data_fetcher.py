import pandas as pd
import requests
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)


class DataFetcher:
    @staticmethod
    def fetch_csv(path, sep=",", encoding="utf-8", **kwargs):
        if path.startswith("http"):
            return pd.read_csv(path, sep=sep, encoding=encoding, **kwargs)
        return pd.read_csv(path, sep=sep, encoding=encoding, **kwargs)

    @staticmethod
    def fetch_excel(path, **kwargs):
        return pd.read_excel(path, **kwargs)

    @staticmethod
    def fetch_api_json(url, headers):
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return pd.json_normalize(response.json())

    @staticmethod
    def fetch_local_json(path):
        return pd.read_json(path)
    
    @staticmethod
    def fetch_api_adem(api_url, page_size=500, rps=9):
        results = []
        next_url = f"{api_url}/lines?page_size={page_size}"
        delay = 1 / rps

        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.headers.update({"User-Agent": "carbonjar-ingestor/1.0"})

        while next_url:
            try:
                logging.info(f"[FETCH] Requesting: {next_url}")
                response = session.get(next_url, timeout=15)

                if response.status_code != 200:
                    logging.error(f"[FAIL] Status {response.status_code} at {next_url}")
                    break

                data = response.json()
                batch = data.get("results", [])
                results.extend(batch)

                logging.info(f"[SUCCESS] {len(batch)} rows fetched")
                next_url = data.get("next")
                time.sleep(delay)

            except requests.exceptions.RequestException as e:
                logging.error(f"[EXCEPTION] Request failed: {e}")
                break

        logging.info(f"[DONE] Total records fetched: {len(results)}")
        return results
