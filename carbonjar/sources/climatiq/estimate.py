import pandas as pd
from pathlib import Path
import json
import time
import requests
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
import os

load_dotenv()

class EstimatorByRegion:
    def __init__(self, api_key: str, region: str, parameter_rules_path: Path):
        self.api_key = api_key
        self.headers = {"Authorization": f"Bearer {self.api_key}"}
        self.region = region
        self.session = requests.Session()
        retries = Retry(connect=3, backoff_factor=0.5)
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.base_url = "https://api.climatiq.io/data/v1"

        with open(parameter_rules_path, "r", encoding="utf-8") as f:
            self.parameter_rules = json.load(f)

    def estimate_activity(self, row):
        unit = row.get("unit")
        param_key, param_value, param_unit = self.parameter_rules.get(
            unit,
            ("energy", 1000, "kWh")  # fallback
        )

        payload = {
            "emission_factor": {
                "activity_id": row["activity_id"],
                "source": row["source"],
                "region": self.region,
                "year": row["year"],
                "source_lca_activity": row.get("source_lca_activity", "unknown"),
                "data_version": "^0"
            },
            "parameters": {
                param_key: param_value
            }
        }

        if param_unit:
            payload["parameters"][f"{param_key}_unit"] = param_unit

        try:
            response = self.session.post(f"{self.base_url}/estimate", headers=self.headers, json=payload)
            r = response.json()

            # Save JSON response
            activity_id = row["activity_id"]
            ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            output_dir = Path(f"data/raw/climatiq/{self.region}/{activity_id}")
            output_dir.mkdir(parents=True, exist_ok=True)
            with open(output_dir / f"{ts}.json", "w", encoding="utf-8") as f:
                json.dump(r, f, indent=2)

            return {
                "activity_id": activity_id,
                "co2e": r.get("co2e"),
                "co2e_unit": r.get("co2e_unit"),
                "activity_value": r.get("activity_value", param_value),
                "EF": float(r["co2e"]) / float(r["activity_value"]) if r.get("co2e") and r.get("activity_value") else None,
                "used_unit": unit,
                "used_param_key": param_key,
                "used_param_unit": param_unit,
                "error": None
            }

        except Exception as e:
            return {
                "activity_id": row["activity_id"],
                "error": str(e)
            }

    def run(self, input_csv: Path, output_csv: Path):
        df = pd.read_csv(input_csv)
        results = []

        for _, row in df.iterrows():
            result = self.estimate_activity(row)
            results.append(result)
            time.sleep(0.5)  # be polite with API

        pd.DataFrame(results).to_csv(output_csv, index=False)
        print(f"[INFO] Estimations saved in {output_csv}")


if __name__== "__main__":
    region = "TN"
    api_key = os.getenv("CLIMATIQ_API_KEY")

    estimator = EstimatorByRegion(api_key=api_key, region=region)
    estimator.run(
        input_csv=Path(f"data/raw/climatiq_search_{region}.csv"),
        output_csv=Path(f"data/processed/climatiq_estimate_{region}.csv")   
    )