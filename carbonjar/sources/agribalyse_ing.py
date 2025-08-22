import os
import json
import time
import pandas as pd
import unicodedata
from base.emission_source import EmissionSource
from base.data_fetcher import DataFetcher


def clean_str(s):
    """Nettoyage : accents, casse, espaces inutiles."""
    if pd.isna(s):
        return ""
    return unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode("utf-8").lower().strip()


class AgribalyseIngredientsExtractor(EmissionSource):
    def __init__(self):
        super().__init__("AgribalyseIngredients")
        self.api_url = os.getenv("AGRIBALYSE_INGREDIENTS_URL")
        self.page_size = int(os.getenv("FETCH_PAGE_SIZE", 500))
        self.rps = int(os.getenv("REQUESTS_PER_SECOND", 9))
        self.raw_data = None
        self.normalized_data = None

    def fetch_data(self):
        print(f"[INFO] Fetching data from ADEME API: {self.api_url}")
        json_data = DataFetcher.fetch_api_adem(self.api_url, self.page_size, rps=self.rps)

        os.makedirs("data/raw", exist_ok=True)
        with open("data/raw/agribalyse_ingredients.json", "w", encoding="utf-8") as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)

        self.raw_data = pd.DataFrame(json_data)
        print(f"[INFO] Fetched {len(self.raw_data)} records.")
        return self.raw_data

    def normalize(self):
        if self.raw_data is None:
            self.fetch_data()

        df = self.raw_data.copy()
        df.columns = [clean_str(col) for col in df.columns]
        print(f"[DEBUG] Colonnes : {df.columns.tolist()}")
        print("\n processed columns :", df.columns.tolist())


        # Dictionnaire dynamique basé sur les noms possibles
        column_map = {
            "product": ["lci_name"],
            "ingredient": ["ingredients"],
            "sector": ["groupe"],
            "element": ["sous-groupe"],
            "EF": ["changement_climatique"],
            "code": ["ciqual code", "ciqual__code"]
        }

        def find_col(keywords):
            for kw in keywords:
                for col in df.columns:
                    if kw in col:
                        return col
            raise KeyError(f"[ERROR] Aucune colonne ne correspond à : {keywords}")

        df["product"] = df[find_col(column_map["product"])].astype(str).str.lower().str.strip()
        df["sector"] = df[find_col(column_map["sector"])].astype(str).str.lower().str.strip()
        df["element"] = df[find_col(column_map["element"])].astype(str).str.lower().str.strip()
        df["EF"] = df[find_col(column_map["EF"])]
        df["ingredient"] = df[find_col(column_map["ingredient"])].astype(str).str.lower().str.strip()
        df["code"] = df[find_col(column_map["code"])]
        df["country"] = "fr"
        df["unit"] = "kgco2e/kg"
        df["source"] = "agribalyse ingredients v3.1"

        self.normalized_data = df[[
            "code", "product", "sector", "element", "ingredient", "country", "EF", "unit", "source"
        ]]
        print(f"[INFO] Normalized {len(self.normalized_data)} records.")
        return self.normalized_data
