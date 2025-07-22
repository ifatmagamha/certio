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


class BaseCarbonExtractor(EmissionSource):
    def __init__(self):
        super().__init__("BaseCarbone")
        self.api_url = os.getenv("BASE_CARBONE_API_URL")
        self.page_size = int(os.getenv("FETCH_PAGE_SIZE", 500))
        self.rps = int(os.getenv("REQUESTS_PER_SECOND", 9))
        self.raw_data = None
        self.normalized_data = None

    def fetch_data(self):
        print(f"[INFO] Fetching data from ADEME API: {self.api_url}")
        json_data = DataFetcher.fetch_api_adem(self.api_url, self.page_size, rps=self.rps)

        # Sauvegarde brute
        os.makedirs("data/raw", exist_ok=True)
        with open("data/raw/base_carbone_raw.json", "w", encoding="utf-8") as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)

        self.raw_data = pd.DataFrame(json_data)
        print(f"[INFO] Fetched {len(self.raw_data)} records.")
        return self.raw_data

    def normalize(self):
        if self.raw_data is None:
            self.fetch_data()

        df = self.raw_data.copy()

        # Nettoyer les colonnes
        df.columns = [clean_str(col) for col in df.columns]
        print(f"\n processed columns: {df.columns.tolist()}")

        def extract_field(comment, field):
            try:
                parts = [s.strip() for s in comment.split(" - ")]
                for part in parts:
                    if part.lower().startswith(field.lower()):
                        return part.split(":", 1)[1].strip()
            except Exception:
                return None
            return None

        df["product"] = df.get("nom_base_anglais", "").apply(clean_str)
        df["EF"] = df.get("qualite")
        df["country"] = df.get("localisation_geographique", "France")
        df["date"] = df.get("date_de_modification")
        df["element_id"] = df.get("identifiant_de_l_element")
        df["source"] = df.get("source", "Base Carbone via ADEME")

        # Commentaire (packaging, livraison, préparation)
        df["commentaire"] = df.get("commentaire_francais", "")
        df["delivery"] = df["commentaire"].apply(lambda x: extract_field(x, "Livraison"))
        df["packaging"] = df["commentaire"].apply(lambda x: extract_field(x, "Matériau d'emballage"))
        df["preparation"] = df["commentaire"].apply(lambda x: extract_field(x, "Préparation"))

        # Gestion dynamique des catégories (splits multiples via >)
        df["code_categorie"] = df.get("code_de_la_categorie", "")
        categories_split = df["code_categorie"].str.split(" > ", expand=True)
        for i in range(categories_split.shape[1]):
            df[f"category_lvl_{i+1}"] = categories_split[i]

        # Final selection
        category_cols = [f"category_lvl_{i+1}" for i in range(categories_split.shape[1])]
        self.normalized_data = df[
            ["product", "EF", "country", "packaging", "delivery", "preparation", "date", "element_id", "source"] + category_cols
        ]
        print(f"[INFO] Normalized {len(self.normalized_data)} records.")
        return self.normalized_data
