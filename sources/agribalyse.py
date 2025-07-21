import pandas as pd
import unicodedata
from base.emission_source import EmissionSource
from base.data_fetcher import DataFetcher

def clean_str(s):
    """Nettoyage : accents, casse, espaces inutiles."""
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("utf-8").lower().strip()

class AgribalyseExtractor(EmissionSource):
    def __init__(self, path):
        super().__init__("Agribalyse")
        self.path = path
        self.raw_data = None
        self.normalized_data = None

    def fetch_data(self):
        try:
            self.raw_data = DataFetcher.fetch_csv(self.path, sep=",", quotechar='"', encoding="cp1252")
        except Exception:
            print("Failure cp1252 → test ISO-8859-1")
            self.raw_data = DataFetcher.fetch_csv(self.path, sep=",", quotechar='"', encoding="ISO-8859-1")

        print("\n raw columns :", self.raw_data.columns.tolist())
        return self.raw_data

    def normalize(self):
        if self.raw_data is None:
            self.fetch_data()

        df = self.raw_data.copy()
        df.columns = [clean_str(col) for col in df.columns]
        print("\n processed columns :", df.columns.tolist())

        # Mappage intelligent basé sur des mots-clés
        column_map = {
            "element": ["sous-groupe"],
            "sector": ["groupe"],
            "product": ["lci name"],
            "EF": ["score unique ef"],
            "livraison": ["livraison"],
            "packaging": ["approche emballage"],
            "code": ["code agb"]
        }

        def find_col(keywords):
            for col in df.columns:
                if any(kw in col for kw in keywords):
                    return col
            raise KeyError(f"not found: {keywords}")

        # Extraction des bonnes colonnes + conversion en minuscules si texte
        df["product"] = df[find_col(column_map["product"])].astype(str).str.lower().str.strip()
        df["sector"] = df[find_col(column_map["sector"])].astype(str).str.lower().str.strip()
        df["element"] = df[find_col(column_map["element"])].astype(str).str.lower().str.strip()
        df["EF"] = df[find_col(column_map["EF"])]
        df["code"] = df[find_col(column_map["code"])]
        df["country"] = "fr"
        df["unit"] = "kgco2e/kg"
        df["source"] = "agribalyse"

        self.normalized_data = df[[
            "product", "sector", "element", "country", "EF", "unit", "source", "code"
        ]]

        return self.normalized_data
