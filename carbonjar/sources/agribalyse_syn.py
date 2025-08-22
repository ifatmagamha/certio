import pandas as pd
import unicodedata
from base.emission_source import EmissionSource
from base.data_fetcher import DataFetcher

def clean_str(s):
    """Remove accents, set lowercase, strip spaces."""
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("utf-8").lower().strip()

class AgribalyseExtractor(EmissionSource):
    def __init__(self, path):
        super().__init__("Agribalyse")
        self.path = path
        self.raw_data = None
        self.normalized_data = None
        self.environmental_impact_data = None

    def fetch_data(self):
        try:
            self.raw_data = DataFetcher.fetch_csv(self.path, sep=",", quotechar='"', encoding="cp1252")
        except Exception:
            print("Failure cp1252 → trying ISO-8859-1")
            self.raw_data = DataFetcher.fetch_csv(self.path, sep=",", quotechar='"', encoding="ISO-8859-1")

        print("\nRaw columns:", self.raw_data.columns.tolist())
        return self.raw_data

    def normalize(self):
        if self.raw_data is None:
            self.fetch_data()

        df = self.raw_data.copy()
        df.columns = [clean_str(col) for col in df.columns]
        print("\nProcessed columns:", df.columns.tolist())

        # Map keywords to clean names
        column_map = {
            "sector": ["groupe"],
            "element": ["sous-groupe"],
            "product": ["lci name"],
            "EF": ["changement climatique"],
            "livraison": ["livraison"],
            "packaging": ["approche emballage"],
            "code": ["code agb"],
            "ozone_layer_depletion": ["appauvrissement"],
            "ionizing_radiation": ["rayonnements"],
            "photochemical_ozone_formation": ["formation"],
            "fine_particulates": ["particules"],
            "non_carcinogenic_substances": ["substances non-cancaroga nes"],
            "carcinogenic_substances": ["substances cancaroga nes"],
            "terrestrial_and_freshwater_acidification": ["acidification"],
            "freshwater_eutrophication": ["eutrophisation eaux douces"],
            "marine_eutrophication": ["eutrophisation marine"],
            "terrestrial_eutrophication": ["eutrophisation terrestre"],
            "freshwater_ecotoxicity": ["acotoxicita"],
            "land_use": ["utilisation du sol"],
            "water_resource_depletion": ["apuisement des ressources eau"],
            "energy_depletion": ["apuisement des ressources anergatiques"],
            "minerals_depletion": ["apuisement des ressources minaraux"]
        }

        def find_col(keywords):
            for col in df.columns:
                if any(kw in col for kw in keywords):
                    return col
            raise KeyError(f"Column not found for keywords: {keywords}")

        # Extract product info
        df["product"] = df[find_col(column_map["product"])].astype(str).str.lower().str.strip()
        df["sector"] = df[find_col(column_map["sector"])].astype(str).str.lower().str.strip()
        df["element"] = df[find_col(column_map["element"])].astype(str).str.lower().str.strip()
        df["EF"] = df[find_col(column_map["EF"])]
        df["code"] = df[find_col(column_map["code"])]
        df["delivery"] = df[find_col(column_map["livraison"])]
        df["packaging"] = df[find_col(column_map["packaging"])]
        df["country"] = "fr"
        df["unit"] = "kgco2e/kg"
        df["source"] = "agribalyse synthese v3.2"

        # Environmental indicator columns
        impact_columns = []
        for key, keywords in column_map.items():
            if key not in ["sector", "element", "product", "EF", "livraison", "packaging", "code"]:
                col_name = find_col(keywords)
                df[key] = df[col_name]
                impact_columns.append(key)

        # Products table 
        self.normalized_data = df[[
            "code", "product", "sector", "element", "delivery", "packaging", "EF", "unit", "country", "source"
        ]]

        # Environmental impacts table 
        self.environmental_impact_data = df.melt(
            id_vars=["code", "product"],
            value_vars=impact_columns,
            var_name="indicator_code",
            value_name="value"
        )

        # Units mapping 
        unit_mapping = {
            "ozone_layer_depletion": "kg CVC11 eq/kg product",
            "ionizing_radiation": "kBq U-235 eq/kg product",
            "photochemical_ozone_formation": "kg NMVOC eq/kg product",
            "fine_particulates": "disease inc./kg product",
            "non_carcinogenic_substances": "CTUh/kg product",
            "carcinogenic_substances": "CTUh/kg product",
            "terrestrial_and_freshwater_acidification": "mol H+ eq/kg product",
            "freshwater_eutrophication": "kg P eq/kg product",
            "marine_eutrophication": "kg N eq/kg product",
            "terrestrial_eutrophication": "mol N eq/kg product",
            "freshwater_ecotoxicity": "CTUe/kg product",
            "land_use": "Pt/kg product",
            "water_resource_depletion": "m3 depriv./kg product",
            "energy_depletion": "MJ/kg product",
            "minerals_depletion": "kg Sb eq/kg product"
        }
        self.environmental_impact_data["unit"] = self.environmental_impact_data["indicator_code"].map(unit_mapping)

        return self.normalized_data, self.environmental_impact_data
