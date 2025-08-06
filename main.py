from sources.base_carbone import BaseCarbonExtractor
from sources.agribalyse_syn import AgribalyseExtractor
from sources.agribalyse_ing import AgribalyseIngredientsExtractor
from sources.agribalyse_per_steps import AgribalyseStepsExtractor
import pyarrow.parquet as pq
import pandas as pd
from base.emission_source import EmissionSource
from dotenv import load_dotenv
                                   

load_dotenv()

# ----- AGRIBALYSE syntèse-----
agribalyse_syn = AgribalyseExtractor("data/raw/agribalyse-31-synthese.csv")

agribalyse_syn.fetch_data()
agribalyse_syn_df = agribalyse_syn.normalize()
agribalyse_syn.save_to_csv("data/processed")
print("[AGRIBALYSE] Sample:")
print(agribalyse_syn_df.head())

agribalyse_csv = pd.read_csv("data/processed/agribalyse_normalized.csv")
print("[AGRIBALYSE CSV] Loaded:")
print(agribalyse_csv.head())

# ----- AGRIBALYSE étapes-----
agribalyse_per_step = AgribalyseStepsExtractor()
agribalyse_per_step.fetch_data()
agribalyse_per_step_df = agribalyse_per_step.normalize()
agribalyse_per_step.save_to_csv("data/processed")
print("[AGRIBALYSE étapes v3.2] Sample:")
print(agribalyse_per_step_df.head())

# ----- AGRIBALYSE ingrédients-----
agribalyse_ingredients = AgribalyseIngredientsExtractor()
agribalyse_ingredients.fetch_data()
agribalyse_ingredients_df = agribalyse_ingredients.normalize()
agribalyse_ingredients.save_to_csv("data/processed")
print("[AGRIBALYSE ingrédients v3.2] Sample:")
print(agribalyse_ingredients_df.head())

# ----- BASE CARBONE -----
base_carbon = BaseCarbonExtractor()
base_carbon.fetch_data()
base_carbon_df = base_carbon.normalize()
base_carbon.save_to_csv("data/processed")
print("[BASE CARBONE] Sample:")
print(base_carbon_df.head())