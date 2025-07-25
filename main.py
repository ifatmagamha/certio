from sources.base_carbone import BaseCarbonExtractor
from sources.agribalyse_syn import AgribalyseExtractor
from sources.agribalyse_ing import AgribalyseIngredientsExtractor
from sources.agribalyse_per_steps import AgribalyseStepsExtractor
import pyarrow.parquet as pq
from base.emission_source import EmissionSource
from dotenv import load_dotenv

load_dotenv()

# ----- AGRIBALYSE syntèse-----
agribalyse = AgribalyseExtractor("data/raw/agribalyse-31-synthese.csv")

agribalyse.fetch_data()
agribalyse_df = agribalyse.normalize()
agribalyse.save_to_parquet("data/processed")
print("[AGRIBALYSE] Sample:")
print(agribalyse_df.head())

agribalyse_parquet = pq.read_table("data/processed/agribalyse_normalized.parquet")
print("[AGRIBALYSE] Schema from Parquet:")
print(agribalyse_parquet.schema)

# ----- AGRIBALYSE étapes-----
agribalyse_per_step = AgribalyseStepsExtractor()
agribalyse_per_step.fetch_data()
agribalyse_per_step_df = agribalyse_per_step.normalize()
agribalyse_per_step.save_to_parquet("data/processed")
print("[AGRIBALYSE étapes v3.2] Sample:")
print(agribalyse_per_step_df.head())



# ----- AGRIBALYSE ingrédients-----
agribalyse_ingredients = AgribalyseIngredientsExtractor()
agribalyse_ingredients.fetch_data()
agribalyse_ingredients_df = agribalyse_ingredients.normalize()
agribalyse_ingredients.save_to_parquet("data/processed")
print("[AGRIBALYSE ingrédients v3.2] Sample:")
print(agribalyse_ingredients_df.head())

# ----- BASE CARBONE -----
base_carbon = BaseCarbonExtractor()
base_carbon.fetch_data()
base_carbon_df = base_carbon.normalize()
base_carbon.save_to_parquet("data/processed")
print("[BASE CARBONE] Sample:")
print(base_carbon_df.head())
