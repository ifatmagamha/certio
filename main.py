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
products_df, impacts_df = agribalyse_syn.normalize()

products_df.to_csv("data/processed/agribalyse_syn_normalized.csv", index=False)
impacts_df.to_csv("data/processed/agribalyse_syn_impacts.csv", index=False)
print("[Save done!]")

print("[AGRIBALYSE Products] Sample:")
print(products_df.head())

print("[AGRIBALYSE Environmental Impacts] Sample:")
print(impacts_df.head())

