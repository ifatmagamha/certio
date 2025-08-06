import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Load datasets
df_synth = pd.read_csv(os.getenv("SYNTH_PATH_CSV"))
df_ing = pd.read_csv(os.getenv("ING_PATH_CSV"))
df_etape = pd.read_csv(os.getenv("STEP_PATH_CSV"))

df_synth["code"] = df_synth["code"].astype(str)
df_ing["code"] = df_ing["code"].astype(str)
df_etape["code"] = df_etape["code"].astype(str)

# Drop 'source' column if it exists and remove duplicates on 'code'
df_ing = df_ing.drop(columns=["source"], errors="ignore")
df_etape = df_etape.drop(columns=["source"], errors="ignore")

# Merge all datasets on 'code'
df_merged = df_synth.merge(df_ing, on="code", how="left", suffixes=("", "_ingredient"))
print(f"[INFO] After merging with ingredients: {df_merged.shape}")

df_merged = df_merged.merge(df_etape, on="code", how="left", suffixes=("", "_etape"))
print(f"[INFO] After merging with steps: {df_merged.shape}")

# Drop redundant columns
cols_to_remove = [
    "source", "element_ingredient", "element_etape", "product_ingredient",
    "sector_ingredient", "country_ingredient", "unit_ingredient",
    "product_etape", "sector_etape", "country_etape", "unit_etape"
]
df_merged.drop(columns=[c for c in cols_to_remove if c in df_merged.columns], inplace=True)

# Save the cleaned dataset in CSV format
output_path = "data/processed/agribalyse_merged.csv"
df_merged.to_csv(output_path, index=False)
print(f"[INFO] Merged dataset saved at: {output_path}")
