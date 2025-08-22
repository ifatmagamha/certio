import pandas as pd
from pathlib import Path
import json


def filter_csv_by_region(input_csv_path, region_column, region):
    df = pd.read_csv(input_csv_path)
    
    if region_column not in df.columns:
        raise ValueError(f"The '{region_column}' columns not found in the input CSV file.")
    
    filtered_df = df[df[region_column] == region]
    
    output_path = input_csv_path.with_name(input_csv_path.stem + f"_{region}.csv")
    filtered_df.to_csv(output_path, index=False)
    print(f"[INFO] Fltered file saved in : {output_path}")
    
    return output_path


if __name__ == "__main__":
    filtered_file = filter_csv_by_region(
    input_csv_path=Path("data/raw/climatiq_search.csv"),
    region_column="region",
    region="FR"
    )

