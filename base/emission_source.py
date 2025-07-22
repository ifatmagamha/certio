from abc import ABC, abstractmethod
import pandas as pd
import os

class EmissionSource(ABC):
    def __init__(self, name):
        self.name = name

    @abstractmethod
    def fetch_data(self):
        """download / read brut data from specific source"""
        pass

    @abstractmethod
    def normalize(self):
        """Transforme to a unified shema : product, sector, country, scope, kgCO2e, unit, source"""
        pass


    def save_to_parquet(self, output_dir: str = "data/processed"):
        """Saved data in .parquet format"""
        if self.normalized_data is None:
            self.normalized_data = self.normalize()
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{self.name.lower()}_normalized.parquet")
        self.normalized_data.to_parquet(output_path, index=False)
        print(f"\n [Save done !]: {output_path}")
