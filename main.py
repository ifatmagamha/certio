from sources.ademe import AdemeSource
from sources.agribalyse import AgribalyseExtractor
import pyarrow.parquet as pq


#from sources.cbam_api import CbamApiSource
#from sources.climatiq import ClimatiqSource
#from sources.cbam_api import CbamApiSource
from base.emission_source import EmissionSource

ademe = AdemeSource("data/raw/ademe.csv")
agribalyse = AgribalyseExtractor("data\\raw\\agribalyse-31-synthese.csv")
#cbam = CbamApiSource()
#climatiq = ClimatiqSource(api_url="https://beta3.api.climatiq.io/data/cbam", api_key="your_api_key")

# Fetch & normalize
sources = [agribalyse]
for s in sources:
    s.fetch_data()
    df= s.normalize()
    s.save_to_parquet("data/processed")
    print(df.head())

table = pq.read_table("data\\processed\\agribalyse_normalized.parquet")
print(table.schema)
