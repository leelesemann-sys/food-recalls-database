"""
Create fact_fsis_species table from FSIS species summary data.
"""
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
import pandas as pd
from io import BytesIO
from pathlib import Path

# Setup
PROJECT_ROOT = Path(__file__).parent.parent.parent
load_dotenv(PROJECT_ROOT / '.env')

# Connect to Azure
account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
account_key = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
connection_string = f'DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net'
blob_service = BlobServiceClient.from_connection_string(connection_string)
container = blob_service.get_container_client('raw')

print("Creating fact_fsis_species...")
print()

# Download species file
blob_client = container.get_blob_client('fsis/FSIS-Recall-Summary-species by year.xlsx')
data = blob_client.download_blob().readall()
df = pd.read_excel(BytesIO(data), header=None)

# Parse the data - it has two sections: Number of Recalls and Pounds Recalled
years = [2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]

# Species mapping (row index -> species name)
# Row 3 = Beef, Row 4 = Mixed, etc. (Row 2 is the years header)
species_rows = {
    3: 'Beef',
    4: 'Mixed',
    5: 'Pork/Swine',
    6: 'Poultry/Chicken/Egg Products',
    7: 'Lamb/Sheep/Ovine/Goat',
    8: 'Siluriformes (Fish)',
    9: 'Turkey'
}

# Build the fact table
records = []
key = 1

for year_idx, year in enumerate(years):
    col_idx = year_idx + 1  # Column index (0 is species name, 1 is 2012, etc.)

    for row_idx, species in species_rows.items():
        recall_count = df.iloc[row_idx, col_idx]

        # Pounds row is 10 rows after recalls row (row 3 -> row 13, etc.)
        pounds_row = row_idx + 10
        pounds_recalled = df.iloc[pounds_row, col_idx] if pounds_row < len(df) else None

        # Clean up values
        recall_count = int(recall_count) if pd.notna(recall_count) else 0
        pounds_recalled = int(pounds_recalled) if pd.notna(pounds_recalled) and pounds_recalled > 0 else None

        records.append({
            'FsisSpeciesKey': key,
            'Year': year,
            'Species': species,
            'RecallCount': recall_count,
            'PoundsRecalled': pounds_recalled
        })
        key += 1

# Create DataFrame
fact_fsis_species = pd.DataFrame(records)

print("=== fact_fsis_species ===")
print(f"Total rows: {len(fact_fsis_species)}")
print()

# Summary by Species
print("Recalls by Species (Total 2012-2024):")
species_totals = fact_fsis_species.groupby('Species').agg({
    'RecallCount': 'sum',
    'PoundsRecalled': 'sum'
}).sort_values('RecallCount', ascending=False)
print(species_totals)
print()

# Summary by Year
print("Recalls by Year:")
year_totals = fact_fsis_species.groupby('Year')['RecallCount'].sum()
print(year_totals)
print()

# Save to parquet
output_path = PROJECT_ROOT / 'data' / 'output' / 'parquet' / 'fact_fsis_species.parquet'
fact_fsis_species.to_parquet(output_path, index=False)
print(f"Saved to {output_path}")
