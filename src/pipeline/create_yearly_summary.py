"""
Create fact_yearly_summary table combining all sources.
- FDA, RASFF, UK_FSA, FSIS (2012-2021) from fact_recalls
- FSIS 2022-2024 from Azure Blob Summary files
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

print("Creating fact_yearly_summary...")
print()

# 1. Load existing fact_recalls
fact_recalls = pd.read_parquet(PROJECT_ROOT / 'data' / 'output' / 'parquet' / 'fact_recalls.parquet')
fact_recalls['Year'] = fact_recalls['DateKey'] // 10000

# Aggregate by Year, Source AND Classification columns (count unique RecallIDs)
yearly_from_recalls = fact_recalls.groupby([
    'Year', 'Source', 'RecallCategory', 'RecallGroup', 'RecallSubgroup'
]).agg(
    RecallCount=('RecallID', 'nunique')
).reset_index()

# Add placeholder for PoundsRecalled (not available for non-FSIS)
yearly_from_recalls['PoundsRecalled'] = None

print(f"Aggregated {len(yearly_from_recalls)} rows from fact_recalls")

# 2. Load FSIS Summary files for 2022-2024
fsis_summaries = []
for year in [2022, 2023, 2024]:
    try:
        blob_client = container.get_blob_client(f'fsis/FSIS-Recall-Summary-{year}.xlsx')
        data = blob_client.download_blob().readall()
        df = pd.read_excel(BytesIO(data), header=None)

        for i, row in df.iterrows():
            val = row[1]
            if pd.notna(val) and isinstance(val, (int, float)) and val > 0:
                fsis_summaries.append({
                    'Year': year,
                    'Source': 'FSIS',
                    'RecallCategory': 'Summary Only',  # No detail for summary years
                    'RecallGroup': 'Summary Only',
                    'RecallSubgroup': 'Summary Only',
                    'RecallCount': int(val),
                    'PoundsRecalled': int(row[2]) if pd.notna(row[2]) and isinstance(row[2], (int, float)) else None
                })
                print(f"  FSIS {year}: {int(val)} recalls")
                break
    except Exception as e:
        print(f"  Warning: Could not load FSIS {year}: {e}")

fsis_df = pd.DataFrame(fsis_summaries)

# 3. Combine - keep FSIS from recalls for 2012-2021, use summary for 2022+
yearly_base = yearly_from_recalls[~((yearly_from_recalls['Source'] == 'FSIS') & (yearly_from_recalls['Year'] >= 2022))]
combined = pd.concat([yearly_base, fsis_df], ignore_index=True)

# 4. Sort and add key
combined = combined.sort_values(['Year', 'Source', 'RecallCategory', 'RecallGroup', 'RecallSubgroup']).reset_index(drop=True)
combined['YearlySummaryKey'] = range(1, len(combined) + 1)

# Reorder columns
combined = combined[['YearlySummaryKey', 'Year', 'Source', 'RecallCategory', 'RecallGroup', 'RecallSubgroup', 'RecallCount', 'PoundsRecalled']]

# Filter to valid years (2012-2025)
combined = combined[(combined['Year'] >= 2012) & (combined['Year'] <= 2025)]

print()
print("=== fact_yearly_summary ===")
print(f"Total rows: {len(combined)}")
print()

# Show summary by Year and Source (aggregated)
year_source_totals = combined.groupby(['Year', 'Source'])['RecallCount'].sum().unstack(fill_value=0)
print("Recalls by Year and Source:")
print(year_source_totals)
print()

# Show unique categories
print(f"Unique RecallCategories: {combined['RecallCategory'].nunique()}")
print(combined['RecallCategory'].value_counts().head(10))
print()

# Save to parquet
output_path = PROJECT_ROOT / 'data' / 'output' / 'parquet' / 'fact_yearly_summary.parquet'
combined.to_parquet(output_path, index=False)
print(f"Saved to {output_path}")
