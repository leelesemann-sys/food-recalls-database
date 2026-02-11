"""
Upload Star Schema Parquet files to Azure Data Lake Gold layer.
"""
import os
from pathlib import Path
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

# Config
STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME", "raw")
CONNECTION_STRING = f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT_NAME};AccountKey={STORAGE_ACCOUNT_KEY};EndpointSuffix=core.windows.net"

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
PARQUET_DIR = PROJECT_ROOT / "data" / "output" / "parquet"
GOLD_PREFIX = "gold/"

# Files to upload
PARQUET_FILES = [
    "dim_date.parquet",
    "dim_geography.parquet",
    "dim_classification.parquet",
    "dim_product.parquet",
    "dim_company.parquet",
    "fact_recalls.parquet",
    "fact_health_impact.parquet",
    "fact_yearly_summary.parquet",
    "fact_fsis_species.parquet",
    "fact_adverse_events.parquet"
]

def upload_parquets():
    print("Verbinde mit Azure Storage...")
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)

    print(f"\nUploading Star Schema files to {CONTAINER_NAME}/{GOLD_PREFIX}")
    print("-" * 50)

    for filename in PARQUET_FILES:
        local_path = PARQUET_DIR / filename
        blob_name = f"{GOLD_PREFIX}{filename}"

        if not local_path.exists():
            print(f"SKIP: {filename} - Datei nicht gefunden")
            continue

        print(f"Uploading: {filename}...", end=" ")

        blob_client = container_client.get_blob_client(blob_name)

        with open(local_path, "rb") as f:
            blob_client.upload_blob(f, overwrite=True)

        file_size = local_path.stat().st_size / 1024 / 1024  # MB
        print(f"OK ({file_size:.2f} MB)")

    print("-" * 50)
    print("Upload abgeschlossen!")

if __name__ == "__main__":
    upload_parquets()
