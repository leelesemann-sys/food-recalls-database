"""
Fetch FSIS recall data from API and upload to Azure Blob Storage
"""
import os
import requests
import json
import time
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

# Config
FSIS_API_URL = "https://www.fsis.usda.gov/fsis/api/recall/v/1"
STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME", "raw")
CONNECTION_STRING = f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT_NAME};AccountKey={STORAGE_ACCOUNT_KEY};EndpointSuffix=core.windows.net"
OUTPUT_BLOB_NAME = "fsis_recalls.json"

def fetch_fsis_data():
    """Fetch FSIS recall data from API with retry logic"""
    print("Fetching FSIS recall data from API...")
    print(f"Endpoint: {FSIS_API_URL}")
    
    # Optimized headers - curl User-Agent works better with FSIS API
    headers = {
        'User-Agent': 'curl/7.88',
        'Accept': 'application/json'
    }
    
    # Retry logic: 3 attempts with increasing backoff
    max_attempts = 3
    timeout = 90  # Increased timeout for slow API
    
    for attempt in range(1, max_attempts + 1):
        try:
            print(f"Attempt {attempt}/{max_attempts}...")
            response = requests.get(FSIS_API_URL, headers=headers, timeout=timeout)
            response.raise_for_status()
            
            data = response.json()
            print(f"✓ Successfully fetched {len(data)} FSIS recall records")
            return data
            
        except requests.exceptions.Timeout:
            print(f"✗ Request timed out (>{timeout} seconds)")
            if attempt < max_attempts:
                wait_time = 5 * attempt
                print(f"  Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print("✗ All retry attempts exhausted")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"✗ Error fetching FSIS data: {e}")
            if attempt < max_attempts:
                wait_time = 5 * attempt
                print(f"  Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print("✗ All retry attempts exhausted")
                return None
                
        except json.JSONDecodeError as e:
            print(f"✗ Error parsing JSON response: {e}")
            return None
    
    return None

def upload_to_azure(data):
    """Upload FSIS data to Azure Blob Storage"""
    print(f"\nUploading to Azure Blob Storage...")
    print(f"Container: {CONTAINER_NAME}")
    print(f"Blob: {OUTPUT_BLOB_NAME}")
    
    try:
        # Connect to Azure
        blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
        blob_client = container_client.get_blob_client(OUTPUT_BLOB_NAME)
        
        # Convert to JSON string
        json_data = json.dumps(data, indent=2)
        
        # Upload
        blob_client.upload_blob(json_data, overwrite=True)
        print(f"✓ Successfully uploaded {len(json_data)} bytes to Azure")
        return True
        
    except Exception as e:
        print(f"✗ Error uploading to Azure: {e}")
        return False

def main():
    print("=" * 60)
    print("FSIS Recall Data Fetcher")
    print("=" * 60)
    
    # Step 1: Fetch data from API
    data = fetch_fsis_data()
    if not data:
        print("\n✗ Failed to fetch FSIS data")
        return 1
    
    # Step 2: Upload to Azure
    success = upload_to_azure(data)
    if not success:
        print("\n✗ Failed to upload to Azure")
        return 1
    
    print("\n" + "=" * 60)
    print("✓ SUCCESS! FSIS data is now available in Azure Blob Storage")
    print("=" * 60)
    return 0

if __name__ == "__main__":
    exit(main())
