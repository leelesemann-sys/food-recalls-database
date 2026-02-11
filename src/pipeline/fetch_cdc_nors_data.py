"""
CDC NORS (National Outbreak Reporting System) Data Fetcher
============================================================
TASK 1: Fetches outbreak data from CDC NORS API for 2012-2023

API Details:
- Endpoint: https://data.cdc.gov/resource/5xkq-dg7x.json
- Authentication: None required (public API)
- Data: Foodborne disease outbreaks
- Expected Records: ~12,000 (2012-2023)

Output:
- Local: data/input/json/cdc_nors_YYYY-MM-DD.json
- Azure: raw/cdc/cdc_nors_YYYY-MM-DD.json (via ADF or manual upload)
"""

import requests
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
CDC_NORS_API_URL = "https://data.cdc.gov/resource/5xkq-dg7x.json"
OUTPUT_DIR = Path(__file__).parent.parent.parent / "data" / "input" / "json"
MIN_YEAR = 2012
BATCH_SIZE = 50000  # CDC API limit

def fetch_cdc_nors_data() -> pd.DataFrame:
    """
    Fetch CDC NORS outbreak data from the API.

    Returns:
        DataFrame with outbreak records from 2012 onwards
    """
    logger.info(f"Fetching CDC NORS data from {CDC_NORS_API_URL}")

    # Query parameters - filter for years >= 2012
    params = {
        "$where": f"year >= {MIN_YEAR}",
        "$limit": BATCH_SIZE,
        "$order": "year DESC"
    }

    try:
        response = requests.get(CDC_NORS_API_URL, params=params, timeout=120)
        response.raise_for_status()

        data = response.json()
        df = pd.DataFrame(data)

        logger.info(f"Fetched {len(df)} records")

        if len(df) > 0:
            # Log basic stats
            if 'year' in df.columns:
                logger.info(f"Year range: {df['year'].min()} - {df['year'].max()}")
            logger.info(f"Columns: {df.columns.tolist()}")

        return df

    except requests.RequestException as e:
        logger.error(f"API request failed: {e}")
        raise

def save_to_json(df: pd.DataFrame, output_dir: Path) -> Path:
    """
    Save DataFrame to JSON file with timestamp.

    Args:
        df: DataFrame to save
        output_dir: Directory for output file

    Returns:
        Path to saved file
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d")
    filename = f"cdc_nors_{timestamp}.json"
    filepath = output_dir / filename

    # Save as JSON (records format for compatibility)
    df.to_json(filepath, orient='records', indent=2, date_format='iso')

    logger.info(f"Saved {len(df)} records to {filepath}")
    return filepath

def validate_data(df: pd.DataFrame) -> dict:
    """
    Validate the fetched CDC NORS data.

    Args:
        df: DataFrame to validate

    Returns:
        Dictionary with validation results
    """
    validation = {
        "total_records": len(df),
        "columns": df.columns.tolist(),
        "year_range": None,
        "states_count": None,
        "has_illnesses": False,
        "has_hospitalizations": False,
        "has_deaths": False
    }

    if 'year' in df.columns:
        validation["year_range"] = {
            "min": int(df['year'].min()),
            "max": int(df['year'].max())
        }

        # Year distribution
        year_counts = df['year'].value_counts().sort_index()
        validation["records_by_year"] = year_counts.to_dict()

    if 'state' in df.columns:
        validation["states_count"] = df['state'].nunique()

    # Check health impact columns
    validation["has_illnesses"] = 'illnesses' in df.columns
    validation["has_hospitalizations"] = 'hospitalizations' in df.columns
    validation["has_deaths"] = 'deaths' in df.columns

    # Summary stats for health impact
    if validation["has_illnesses"]:
        validation["total_illnesses"] = int(pd.to_numeric(df['illnesses'], errors='coerce').sum())
    if validation["has_hospitalizations"]:
        validation["total_hospitalizations"] = int(pd.to_numeric(df['hospitalizations'], errors='coerce').sum())
    if validation["has_deaths"]:
        validation["total_deaths"] = int(pd.to_numeric(df['deaths'], errors='coerce').sum())

    return validation

def main():
    """Main execution function."""
    logger.info("=" * 60)
    logger.info("CDC NORS Data Fetcher - TASK 1")
    logger.info("=" * 60)

    # Fetch data
    df = fetch_cdc_nors_data()

    if df.empty:
        logger.warning("No data fetched from CDC API")
        return

    # Validate
    validation = validate_data(df)
    logger.info(f"\nValidation Results:")
    logger.info(f"  Total Records: {validation['total_records']}")
    logger.info(f"  Year Range: {validation.get('year_range', 'N/A')}")
    logger.info(f"  States: {validation.get('states_count', 'N/A')}")

    if validation.get('has_illnesses'):
        logger.info(f"  Total Illnesses: {validation.get('total_illnesses', 0):,}")
    if validation.get('has_hospitalizations'):
        logger.info(f"  Total Hospitalizations: {validation.get('total_hospitalizations', 0):,}")
    if validation.get('has_deaths'):
        logger.info(f"  Total Deaths: {validation.get('total_deaths', 0):,}")

    # Save to JSON
    output_path = save_to_json(df, OUTPUT_DIR)

    # Also save validation results
    validation_path = OUTPUT_DIR / "cdc_nors_validation.json"
    with open(validation_path, 'w') as f:
        json.dump(validation, f, indent=2)
    logger.info(f"Validation results saved to {validation_path}")

    logger.info("\n" + "=" * 60)
    logger.info("TASK 1 COMPLETE - CDC NORS data fetched successfully!")
    logger.info(f"Output file: {output_path}")
    logger.info("=" * 60)

    # Print sample columns for mapping reference
    logger.info("\nAvailable columns for Star Schema mapping:")
    for col in sorted(df.columns):
        logger.info(f"  - {col}")

    return df

if __name__ == "__main__":
    main()
