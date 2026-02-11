"""
TASK 4: Data Validation & Testing (Local Version)
===================================================
Validates the Star Schema Parquet files locally before Azure upload.

Validates:
- Record counts
- Date ranges
- Classification distribution
- Data quality metrics
- Referential integrity
"""

import pandas as pd
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ROOT = Path(__file__).parent.parent.parent
PARQUET_DIR = PROJECT_ROOT / "data" / "output" / "parquet"


def load_parquet(name: str) -> pd.DataFrame:
    """Load a Parquet file."""
    filepath = PARQUET_DIR / f"{name}.parquet"
    if not filepath.exists():
        logger.warning(f"File not found: {filepath}")
        return pd.DataFrame()
    return pd.read_parquet(filepath)


def print_section(title: str):
    """Print a section header."""
    logger.info("")
    logger.info("=" * 60)
    logger.info(title)
    logger.info("=" * 60)


def validate_record_counts():
    """Validate record counts per source."""
    print_section("1. RECORD COUNTS BY SOURCE")

    fact_recalls = load_parquet("fact_recalls")
    fact_health = load_parquet("fact_health_impact")

    if not fact_recalls.empty:
        counts = fact_recalls.groupby('Source').size()
        logger.info("Fact Recalls by Source:")
        for source, count in counts.items():
            logger.info(f"  {source}: {count:,}")
        logger.info(f"  Total: {len(fact_recalls):,}")

    if not fact_health.empty:
        logger.info(f"\nFact Health Impact (CDC NORS): {len(fact_health):,}")


def validate_date_ranges():
    """Validate date ranges per source."""
    print_section("2. DATE RANGE VALIDATION")

    fact_recalls = load_parquet("fact_recalls")

    if not fact_recalls.empty and 'RecallDate' in fact_recalls.columns:
        fact_recalls['RecallDate'] = pd.to_datetime(fact_recalls['RecallDate'], errors='coerce')

        for source in fact_recalls['Source'].unique():
            source_df = fact_recalls[fact_recalls['Source'] == source]
            valid_dates = source_df['RecallDate'].dropna()

            if len(valid_dates) > 0:
                logger.info(f"{source}:")
                logger.info(f"  Earliest: {valid_dates.min()}")
                logger.info(f"  Latest:   {valid_dates.max()}")
                logger.info(f"  Valid dates: {len(valid_dates):,} / {len(source_df):,}")


def validate_classifications():
    """Validate classification distribution."""
    print_section("3. CLASSIFICATION DISTRIBUTION")

    fact_recalls = load_parquet("fact_recalls")
    dim_class = load_parquet("dim_classification")

    if fact_recalls.empty or dim_class.empty:
        return

    merged = fact_recalls.merge(dim_class, on='ClassificationKey', how='left')

    for source in merged['Source_x'].unique():
        source_df = merged[merged['Source_x'] == source]
        class_counts = source_df.groupby(['USAClassLevel', 'SeverityLevel']).size()

        logger.info(f"\n{source} Classification Distribution:")
        for (class_level, severity), count in class_counts.items():
            pct = 100 * count / len(source_df)
            logger.info(f"  {class_level} ({severity}): {count:,} ({pct:.1f}%)")


def validate_geography():
    """Validate geographic distribution."""
    print_section("4. TOP STATES BY RECALL COUNT")

    fact_recalls = load_parquet("fact_recalls")
    dim_geo = load_parquet("dim_geography")

    if fact_recalls.empty or dim_geo.empty:
        return

    merged = fact_recalls.merge(dim_geo, on='GeographyKey', how='left')
    state_counts = merged[merged['State'].notna() & (merged['State'] != 'None')]\
        .groupby('State').size().sort_values(ascending=False).head(15)

    logger.info("Top 15 States:")
    for state, count in state_counts.items():
        logger.info(f"  {state}: {count:,}")


def validate_yearly_distribution():
    """Validate yearly distribution."""
    print_section("5. YEARLY DISTRIBUTION")

    fact_recalls = load_parquet("fact_recalls")
    dim_date = load_parquet("dim_date")

    if fact_recalls.empty or dim_date.empty:
        return

    merged = fact_recalls.merge(dim_date, on='DateKey', how='left')

    yearly = merged.groupby(['Year', 'Source']).size().unstack(fill_value=0)
    logger.info("\nRecalls by Year and Source:")
    logger.info(yearly.to_string())


def validate_product_categories():
    """Validate product category distribution."""
    print_section("6. TOP PRODUCT CATEGORIES")

    fact_recalls = load_parquet("fact_recalls")
    dim_product = load_parquet("dim_product")

    if fact_recalls.empty or dim_product.empty:
        return

    merged = fact_recalls.merge(dim_product, on='ProductKey', how='left')
    cat_counts = merged[merged['ProductCategory'].notna() & (merged['ProductCategory'] != 'None')]\
        .groupby('ProductCategory').size().sort_values(ascending=False).head(15)

    logger.info("Top 15 Product Categories:")
    for cat, count in cat_counts.items():
        pct = 100 * count / len(fact_recalls)
        logger.info(f"  {cat}: {count:,} ({pct:.1f}%)")


def validate_cdc_health_impact():
    """Validate CDC health impact data."""
    print_section("7. CDC NORS HEALTH IMPACT SUMMARY")

    fact_health = load_parquet("fact_health_impact")

    if fact_health.empty:
        logger.warning("No CDC health impact data found")
        return

    yearly = fact_health.groupby('Year').agg({
        'HealthImpactKey': 'count',
        'Illnesses': 'sum',
        'Hospitalizations': 'sum',
        'Deaths': 'sum'
    }).rename(columns={'HealthImpactKey': 'Outbreaks'})

    logger.info("Summary by Year:")
    logger.info(yearly.to_string())

    logger.info(f"\nTotals:")
    logger.info(f"  Total Outbreaks: {len(fact_health):,}")
    logger.info(f"  Total Illnesses: {fact_health['Illnesses'].sum():,}")
    logger.info(f"  Total Hospitalizations: {fact_health['Hospitalizations'].sum():,}")
    logger.info(f"  Total Deaths: {fact_health['Deaths'].sum():,}")


def validate_data_quality():
    """Run data quality checks."""
    print_section("8. DATA QUALITY CHECKS")

    fact_recalls = load_parquet("fact_recalls")

    if fact_recalls.empty:
        return

    logger.info("NULL Value Analysis:")
    for col in ['RecallID', 'RecallDate', 'Source', 'GeographyKey', 'ClassificationKey']:
        null_count = fact_recalls[col].isna().sum()
        none_count = (fact_recalls[col] == 'None').sum() if fact_recalls[col].dtype == 'object' else 0
        total_missing = null_count + none_count
        pct = 100 * total_missing / len(fact_recalls)
        status = "OK" if pct < 5 else "WARNING"
        logger.info(f"  {col}: {total_missing:,} missing ({pct:.1f}%) [{status}]")

    # Check for orphan keys
    dim_geo = load_parquet("dim_geography")
    dim_class = load_parquet("dim_classification")

    if not dim_geo.empty:
        geo_keys = set(dim_geo['GeographyKey'])
        orphan_geo = fact_recalls[~fact_recalls['GeographyKey'].isin(geo_keys)]
        logger.info(f"\nOrphan GeographyKeys: {len(orphan_geo)}")

    if not dim_class.empty:
        class_keys = set(dim_class['ClassificationKey'])
        orphan_class = fact_recalls[~fact_recalls['ClassificationKey'].isin(class_keys)]
        logger.info(f"Orphan ClassificationKeys: {len(orphan_class)}")


def generate_summary():
    """Generate final summary."""
    print_section("VALIDATION SUMMARY")

    tables = ['fact_recalls', 'fact_health_impact', 'dim_date', 'dim_geography',
              'dim_classification', 'dim_product', 'dim_company']

    logger.info("Table Sizes:")
    for table in tables:
        df = load_parquet(table)
        logger.info(f"  {table}: {len(df):,} rows")

    fact_recalls = load_parquet("fact_recalls")
    if not fact_recalls.empty:
        logger.info(f"\nDate Range: {fact_recalls['RecallDate'].min()} to {fact_recalls['RecallDate'].max()}")

    logger.info("\n" + "=" * 60)
    logger.info("TASK 4 COMPLETE: Local Validation Finished")
    logger.info("=" * 60)


def main():
    """Main validation function."""
    logger.info("=" * 60)
    logger.info("TASK 4: DATA VALIDATION & TESTING (Local)")
    logger.info("=" * 60)

    validate_record_counts()
    validate_date_ranges()
    validate_classifications()
    validate_geography()
    validate_yearly_distribution()
    validate_product_categories()
    validate_cdc_health_impact()
    validate_data_quality()
    generate_summary()


if __name__ == "__main__":
    main()
