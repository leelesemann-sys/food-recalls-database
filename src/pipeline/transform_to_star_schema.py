"""
TASK 2: Data Harmonization & Star Schema Transformation
=========================================================
Creates a unified Star Schema from FDA, FSIS, RASFF, UK FSA, and CDC NORS data sources.

Input Sources:
- FDA: data/input/fda-data-usa/food-enforcement-0001-of-0001.json
- FSIS: data/input/fsis-data-usa/FSIS_ALL_YEARS_COMPLETE.xlsx
- RASFF: data/input/rasff-data-europe/RASFF_pre2021.xlsx + RASFF_current.xlsx (EU Food Safety Alerts)
- UK FSA: data/input/json/uk_fsa_alerts_2019-2026.json (UK Food Standards Agency - post-Brexit)
- CDC: data/input/json/cdc_nors_*.json

Output (Gold Layer):
- data/output/parquet/fact_recalls.parquet
- data/output/parquet/fact_health_impact.parquet (CDC outbreaks)
- data/output/parquet/dim_geography.parquet
- data/output/parquet/dim_classification.parquet
- data/output/parquet/dim_product.parquet
- data/output/parquet/dim_company.parquet
- data/output/parquet/dim_date.parquet
"""

import pandas as pd
import numpy as np
import json
import re
from datetime import datetime
from pathlib import Path
import logging
from typing import Tuple, Optional

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ROOT = Path(__file__).parent.parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data" / "output" / "parquet"

# Data source paths
FDA_JSON_PATH = PROJECT_ROOT / "data" / "input" / "fda-data-usa" / "food-enforcement-0001-of-0001.json"
FSIS_EXCEL_PATH = PROJECT_ROOT / "data" / "input" / "fsis-data-usa" / "FSIS_ALL_YEARS_COMPLETE.xlsx"
CDC_JSON_DIR = PROJECT_ROOT / "data" / "input" / "json"
RASFF_DIR = PROJECT_ROOT / "data" / "input" / "rasff-data-europe"
UK_FSA_JSON_PATH = PROJECT_ROOT / "data" / "input" / "json" / "uk_fsa_alerts_2019-2026.json"

# EU Country mappings (for harmonization)
EU_MEMBERS = {
    'Austria', 'Belgium', 'Bulgaria', 'Croatia', 'Cyprus', 'Czech Republic', 'Czechia',
    'Denmark', 'Estonia', 'Finland', 'France', 'Germany', 'Greece', 'Hungary',
    'Ireland', 'Italy', 'Latvia', 'Lithuania', 'Luxembourg', 'Malta', 'Netherlands',
    'Poland', 'Portugal', 'Romania', 'Slovakia', 'Slovenia', 'Spain', 'Sweden'
}
EFTA_COUNTRIES = {'Switzerland', 'Norway', 'Iceland', 'Liechtenstein'}

# Country name harmonization
COUNTRY_NAME_MAP = {
    'AUSTRIA': 'Austria', 'BELGIUM': 'Belgium', 'BULGARIA': 'Bulgaria',
    'CROATIA': 'Croatia', 'CYPRUS': 'Cyprus', 'CZECH REPUBLIC': 'Czech Republic',
    'CZECHIA': 'Czech Republic', 'DENMARK': 'Denmark', 'ESTONIA': 'Estonia',
    'FINLAND': 'Finland', 'FRANCE': 'France', 'GERMANY': 'Germany',
    'GREECE': 'Greece', 'HUNGARY': 'Hungary', 'IRELAND': 'Ireland',
    'ITALY': 'Italy', 'LATVIA': 'Latvia', 'LITHUANIA': 'Lithuania',
    'LUXEMBOURG': 'Luxembourg', 'MALTA': 'Malta', 'NETHERLANDS': 'Netherlands',
    'THE NETHERLANDS': 'Netherlands', 'POLAND': 'Poland', 'PORTUGAL': 'Portugal',
    'ROMANIA': 'Romania', 'SLOVAKIA': 'Slovakia', 'SLOVENIA': 'Slovenia',
    'SPAIN': 'Spain', 'SWEDEN': 'Sweden', 'SWITZERLAND': 'Switzerland',
    'NORWAY': 'Norway', 'ICELAND': 'Iceland', 'TURKEY': 'Türkiye',
    'TÜRKİYE': 'Türkiye', 'TURKIYE': 'Türkiye', 'UNITED KINGDOM': 'United Kingdom',
    'CHINA': 'China', 'INDIA': 'India', 'BRAZIL': 'Brazil', 'THAILAND': 'Thailand',
    'VIETNAM': 'Vietnam', 'VIET NAM': 'Vietnam', 'INDONESIA': 'Indonesia',
    'EGYPT': 'Egypt', 'MOROCCO': 'Morocco', 'ARGENTINA': 'Argentina',
    'UNITED STATES': 'United States', 'USA': 'United States',
}


def load_fda_data() -> pd.DataFrame:
    """Load FDA enforcement data from JSON."""
    logger.info("Loading FDA data...")

    with open(FDA_JSON_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if 'results' in data:
        df = pd.DataFrame(data['results'])
    else:
        df = pd.DataFrame(data)

    logger.info(f"FDA: Loaded {len(df)} records")
    return df


def load_fsis_data() -> pd.DataFrame:
    """Load FSIS recall data from Excel."""
    logger.info("Loading FSIS data...")

    df = pd.read_excel(FSIS_EXCEL_PATH)
    logger.info(f"FSIS: Loaded {len(df)} records")
    return df


def load_cdc_data() -> pd.DataFrame:
    """Load CDC NORS outbreak data from JSON."""
    logger.info("Loading CDC NORS data...")

    cdc_files = list(CDC_JSON_DIR.glob("cdc_nors_*.json"))
    # Exclude validation files
    cdc_files = [f for f in cdc_files if 'validation' not in f.name]

    if not cdc_files:
        logger.warning("No CDC NORS files found!")
        return pd.DataFrame()

    # Use most recent file
    cdc_file = sorted(cdc_files)[-1]

    # Load JSON directly then convert to DataFrame
    with open(cdc_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    # Filter to Food-related outbreaks only (exclude Person-to-Person, Water, Animal contact, etc.)
    total_before = len(df)
    if 'primary_mode' in df.columns:
        df = df[df['primary_mode'] == 'Food']
        logger.info(f"CDC: Filtered to Food-related only: {len(df)} of {total_before} records")

    logger.info(f"CDC: Loaded {len(df)} records from {cdc_file.name}")
    return df


def harmonize_country_name(country: str) -> str:
    """Harmonize country names (uppercase to title case, fix variations)."""
    if pd.isna(country) or not country:
        return None
    country_str = str(country).strip()
    # Check mapping first
    if country_str.upper() in COUNTRY_NAME_MAP:
        return COUNTRY_NAME_MAP[country_str.upper()]
    # If already title case, return as-is
    if country_str[0].isupper() and not country_str.isupper():
        return country_str
    # Convert to title case
    return country_str.title()


def load_rasff_data() -> pd.DataFrame:
    """Load RASFF data from Excel files (Historical + Current)."""
    logger.info("Loading RASFF data...")

    rasff_pre2021_path = RASFF_DIR / "RASFF_pre2021.xlsx"
    rasff_current_path = RASFF_DIR / "RASFF_current.xlsx"

    dfs = []

    # Load pre-2021 historical data
    if rasff_pre2021_path.exists():
        logger.info("Loading RASFF pre-2021 (historical)...")
        df_hist = pd.read_excel(rasff_pre2021_path)

        # Filter for 2012+ only
        df_hist['Date'] = pd.to_datetime(df_hist['Date'], errors='coerce')
        df_hist = df_hist[df_hist['Date'] >= '2012-01-01']
        df_hist = df_hist[df_hist['Date'] < '2021-01-01']  # Avoid overlap

        # Standardize column names
        df_hist = df_hist.rename(columns={
            'REFERENCE': 'reference',
            'Date': 'date',
            'notifying': 'notifying_country',
            'origin': 'origin',
            'Type': 'type',
            'type2': 'classification',
            'subject': 'subject',
            'product': 'product',
            'product category': 'category',
            'Action taken': 'action_taken',
            'distribution status': 'distribution',
            'hazard category': 'hazard_category',
            'substance/finding': 'substance'
        })

        # Add missing columns
        df_hist['risk_decision'] = None
        df_hist['hazards'] = df_hist.apply(
            lambda r: f"{r.get('substance', '')} - {{{r.get('hazard_category', '')}}}"
            if pd.notna(r.get('substance')) else None,
            axis=1
        )

        logger.info(f"RASFF Historical: {len(df_hist)} records (2012-2020)")
        dfs.append(df_hist)

    # Load current data (2017-2025)
    if rasff_current_path.exists():
        logger.info("Loading RASFF current...")
        df_curr = pd.read_excel(rasff_current_path)

        # Parse date - handle different formats
        df_curr['date'] = pd.to_datetime(df_curr['date'], format='%d-%m-%Y %H:%M:%S', errors='coerce')

        # Filter for 2021+ only (to avoid overlap with historical)
        df_curr = df_curr[df_curr['date'] >= '2021-01-01']

        # Add missing columns for consistency
        df_curr['action_taken'] = None
        df_curr['hazard_category'] = None
        df_curr['substance'] = None
        df_curr['product'] = None  # 2021+ data has no separate product field

        # Parse hazards field: "Listeria monocytogenes - {pathogenic micro-organisms}"
        def parse_hazards(hazards_text):
            if pd.isna(hazards_text):
                return None, None
            pattern = r'(.+?)\s*-\s*\{(.+?)\}'
            match = re.match(pattern, str(hazards_text))
            if match:
                return match.group(1).strip(), match.group(2).strip()
            return str(hazards_text), None

        df_curr[['substance', 'hazard_category']] = df_curr['hazards'].apply(
            lambda x: pd.Series(parse_hazards(x))
        )

        logger.info(f"RASFF Current: {len(df_curr)} records (2021-2025)")
        dfs.append(df_curr)

    if not dfs:
        logger.warning("No RASFF files found!")
        return pd.DataFrame()

    # Combine datasets
    rasff_df = pd.concat(dfs, ignore_index=True)

    # Filter to food only (exclude feed and food contact materials)
    total_before = len(rasff_df)
    rasff_df = rasff_df[rasff_df['type'].str.lower() == 'food']
    logger.info(f"RASFF filtered to food only: {len(rasff_df)} of {total_before} records (excluded feed & food contact materials)")

    # Harmonize country names
    rasff_df['origin'] = rasff_df['origin'].apply(harmonize_country_name)
    rasff_df['notifying_country'] = rasff_df['notifying_country'].apply(harmonize_country_name)

    logger.info(f"RASFF Total: {len(rasff_df)} records")
    return rasff_df


def load_uk_fsa_data() -> pd.DataFrame:
    """Load UK FSA Food Alerts from JSON (post-Brexit UK data)."""
    logger.info("Loading UK FSA data...")

    if not UK_FSA_JSON_PATH.exists():
        logger.warning(f"UK FSA file not found: {UK_FSA_JSON_PATH}")
        return pd.DataFrame()

    with open(UK_FSA_JSON_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)

    items = data.get('items', [])
    if not items:
        logger.warning("No items found in UK FSA JSON")
        return pd.DataFrame()

    # Parse UK FSA records
    records = []
    for item in items:
        # Parse date
        created = item.get('created', '')
        date_val = pd.to_datetime(created, errors='coerce') if created else None

        # Extract alert type from 'type' list
        types = item.get('type', [])
        alert_type = 'Alert'
        for t in types:
            if '/AA' in t:
                alert_type = 'Allergy Alert'
            elif '/PRIN' in t:
                alert_type = 'Product Recall'
            elif '/FAFA' in t:
                alert_type = 'Food Alert For Action'

        # Extract product info from productDetails
        product_details = item.get('productDetails', [])
        product_name = ''
        if product_details:
            product_name = product_details[0].get('productName', '')

        # Extract risk/problem info
        problems = item.get('problem', [])
        risk_statement = ''
        allergens = []
        for prob in problems:
            rs = prob.get('riskStatement', '')
            if rs:
                risk_statement = rs
            for allergen in prob.get('allergen', []):
                allergens.append(allergen.get('label', ''))

        # Extract country (usually UK nations)
        countries = item.get('country', [])
        country_labels = []
        for c in countries:
            label = c.get('label', '')
            # Label can be a list or string
            if isinstance(label, list):
                label = label[0] if label else ''
            if label:
                country_labels.append(label)

        records.append({
            'reference': item.get('notation', ''),
            'date': date_val,
            'title': item.get('title', ''),
            'product_name': product_name if product_name else item.get('shortTitle', ''),
            'alert_type': alert_type,
            'risk_statement': risk_statement,
            'allergens': ', '.join(allergens) if allergens else None,
            'countries': ', '.join(country_labels) if country_labels else 'United Kingdom',
            'url': item.get('alertURL', ''),
            'status': item.get('status', {}).get('label', '') if isinstance(item.get('status', {}).get('label', ''), str) else item.get('status', {}).get('label', [''])[0]
        })

    df = pd.DataFrame(records)
    logger.info(f"UK FSA: Loaded {len(df)} records")

    # Show date range
    if not df.empty and df['date'].notna().any():
        min_date = df['date'].min()
        max_date = df['date'].max()
        logger.info(f"UK FSA date range: {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}")

    return df


def create_dim_date(min_year: int = 2012, max_year: int = 2025) -> pd.DataFrame:
    """Create date dimension table."""
    logger.info(f"Creating dim_date for {min_year}-{max_year}...")

    dates = pd.date_range(
        start=f'{min_year}-01-01',
        end=f'{max_year}-12-31',
        freq='D'
    )

    # FDA Fiscal Year: October-September (FY 2024 = Oct 2023 - Sep 2024)
    fiscal_year = dates.year.where(dates.month < 10, dates.year + 1)

    dim_date = pd.DataFrame({
        'DateKey': dates.strftime('%Y%m%d').astype(int),
        'Date': dates.strftime('%Y-%m-%d'),  # String format for Synapse compatibility
        'Year': dates.year,
        'FiscalYear': fiscal_year.astype(int),  # FDA Fiscal Year (Oct-Sep)
        'Quarter': dates.quarter,
        'FiscalQuarter': ((dates.month - 10) % 12 // 3 + 1),  # FDA FQ: Q1=Oct-Dec, Q2=Jan-Mar, etc.
        'Month': dates.month,
        'MonthName': dates.strftime('%B'),
        'Day': dates.day,
        'DayOfWeek': dates.dayofweek + 1,  # 1=Monday
        'DayName': dates.strftime('%A'),
        'WeekOfYear': dates.isocalendar().week.astype(int)
    })

    logger.info(f"dim_date: {len(dim_date)} rows created")
    return dim_date


def create_dim_geography(fda_df: pd.DataFrame, fsis_df: pd.DataFrame, rasff_df: pd.DataFrame = None, uk_fsa_df: pd.DataFrame = None) -> Tuple[pd.DataFrame, dict]:
    """
    Create geography dimension table and mapping.

    Geography Logic:
    - FDA: Country = USA, State = US state (from 'state' field)
    - FSIS: Country = USA, State = NULL (no state info available)
    - RASFF: Country = notifying_country (where recall was reported), State = NULL
    - UK_FSA: Country = United Kingdom, State = NULL

    Returns geo_map with keys:
    - "USA|{state}" for US states
    - "USA|" for USA without state
    - "{country}" for other countries (RASFF notifying + origin countries)
    - "UK" for United Kingdom
    """
    logger.info("Creating dim_geography...")

    geographies = []
    geo_key = 1
    geo_map = {}

    # US States from FDA
    if 'state' in fda_df.columns:
        us_states = fda_df['state'].dropna().unique()
        for state in us_states:
            key = f"USA|{state}"
            if key not in geo_map:
                geo_map[key] = geo_key
                geographies.append({
                    'GeographyKey': geo_key,
                    'Country': 'United States',
                    'CountryCode': 'USA',
                    'State': state,
                    'Region': 'USA',
                    'IsEUMember': False,
                    'IsEFTA': False
                })
                geo_key += 1

    # USA default (for records without state, e.g. FSIS)
    if "USA|" not in geo_map:
        geo_map["USA|"] = geo_key
        geographies.append({
            'GeographyKey': geo_key,
            'Country': 'United States',
            'CountryCode': 'USA',
            'State': None,
            'Region': 'USA',
            'IsEUMember': False,
            'IsEFTA': False
        })
        geo_key += 1

    # RASFF Countries - separate notifying_country (recall geography) and origin (product origin)
    if rasff_df is not None and not rasff_df.empty:
        # Collect all unique countries from RASFF (both notifying and origin)
        countries = set()
        if 'notifying_country' in rasff_df.columns:
            countries.update(rasff_df['notifying_country'].dropna().unique())
        if 'origin' in rasff_df.columns:
            # Also add origin countries for OriginGeographyKey lookup
            countries.update(rasff_df['origin'].dropna().unique())

        for country in countries:
            if not country or country == 'None':
                continue
            # Skip entries that look like comma-separated lists (distribution data)
            if ',' in str(country):
                continue

            key = str(country)  # Simple country name as key
            if key not in geo_map:
                is_eu = country in EU_MEMBERS
                is_efta = country in EFTA_COUNTRIES
                # Determine region
                if country == 'United Kingdom':
                    region = 'UK'
                elif is_eu:
                    region = 'EU'
                elif is_efta:
                    region = 'EFTA'
                else:
                    region = 'Other'

                geo_map[key] = geo_key
                geographies.append({
                    'GeographyKey': geo_key,
                    'Country': country,
                    'CountryCode': None,
                    'State': None,
                    'Region': region,
                    'IsEUMember': is_eu,
                    'IsEFTA': is_efta
                })
                geo_key += 1

    # UK FSA (United Kingdom - post Brexit)
    if uk_fsa_df is not None and not uk_fsa_df.empty:
        key = "United Kingdom"
        if key not in geo_map:
            geo_map[key] = geo_key
            geographies.append({
                'GeographyKey': geo_key,
                'Country': 'United Kingdom',
                'CountryCode': 'GBR',
                'State': None,
                'Region': 'UK',
                'IsEUMember': False,  # Post-Brexit
                'IsEFTA': False
            })
            geo_key += 1

    # Add FDA origin countries (country field, mostly USA but some imports)
    if 'country' in fda_df.columns:
        for country in fda_df['country'].dropna().unique():
            country_clean = harmonize_country_name(country)
            if country_clean and country_clean not in geo_map and ',' not in str(country_clean):
                is_eu = country_clean in EU_MEMBERS
                is_efta = country_clean in EFTA_COUNTRIES
                if country_clean == 'United States':
                    continue  # Already have USA entries
                # Determine region
                if country_clean == 'United Kingdom':
                    region = 'UK'
                elif is_eu:
                    region = 'EU'
                elif is_efta:
                    region = 'EFTA'
                else:
                    region = 'Other'

                geo_map[country_clean] = geo_key
                geographies.append({
                    'GeographyKey': geo_key,
                    'Country': country_clean,
                    'CountryCode': None,
                    'State': None,
                    'Region': region,
                    'IsEUMember': is_eu,
                    'IsEFTA': is_efta
                })
                geo_key += 1

    dim_geography = pd.DataFrame(geographies)
    logger.info(f"dim_geography: {len(dim_geography)} rows created")

    return dim_geography, geo_map


def create_dim_classification(fda_df: pd.DataFrame, fsis_df: pd.DataFrame, rasff_df: pd.DataFrame = None, uk_fsa_df: pd.DataFrame = None) -> Tuple[pd.DataFrame, dict]:
    """Create classification dimension table and mapping."""
    logger.info("Creating dim_classification...")

    classifications = []
    class_key = 1
    class_map = {}

    # Severity mapping for USA classifications
    severity_map = {
        'Class I': ('High', 10),
        'Class II': ('Medium', 5),
        'Class III': ('Low', 2)
    }

    # RASFF risk decision to severity mapping
    rasff_severity_map = {
        'serious': ('High', 10),
        'potentially serious': ('High', 8),
        'potential risk': ('Medium', 5),
        'not serious': ('Low', 2),
        'undecided': ('Unknown', 0),
        'not determined': ('Unknown', 0)
    }

    # RASFF notification type to severity mapping (fallback when no risk_decision)
    # Includes both old format (pre-2021: "alert", "border rejection") and
    # new format (2021+: "alert notification", "border rejection notification")
    rasff_notification_severity_map = {
        # New format (2021+)
        'alert notification': ('High', 9),
        'border rejection notification': ('Medium', 6),
        'information notification for attention': ('Medium', 5),
        'information notification for follow-up': ('Medium', 4),
        'non-compliance notification': ('Medium', 5),
        # Old format (pre-2021)
        'alert': ('High', 9),                      # Alert = immediate danger
        'border rejection': ('Medium', 6),         # Rejected at border = significant issue
        'information for attention': ('Medium', 5), # Requires attention
        'information for follow-up': ('Medium', 4), # Needs follow-up
        'information': ('Low', 3),                 # General information
        'news': ('Low', 2),                        # General news
    }

    # FDA Classifications
    if 'classification' in fda_df.columns:
        for cls in fda_df['classification'].dropna().unique():
            key = f"FDA|{cls}"
            if key not in class_map:
                severity_level, severity_score = severity_map.get(cls, ('Unknown', 0))
                class_map[key] = class_key
                classifications.append({
                    'ClassificationKey': class_key,
                    'Source': 'FDA',
                    'OriginalClassification': cls,
                    'USAClassLevel': cls,
                    'NotificationType': None,
                    'RiskDecision': None,
                    'SeverityLevel': severity_level,
                    'SeverityScore': severity_score
                })
                class_key += 1

    # FSIS Classifications
    if 'class' in fsis_df.columns:
        for cls in fsis_df['class'].dropna().unique():
            key = f"FSIS|{cls}"
            if key not in class_map:
                # FSIS uses 1, 2, 3 or 'Class I', 'Class II', 'Class III'
                cls_str = str(cls)
                if cls_str.isdigit():
                    cls_formatted = f"Class {['I', 'II', 'III'][int(cls_str)-1]}" if int(cls_str) <= 3 else cls_str
                else:
                    cls_formatted = cls_str
                severity_level, severity_score = severity_map.get(cls_formatted, ('Unknown', 0))
                class_map[key] = class_key
                classifications.append({
                    'ClassificationKey': class_key,
                    'Source': 'FSIS',
                    'OriginalClassification': cls,
                    'USAClassLevel': cls_formatted,
                    'NotificationType': None,
                    'RiskDecision': None,
                    'SeverityLevel': severity_level,
                    'SeverityScore': severity_score
                })
                class_key += 1

    # RASFF Classifications (based on notification type + risk decision)
    if rasff_df is not None and not rasff_df.empty:
        # Combine classification and risk_decision for unique keys
        rasff_combos = set()
        for _, row in rasff_df[['classification', 'risk_decision']].drop_duplicates().iterrows():
            notif_type = str(row.get('classification', '')) if pd.notna(row.get('classification')) else 'unknown'
            risk = str(row.get('risk_decision', '')) if pd.notna(row.get('risk_decision')) else 'unknown'
            rasff_combos.add((notif_type, risk))

        for notif_type, risk in rasff_combos:
            key = f"RASFF|{notif_type}|{risk}"
            if key not in class_map:
                # Map notification type
                notif_clean = notif_type.lower().replace(' notification', '').strip()

                # Determine severity based on risk_decision
                risk_lower = risk.lower() if risk else 'unknown'
                severity_level, severity_score = rasff_severity_map.get(risk_lower, ('Unknown', 0))

                # If no risk decision or unknown, use notification type for severity (fallback)
                if severity_level == 'Unknown':
                    notif_lower = notif_type.lower().strip()
                    # Try exact match first, then partial matches
                    if notif_lower in rasff_notification_severity_map:
                        severity_level, severity_score = rasff_notification_severity_map[notif_lower]
                    else:
                        # Partial matching for variations
                        for key, value in rasff_notification_severity_map.items():
                            if key in notif_lower or notif_lower in key:
                                severity_level, severity_score = value
                                break

                class_map[key] = class_key
                classifications.append({
                    'ClassificationKey': class_key,
                    'Source': 'RASFF',
                    'OriginalClassification': notif_type,
                    'USAClassLevel': None,  # Not applicable for EU
                    'NotificationType': notif_type,
                    'RiskDecision': risk if risk != 'unknown' else None,
                    'SeverityLevel': severity_level,
                    'SeverityScore': severity_score
                })
                class_key += 1

    # UK FSA Classifications (based on alert type)
    if uk_fsa_df is not None and not uk_fsa_df.empty:
        # UK FSA alert types: Allergy Alert, Product Recall, Food Alert For Action
        uk_severity_map = {
            'Allergy Alert': ('High', 8),           # Allergens can be life-threatening
            'Product Recall': ('High', 9),           # Product recalls are serious
            'Food Alert For Action': ('High', 10),   # Most serious type
            'Alert': ('Medium', 5)                   # Generic alert
        }

        for alert_type in uk_fsa_df['alert_type'].dropna().unique():
            key = f"UK_FSA|{alert_type}"
            if key not in class_map:
                severity_level, severity_score = uk_severity_map.get(alert_type, ('Medium', 5))

                class_map[key] = class_key
                classifications.append({
                    'ClassificationKey': class_key,
                    'Source': 'UK_FSA',
                    'OriginalClassification': alert_type,
                    'USAClassLevel': None,  # Not applicable for UK
                    'NotificationType': alert_type,
                    'RiskDecision': None,
                    'SeverityLevel': severity_level,
                    'SeverityScore': severity_score
                })
                class_key += 1

    dim_classification = pd.DataFrame(classifications)

    # Normalize RASFF NotificationType to new naming convention (post-2020 format)
    notification_type_mapping = {
        'alert': 'alert notification',
        'border rejection': 'border rejection notification',
        'information for attention': 'information notification for attention',
        'information for follow-up': 'information notification for follow-up',
    }
    dim_classification['NotificationType'] = dim_classification['NotificationType'].replace(notification_type_mapping)

    logger.info(f"dim_classification: {len(dim_classification)} rows created")

    return dim_classification, class_map


def create_dim_product(fda_df: pd.DataFrame, fsis_df: pd.DataFrame, rasff_df: pd.DataFrame = None, uk_fsa_df: pd.DataFrame = None) -> Tuple[pd.DataFrame, dict]:
    """Create product dimension table and mapping."""
    logger.info("Creating dim_product...")

    products = []
    product_key = 1
    product_map = {}

    # FDA Products - use product_description
    if 'product_description' in fda_df.columns:
        # Group by truncated product description to reduce cardinality
        for desc in fda_df['product_description'].dropna().unique():
            # Use first 200 chars as key
            key = f"FDA|{desc[:200]}"
            if key not in product_map:
                product_map[key] = product_key
                # Try to categorize based on keywords
                desc_lower = desc.lower()
                category = categorize_product(desc_lower)
                products.append({
                    'ProductKey': product_key,
                    'ProductName': desc[:500],
                    'ProductCategory': category,
                    'ProductType': get_product_type(category)
                })
                product_key += 1

    # FSIS Products - use species and product
    if 'product' in fsis_df.columns:
        for _, row in fsis_df[['product', 'species']].drop_duplicates().iterrows():
            product = str(row.get('product', ''))[:200]
            species = str(row.get('species', '')) if pd.notna(row.get('species')) else ''
            key = f"FSIS|{product}"
            if key not in product_map:
                product_map[key] = product_key
                fsis_category = species if species else 'Meat/Poultry'
                products.append({
                    'ProductKey': product_key,
                    'ProductName': product[:500],
                    'ProductCategory': fsis_category,
                    'ProductType': get_product_type(fsis_category)
                })
                product_key += 1

    # RASFF Products - use 'product' field for pre-2021, 'category' for 2021+
    if rasff_df is not None and not rasff_df.empty:
        # Include 'product' column (filled for pre-2021, None for 2021+)
        cols = ['subject', 'category', 'product']
        rasff_products = rasff_df[cols].drop_duplicates()
        for _, row in rasff_products.iterrows():
            subject = str(row.get('subject', ''))[:200] if pd.notna(row.get('subject')) else ''
            if not subject:
                continue
            key = f"RASFF|{subject}"
            if key not in product_map:
                category = str(row.get('category', '')) if pd.notna(row.get('category')) else None
                product_name_raw = row.get('product')

                # Use 'product' if available (pre-2021), otherwise use 'category' (2021+)
                if pd.notna(product_name_raw) and str(product_name_raw).strip():
                    product_name = str(product_name_raw)[:500]
                else:
                    # For 2021+ data, use category as product identifier
                    product_name = category if category else 'Unknown Product'

                product_map[key] = product_key
                products.append({
                    'ProductKey': product_key,
                    'ProductName': product_name,
                    'ProductCategory': category,
                    'ProductType': get_product_type(category)
                })
                product_key += 1

    # UK FSA Products - use product_name
    if uk_fsa_df is not None and not uk_fsa_df.empty:
        for _, row in uk_fsa_df[['product_name']].drop_duplicates().iterrows():
            product_name = str(row.get('product_name', ''))[:200] if pd.notna(row.get('product_name')) else ''
            if not product_name:
                continue
            key = f"UK_FSA|{product_name}"
            if key not in product_map:
                # Try to categorize based on product name
                category = categorize_product(product_name.lower())

                product_map[key] = product_key
                products.append({
                    'ProductKey': product_key,
                    'ProductName': product_name[:500],
                    'ProductCategory': category,
                    'ProductType': get_product_type(category)
                })
                product_key += 1

    dim_product = pd.DataFrame(products)
    logger.info(f"dim_product: {len(dim_product)} rows created")

    return dim_product, product_map


def categorize_product(desc: str) -> str:
    """Categorize product based on description keywords."""
    if any(kw in desc for kw in ['beef', 'steak', 'burger', 'meat', 'pork', 'chicken', 'poultry', 'turkey']):
        return 'Meat/Poultry'
    elif any(kw in desc for kw in ['fish', 'salmon', 'tuna', 'seafood', 'shrimp', 'crab']):
        return 'Fish/Seafood'
    elif any(kw in desc for kw in ['milk', 'cheese', 'dairy', 'yogurt', 'butter', 'cream']):
        return 'Dairy'
    elif any(kw in desc for kw in ['vegetable', 'lettuce', 'spinach', 'tomato', 'salad']):
        return 'Vegetables'
    elif any(kw in desc for kw in ['fruit', 'apple', 'orange', 'berry', 'grape']):
        return 'Fruits'
    elif any(kw in desc for kw in ['nut', 'peanut', 'almond', 'cashew']):
        return 'Nuts/Seeds'
    elif any(kw in desc for kw in ['bread', 'bakery', 'cookie', 'cake', 'pastry']):
        return 'Bakery'
    elif any(kw in desc for kw in ['candy', 'chocolate', 'sweet']):
        return 'Confectionery'
    elif any(kw in desc for kw in ['spice', 'seasoning', 'herb']):
        return 'Spices/Seasonings'
    elif any(kw in desc for kw in ['supplement', 'vitamin', 'dietary']):
        return 'Dietary Supplements'
    else:
        return 'Other'


# ============================================================================
# PRODUCT TYPE MAPPING
# ============================================================================
# Maps ProductCategory to broader ProductType for high-level analysis

PRODUCT_TYPE_MAPPING = {
    # Fresh Produce
    'fruits and vegetables': 'Fresh Produce',
    'Fruits': 'Fresh Produce',
    'Vegetables': 'Fresh Produce',
    # Fresh Protein (Meat/Poultry)
    'poultry meat and poultry meat products': 'Fresh Protein',
    'Meat/Poultry': 'Fresh Protein',
    'meat and meat products (other than poultry)': 'Fresh Protein',
    'poultry': 'Fresh Protein',
    'meat': 'Fresh Protein',
    # Seafood
    'fish and fish products': 'Seafood',
    'Fish/Seafood': 'Seafood',
    'fish': 'Seafood',
    'bivalve molluscs and products thereof': 'Seafood',
    'crustaceans and products thereof': 'Seafood',
    'cephalopods and products thereof': 'Seafood',
    # Dairy
    'Dairy': 'Dairy',
    'milk and milk products': 'Dairy',
    # Bakery/Grains
    'cereals and bakery products': 'Bakery/Grains',
    'Bakery': 'Bakery/Grains',
    'cereals/bakery': 'Bakery/Grains',
    # Nuts/Seeds
    'nuts, nut products and seeds': 'Nuts/Seeds',
    'Nuts/Seeds': 'Nuts/Seeds',
    'nuts/seeds': 'Nuts/Seeds',
    # Ingredients/Spices
    'herbs and spices': 'Ingredients',
    'Spices/Seasonings': 'Ingredients',
    'herbs/spices': 'Ingredients',
    'food additives and flavourings': 'Ingredients',
    # Supplements
    'dietetic foods, food supplements and fortified foods': 'Supplement',
    'dietetic foods, food supplements, fortified foods': 'Supplement',
    'Dietary Supplements': 'Supplement',
    'supplements': 'Supplement',
    # Ready-to-Eat / Prepared Foods
    'prepared dishes and snacks': 'Ready-to-Eat',
    'ices and desserts': 'Ready-to-Eat',
    # Confectionery
    'confectionery': 'Confectionery',
    'Confectionery': 'Confectionery',
    'cocoa and cocoa preparations, coffee and tea': 'Confectionery',
    # Processed Foods
    'soups, broths, sauces and condiments': 'Processed',
    'fats and oils': 'Processed',
    # Beverages
    'alcoholic beverages': 'Beverage',
    'non-alcoholic beverages': 'Beverage',
    'water for human consumption': 'Beverage',
    # Animal Feed
    'feed materials': 'Animal Feed',
    'pet food': 'Animal Feed',
    'compound feeds': 'Animal Feed',
    'feed additives': 'Animal Feed',
    'feed premixtures': 'Animal Feed',
    # Non-Food
    'food contact materials': 'Non-Food',
    'materials and articles intended to come into contact with foodstuffs': 'Non-Food',
}


def get_product_type(category: str) -> str:
    """Map ProductCategory to ProductType."""
    if not category or pd.isna(category):
        return 'Unknown'

    category_lower = str(category).lower().strip()

    # Direct match
    if category_lower in PRODUCT_TYPE_MAPPING:
        return PRODUCT_TYPE_MAPPING[category_lower]

    # Case-insensitive lookup
    for key, value in PRODUCT_TYPE_MAPPING.items():
        if key.lower() == category_lower:
            return value

    # Partial match for common patterns
    if 'meat' in category_lower or 'poultry' in category_lower:
        return 'Fresh Protein'
    if 'fish' in category_lower or 'seafood' in category_lower:
        return 'Seafood'
    if 'dairy' in category_lower or 'milk' in category_lower:
        return 'Dairy'
    if 'vegetable' in category_lower or 'fruit' in category_lower:
        return 'Fresh Produce'
    if 'supplement' in category_lower or 'vitamin' in category_lower:
        return 'Supplement'
    if 'feed' in category_lower or 'pet food' in category_lower:
        return 'Animal Feed'

    return 'Other'


# ============================================================================
# RECALL REASON CLASSIFICATION (based on DeBeer et al. 2024 & Blickem et al. 2025)
# ============================================================================
# Three-level hierarchy for classifying recall reasons:
# Level 1 (RecallCategory): Product Contaminant vs. Process Issue
# Level 2 (RecallGroup): Biological Contamination, Allergens, Chemical Contamination, etc.
# Level 3 (RecallSubgroup): Specific pathogens (Listeria, Salmonella) or allergens (Milk, Peanuts)

# Biological pathogens (Level 3)
PATHOGENS = {
    # Bacteria
    'listeria': 'Listeria monocytogenes',
    'listeria monocytogenes': 'Listeria monocytogenes',
    'listeriosis': 'Listeria monocytogenes',  # disease name
    'l. monocytogenes': 'Listeria monocytogenes',
    'l.monocytogenes': 'Listeria monocytogenes',
    'l. mono': 'Listeria monocytogenes',  # common abbreviation
    'l.mono': 'Listeria monocytogenes',
    'salmonella': 'Salmonella',
    'salmonellosis': 'Salmonella',  # disease name
    's. enteritidis': 'Salmonella',
    's. typhimurium': 'Salmonella',
    'e. coli': 'E. coli',
    'e.coli': 'E. coli',
    'escherichia coli': 'E. coli',
    'coliform': 'Coliforms',  # indicator bacteria for fecal contamination
    'stec': 'E. coli (STEC)',
    'o157': 'E. coli O157:H7',
    'o157:h7': 'E. coli O157:H7',
    'clostridium botulinum': 'Clostridium botulinum',
    'c. botulinum': 'Clostridium botulinum',
    'botulism': 'Clostridium botulinum',
    'botulinum': 'Clostridium botulinum',
    'campylobacter': 'Campylobacter',
    'staphylococcus': 'Staphylococcus aureus',
    's. aureus': 'Staphylococcus aureus',
    'bacillus cereus': 'Bacillus cereus',
    'b. cereus': 'Bacillus cereus',
    'cronobacter': 'Cronobacter',
    'shigella': 'Shigella',
    'vibrio': 'Vibrio',
    'yersinia': 'Yersinia',
    'clostridium perfringens': 'Clostridium perfringens',
    'c. perfringens': 'Clostridium perfringens',
    # Viruses
    'hepatitis a': 'Hepatitis A',
    'hepatitis': 'Hepatitis A',
    'norovirus': 'Norovirus',
    # Parasites
    'cyclospora': 'Cyclospora',
    'cryptosporidium': 'Cryptosporidium',
    'trichinella': 'Trichinella',
    'anisakis': 'Anisakis',
    # Molds/Fungi
    'aflatoxin': 'Aflatoxin (Mold)',
    'mycotoxin': 'Mycotoxin (Mold)',
    'ochratoxin': 'Ochratoxin (Mold)',
    'patulin': 'Patulin (Mold)',
    'mold': 'Mold',
    'mould': 'Mold',
}

# Major allergens (Level 3) - FDA Big 9 + EU allergens
ALLERGENS = {
    # Milk/Dairy
    'milk': 'Milk',
    'dairy': 'Milk',
    'lactose': 'Milk',
    'casein': 'Milk',
    'whey': 'Milk',
    'cream': 'Milk',
    'butter': 'Milk',
    'cheese': 'Milk',
    # Eggs
    'egg': 'Eggs',
    'eggs': 'Eggs',
    'albumin': 'Eggs',
    'ovalbumin': 'Eggs',
    # Wheat/Gluten
    'wheat': 'Wheat',
    'gluten': 'Wheat/Gluten',
    'barley': 'Wheat/Gluten',
    'rye': 'Wheat/Gluten',
    'oats': 'Wheat/Gluten',
    # Peanuts
    'peanut': 'Peanuts',
    'peanut protein': 'Peanuts',
    'peanuts': 'Peanuts',
    # Tree nuts
    'tree nut': 'Tree Nuts',
    'tree nuts': 'Tree Nuts',
    'almond': 'Tree Nuts (Almond)',
    'almonds': 'Tree Nuts (Almond)',
    'walnut': 'Tree Nuts (Walnut)',
    'walnuts': 'Tree Nuts (Walnut)',
    'cashew': 'Tree Nuts (Cashew)',
    'cashews': 'Tree Nuts (Cashew)',
    'pistachio': 'Tree Nuts (Pistachio)',
    'pistachios': 'Tree Nuts (Pistachio)',
    'pecan': 'Tree Nuts (Pecan)',
    'pecans': 'Tree Nuts (Pecan)',
    'hazelnut': 'Tree Nuts (Hazelnut)',
    'hazelnuts': 'Tree Nuts (Hazelnut)',
    'macadamia': 'Tree Nuts (Macadamia)',
    'brazil nut': 'Tree Nuts (Brazil Nut)',
    # Soy
    'soy': 'Soy',
    'soya': 'Soy',
    'soybean': 'Soy',
    'soybeans': 'Soy',
    # Fish
    'fish': 'Fish',
    'anchovy': 'Fish',
    'anchovies': 'Fish',
    'cod': 'Fish',
    'salmon': 'Fish',
    'tuna': 'Fish',
    # Shellfish/Crustaceans
    'shellfish': 'Shellfish',
    'crustacean': 'Shellfish',
    'shrimp': 'Shellfish',
    'crab': 'Shellfish',
    'lobster': 'Shellfish',
    'prawn': 'Shellfish',
    'mollusc': 'Molluscs',
    'mollusk': 'Molluscs',
    'clam': 'Molluscs',
    'mussel': 'Molluscs',
    'oyster': 'Molluscs',
    'squid': 'Molluscs',
    # Sesame (added to FDA Big 9 in 2023)
    'sesame': 'Sesame',
    # Other EU allergens
    'celery': 'Celery',
    'mustard': 'Mustard',
    'lupin': 'Lupin',
    'sulphite': 'Sulphites',
    'sulfite': 'Sulphites',
    'sulphur dioxide': 'Sulphites',
    # RASFF-specific allergen terms
    'lactoprotein': 'Milk',
    '(allergens)': 'Allergens - Other',  # RASFF generic allergen
    'nuts (allergens)': 'Tree Nuts',
}

# Chemical contaminants (Level 3)
CHEMICALS = {
    'lead': 'Lead',
    'mercury': 'Mercury',
    'cadmium': 'Cadmium',
    'arsenic': 'Arsenic',
    'pesticide': 'Pesticides',
    'herbicide': 'Pesticides',
    'insecticide': 'Pesticides',
    'chlorpyrifos': 'Pesticides',
    'dieldrin': 'Pesticides',
    'glyphosate': 'Pesticides',  # herbicide residue
    'melamine': 'Melamine',
    'ethylene oxide': 'Ethylene Oxide',
    'dioxin': 'Dioxins',
    'pcb': 'PCBs',
    'polychlorinated': 'PCBs',
    'pah': 'PAHs',
    'benzo[a]pyrene': 'PAHs',
    'benzo(a)pyrene': 'PAHs',  # RASFF format
    'polycyclic aromatic': 'PAHs',
    'acrylamide': 'Acrylamide',
    'benzene': 'Benzene',
    'veterinary drug': 'Veterinary Drugs',
    'veterinary medicinal': 'Veterinary Drugs',  # RASFF format
    'antibiotic': 'Antibiotics',
    'chloramphenicol': 'Antibiotics',
    'nitrofuran': 'Antibiotics',
    'beta lactam': 'Antibiotics',  # beta-lactam antibiotic residues
    'beta-lactam': 'Antibiotics',
    'leucomalachite': 'Veterinary Drugs',  # RASFF: leucomalachite green
    'malachite green': 'Veterinary Drugs',
    'clenbuterol': 'Veterinary Drugs',
    'histamine': 'Histamine',
    'scombroid': 'Histamine',
    # RASFF-specific: Migration from packaging
    '(migration)': 'Migration (Packaging)',
    'migration': 'Migration (Packaging)',
    'phthalate': 'Phthalates (Migration)',
    'dinch': 'DINCH (Migration)',
    'esbo': 'ESBO (Migration)',
    'epoxidised soybean oil': 'ESBO (Migration)',
    'dotp': 'DOTP (Migration)',
    'primary aromatic amines': 'Aromatic Amines (Migration)',
    # RASFF-specific: Environmental pollutants
    'environmental pollutant': 'Environmental Pollutants',
    '(environmental pollutants)': 'Environmental Pollutants',
    # RASFF-specific: Natural toxins
    'pyrrolizidine': 'Pyrrolizidine Alkaloids',
    'alkaloid': 'Plant Alkaloids',
    'natural toxin': 'Natural Toxins',
    '(natural toxins)': 'Natural Toxins',
    'tropane': 'Tropane Alkaloids',
    'cyanide': 'Cyanide',
    'glycoalkaloid': 'Glycoalkaloids',
    'solanine': 'Glycoalkaloids',
    # RASFF-specific: Pharma/drugs in food
    'sildenafil': 'Undeclared Drugs',
    'tadalafil': 'Undeclared Drugs',
    'anabolic steroid': 'Undeclared Drugs',
    'steroid': 'Undeclared Drugs',
    'picamilon': 'Undeclared Drugs',  # unapproved nootropic
    'hidden drug': 'Undeclared Drugs',
    'drug ingredient': 'Undeclared Drugs',
    'unapproved ingredient': 'Unauthorised Substances',
    # Note: generic 'unapproved' removed - too ambiguous (could be FDA product approval vs ingredient)
    'unauthorised substance': 'Unauthorised Substances',
    'unauthorised': 'Unauthorised Substances',
    'cyclamate': 'Unauthorised Substances',  # banned artificial sweetener (US)
    'kratom': 'Unauthorised Substances',  # FDA-unapproved psychoactive plant
    'dmha': 'Unauthorised Substances',  # unapproved stimulant in supplements
    'dmaa': 'Unauthorised Substances',  # 1,4-dimethylamylamine - banned stimulant
    'dimethylamylamine': 'Unauthorised Substances',
    'hordenine': 'Unauthorised Substances',  # unapproved stimulant
    # Toxic plants
    'oleander': 'Toxic Plants',
    'toxic': 'Toxic Substances',
    'poisonous': 'Toxic Substances',
    # Cleaning chemicals
    'cleaning solution': 'Cleaning Chemicals',
    'cleaning agent': 'Cleaning Chemicals',
    # RASFF-specific: Industrial contaminants
    '3-mcpd': '3-MCPD',
    'monochlor': '3-MCPD',
    'glycidyl': 'Glycidyl Esters',
    '(industrial contaminants)': 'Industrial Contaminants',
    '(process contaminants)': 'Process Contaminants',
    # RASFF-specific: THC/CBD
    'thc': 'THC (Cannabis)',
    'tetrahydrocanabinol': 'THC (Cannabis)',
    'cannabidiol': 'CBD (Cannabis)',
    'cbd': 'CBD (Cannabis)',
    # Food Additives Issues
    'tbhq': 'Food Additives Issues',
    'tert-butylhydroquinone': 'Food Additives Issues',
    'dmps': 'Food Additives Issues',
    'dimethyl polysiloxane': 'Food Additives Issues',
    'excessive amount': 'Food Additives Issues',
}

# Foreign objects (Level 3)
FOREIGN_OBJECTS = {
    'metal': 'Metal Fragments',
    'wire': 'Metal Fragments',  # flexible wire, wire mesh, wire fragment
    'glass': 'Glass Fragments',
    'plastic': 'Plastic Pieces',
    'polyethylene': 'Plastic Pieces',  # HDPE, LDPE packaging components
    'wood': 'Wood Pieces',
    'stone': 'Stones',
    'rubber': 'Rubber Pieces',
    'cloth': 'Cloth/Fabric',  # cloth material from production equipment
    'bone': 'Bone Fragments',
    'insect': 'Insects',
    'rodent': 'Rodent Contamination',
    'pest': 'Pest Contamination',
    'hair': 'Hair/Foreign Matter',
    'human fingertip': 'Human Body Parts',
    'extraneous': 'Foreign Matter',
    'foreign material': 'Foreign Matter',
    'foreign matter': 'Foreign Matter',
    'foreign object': 'Foreign Matter',
    'foreign body': 'Foreign Matter',
    'foreign bodies': 'Foreign Matter',  # RASFF format
    'fragments': 'Fragments',  # RASFF: fragments (foreign bodies)
    'physical hazard': 'Physical Hazard',
    'physical contaminant': 'Foreign Matter',  # physical contaminants = foreign objects
}

# Process issues keywords (Level 2 = Process Issue)
PROCESS_ISSUE_KEYWORDS = {
    # cGMP Issues (facility/sanitary conditions)
    'cgmp': 'cGMP Issues',
    'good manufacturing': 'cGMP Issues',
    'manufacturing practice': 'cGMP Issues',
    'under gmp': 'cGMP Issues',  # "not manufactured under GMP's"
    'sanitation': 'cGMP Issues',
    'sanitary': 'cGMP Issues',
    'sanitizer': 'cGMP Issues',  # inadequate sanitizer in wash water
    'hygienic': 'cGMP Issues',
    'infestation of mice': 'cGMP Issues',  # facility infestation = sanitary issue
    'insanitary': 'cGMP Issues',  # unsanitary facility conditions
    'unsanitary': 'cGMP Issues',  # unsanitary facility conditions
    # HACCP Issues
    'haccp': 'HACCP Issues',
    'critical control': 'HACCP Issues',
    # Manufacturing Issues
    'manufacturing defect': 'Manufacturing Issues',
    'production error': 'Manufacturing Issues',
    'process deviation': 'Manufacturing Issues',  # FDA term for process failure
    'equipment failure': 'Manufacturing Issues',
    'cross-contact': 'Manufacturing Issues',
    'cross contact': 'Manufacturing Issues',
    'pasteurization': 'Manufacturing Issues',  # inadequate pasteurization
    'pasteurisation': 'Manufacturing Issues',  # UK spelling
    # Mislabeling/Misbranding
    'mislabel': 'Mislabeling',
    'misbranding': 'Mislabeling',
    'misbrand': 'Mislabeling',
    'incorrect label': 'Mislabeling',
    'wrong label': 'Mislabeling',
    'labeling error': 'Mislabeling',
    'label error': 'Mislabeling',
    'packaging error': 'Mislabeling',
    'wrong package': 'Mislabeling',
    'does not contain a listing': 'Mislabeling',  # missing ingredient list
    'fails to list': 'Mislabeling',  # label fails to list ingredients
    'labels lack': 'Mislabeling',  # product labels lack the statement
    'labeled in english': 'Mislabeling',  # not labeled in English
    # Regulatory Issues
    'not fda approved': 'Regulatory Issues',  # product not FDA approved
    # Refrigeration Issues
    'temperature abuse': 'Refrigeration Issues',
    'cold chain': 'Refrigeration Issues',
    'refrigeration': 'Refrigeration Issues',
    'temperature control': 'Refrigeration Issues',
    'keep refrigerated': 'Refrigeration Issues',  # FDA: missing refrigeration statement
    'not held at an appropriate temperature': 'Refrigeration Issues',  # FDA common phrase
    'holding temperature': 'Refrigeration Issues',  # FDA temperature holding
    'cooler': 'Refrigeration Issues',  # Cooler malfunction (matches coolers too)
    # Under-Processing
    'underprocess': 'Under-Processing',
    'under-process': 'Under-Processing',
    'undercook': 'Under-Processing',
    'under-cook': 'Under-Processing',
    'insufficient processing': 'Under-Processing',
    'inadequate processing': 'Under-Processing',
    'inadequate heat': 'Under-Processing',
    'low acid': 'Under-Processing',
    'swollen': 'Under-Processing',  # swollen containers = gas-producing bacteria from inadequate processing
    'bloated': 'Under-Processing',  # bloated containers = inadequate thermal treatment
    # RASFF-specific: Packaging Issues
    'packaging defective': 'Packaging Issues',
    'packaging incorrect': 'Packaging Issues',
    'packaging concern': 'Packaging Issues',  # FDA: packaging concerns
    'air space': 'Packaging Issues',  # FDA: air space specifications
    '(packaging': 'Packaging Issues',
    # RASFF-specific: Composition Issues
    '(composition)': 'Composition Issues',
    'composition': 'Composition Issues',
    'vitamin d': 'Composition Issues',  # Common RASFF composition issue
    # RASFF-specific: GMO
    'genetically modified': 'GMO Issues',
    # RASFF-specific: Novel Food
    'novel food': 'Novel Food Issues',
    '(novel food)': 'Novel Food Issues',
    # RASFF-specific: Foodborne outbreak
    'foodborne outbreak': 'Foodborne Outbreak',
    # RASFF-specific: Labelling issues
    'labelling (labelling': 'Mislabeling',  # consolidated from Labelling Issues
    'labelling absent': 'Mislabeling',  # consolidated from Labelling Issues
    'labelling incomplete': 'Mislabeling',  # consolidated from Labelling Issues
    'labelling incorrect': 'Mislabeling',  # consolidated from Labelling Issues
    'expiry date': 'Mislabeling',  # consolidated from Labelling Issues
    # RASFF-specific: Thermal processing
    'thermal processing': 'Under-Processing',
    'poor or insufficient controls': 'Manufacturing Issues',
    # FDA-specific: Processing/Inspection
    'processing defect': 'Manufacturing Issues',
    'without inspection': 'Regulatory Issues',
    'import violation': 'Regulatory Issues',
    # RASFF-specific: Physical hazards (product form, not foreign objects)
    'suffocation': 'Physical Hazard',
    'choking': 'Physical Hazard',
    'mouth injury': 'Physical Hazard',
    # RASFF-specific: Sensory/Quality
    'organoleptic': 'Quality Issues',
    'acidity': 'Quality Issues',
    'off-odour': 'Quality Issues',
    'off-flavour': 'Quality Issues',
    'spoilage': 'Quality Issues',
    # Generic catch-all
    'food poisoning': 'Foodborne Illness',
    'allergic reaction': 'Allergic Reaction',
}

# Additional RASFF categories that map to Product Contaminant
RASFF_PRODUCT_CONTAMINANTS = {
    # These RASFF hazard categories should be Product Contaminants
    'residues of veterinary': 'Veterinary Drug Residues',
    '(residues of veterinary': 'Veterinary Drug Residues',
    'food additives': 'Food Additives Issues',
    '(food additives': 'Food Additives Issues',
    'flavouring': 'Food Additives Issues',
    'rhodamine': 'Unauthorised Colors',
}


def classify_recall_reason(reason_text: str) -> Tuple[str, str, str]:
    """
    Classify recall reason into 3-level hierarchy based on DeBeer et al. 2024.

    Returns:
        Tuple of (RecallCategory, RecallGroup, RecallSubgroup)
        - RecallCategory: 'Product Contaminant' or 'Process Issue'
        - RecallGroup: 'Biological Contamination', 'Allergens', 'Chemical Contamination', etc.
        - RecallSubgroup: Specific pathogen/allergen/chemical or None
    """
    if not reason_text or pd.isna(reason_text):
        return ('Other', 'Other', None)

    text_lower = str(reason_text).lower()

    # Check for biological contamination (pathogens)
    for keyword, pathogen in PATHOGENS.items():
        if keyword in text_lower:
            return ('Product Contaminant', 'Biological Contamination', pathogen)

    # Check for allergens - "undeclared" is key indicator
    # But also check for allergen keywords without undeclared (still allergen issue)
    is_undeclared = ('undeclared' in text_lower or 'not declared' in text_lower or
                     'may contain' in text_lower or 'same equipment' in text_lower or
                     'shared equipment' in text_lower or 'presence of' in text_lower or
                     'tested positive' in text_lower or 'detected' in text_lower)  # e.g. "peanuts detected"
    is_labeling_issue = ('does not declare' in text_lower or 'do not declare' in text_lower or
                         'not list' in text_lower or 'without an ingredient' in text_lower or
                         'absence of' in text_lower or 'did not list' in text_lower or
                         'not on the label' in text_lower or 'missing' in text_lower)
    for keyword, allergen in ALLERGENS.items():
        if keyword in text_lower:
            # Allergens are Product Contaminants when undeclared or labeling issue
            if is_undeclared or is_labeling_issue or 'label' in text_lower:
                return ('Product Contaminant', 'Allergens', allergen)
            elif 'allerg' in text_lower:
                return ('Product Contaminant', 'Allergens', allergen)

    # Check for chemical contamination (includes RASFF-specific categories)
    for keyword, chemical in CHEMICALS.items():
        if keyword in text_lower:
            return ('Product Contaminant', 'Chemical Contamination', chemical)

    # Check for RASFF-specific product contaminants
    for keyword, contaminant in RASFF_PRODUCT_CONTAMINANTS.items():
        if keyword in text_lower:
            return ('Product Contaminant', 'Chemical Contamination', contaminant)

    # Check for foreign objects
    for keyword, obj in FOREIGN_OBJECTS.items():
        if keyword in text_lower:
            return ('Product Contaminant', 'Foreign Objects', obj)

    # Check for RASFF allergen patterns like "(allergens)" or "nuts (allergens)"
    if '(allergens)' in text_lower:
        # Try to extract specific allergen before the (allergens) tag
        if 'nuts' in text_lower:
            return ('Product Contaminant', 'Allergens', 'Tree Nuts')
        elif 'lactoprotein' in text_lower or 'milk' in text_lower:
            return ('Product Contaminant', 'Allergens', 'Milk')
        else:
            return ('Product Contaminant', 'Allergens', 'Allergens - Other')

    # Check for undeclared food colors
    if 'undeclared' in text_lower and ('color' in text_lower or 'colour' in text_lower or 'dye' in text_lower):
        return ('Product Contaminant', 'Undeclared Food Colors', 'Undeclared Food Colors - Other')
    if 'fd&c' in text_lower or 'artificial color' in text_lower:
        return ('Product Contaminant', 'Undeclared Food Colors', 'Undeclared Food Colors - Other')

    # Check for process issues (includes RASFF-specific categories)
    for keyword, issue_type in PROCESS_ISSUE_KEYWORDS.items():
        if keyword in text_lower:
            return ('Process Issue', issue_type, f'{issue_type} - Other')

    # If "undeclared" or labeling issue without specific allergen found, likely allergen issue
    if is_undeclared or is_labeling_issue:
        return ('Product Contaminant', 'Allergens', 'Allergens - Other')

    # Generic pathogen keywords
    if any(kw in text_lower for kw in ['pathogen', 'bacteria', 'microbial', 'microorganism', 'contamination', 'contaminated']):
        return ('Product Contaminant', 'Biological Contamination', 'Biological Contamination - Other')

    # Default: Other (we know something about it, just not classified)
    return ('Other', 'Other', 'Other')


def create_dim_company(fda_df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    """Create company dimension table and mapping."""
    logger.info("Creating dim_company...")

    companies = []
    company_key = 1
    company_map = {}

    # FDA Companies
    if 'recalling_firm' in fda_df.columns:
        for _, row in fda_df[['recalling_firm', 'city', 'state', 'country']].drop_duplicates().iterrows():
            firm = str(row.get('recalling_firm', ''))[:200]
            if not firm or firm == 'nan':
                continue
            key = f"{firm}"
            if key not in company_map:
                company_map[key] = company_key
                companies.append({
                    'CompanyKey': company_key,
                    'CompanyName': firm,
                    'City': str(row.get('city', '')) if pd.notna(row.get('city')) else None,
                    'State': str(row.get('state', '')) if pd.notna(row.get('state')) else None,
                    'Country': str(row.get('country', 'United States')) if pd.notna(row.get('country')) else 'United States',
                    'EstablishmentNumber': None
                })
                company_key += 1

    dim_company = pd.DataFrame(companies)
    logger.info(f"dim_company: {len(dim_company)} rows created")

    return dim_company, company_map


def parse_date(date_val) -> Optional[datetime]:
    """Parse various date formats."""
    if pd.isna(date_val):
        return None

    date_str = str(date_val)

    # Try common formats
    formats = [
        '%Y%m%d',       # 20240115
        '%Y-%m-%d',     # 2024-01-15
        '%m/%d/%Y',     # 01/15/2024
        '%d/%m/%Y',     # 15/01/2024
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str[:10], fmt)
        except ValueError:
            continue

    return None


def create_fact_recalls(
    fda_df: pd.DataFrame,
    fsis_df: pd.DataFrame,
    rasff_df: pd.DataFrame,
    uk_fsa_df: pd.DataFrame,
    geo_map: dict,
    class_map: dict,
    product_map: dict,
    company_map: dict
) -> pd.DataFrame:
    """
    Create fact_recalls table from FDA, FSIS, RASFF, and UK FSA data.

    Geography Keys:
    - GeographyKey: Recall Geography (where the recall was reported)
      - FDA: USA + State
      - FSIS: USA (no state info)
      - RASFF: notifying_country (EU member that reported)
      - UK_FSA: United Kingdom
    - OriginGeographyKey: Product Origin (where the product came from)
      - FDA: country field (mostly USA, some imports)
      - FSIS: NULL (no origin info)
      - RASFF: origin field (China, Turkey, etc.)
      - UK_FSA: NULL (no origin info)
    """
    logger.info("Creating fact_recalls...")

    facts = []
    recall_key = 1

    # Process FDA records
    for _, row in fda_df.iterrows():
        # Parse date
        date_val = parse_date(row.get('recall_initiation_date'))
        if date_val is None:
            date_val = parse_date(row.get('report_date'))

        date_key = int(date_val.strftime('%Y%m%d')) if date_val else None

        # Recall Geography: USA + State
        state = str(row.get('state', '')) if pd.notna(row.get('state')) else ''
        geo_key = geo_map.get(f"USA|{state}", geo_map.get("USA|", 1))

        # Origin Geography: FDA country field (product origin)
        origin_country = str(row.get('country', '')) if pd.notna(row.get('country')) else ''
        origin_country_clean = harmonize_country_name(origin_country) if origin_country else None
        if origin_country_clean == 'United States':
            # For USA products, use the same geo key as recall (with state)
            origin_geo_key = geo_key
        else:
            origin_geo_key = geo_map.get(origin_country_clean) if origin_country_clean else None

        cls = str(row.get('classification', '')) if pd.notna(row.get('classification')) else ''
        class_key = class_map.get(f"FDA|{cls}", 1)

        desc = str(row.get('product_description', ''))[:200]
        product_key = product_map.get(f"FDA|{desc}", 1)

        firm = str(row.get('recalling_firm', ''))[:200]
        company_key = company_map.get(firm, 1)

        # Classify recall reason
        reason_text = str(row.get('reason_for_recall', ''))[:500] if pd.notna(row.get('reason_for_recall')) else None
        recall_category, recall_group, recall_subgroup = classify_recall_reason(reason_text)

        facts.append({
            'RecallKey': recall_key,
            'RecallID': str(row.get('recall_number', f'FDA-{recall_key}')),
            'EventID': str(row.get('event_id', '')) if pd.notna(row.get('event_id')) else None,  # FDA Event (groups multiple products)
            'RecallDate': date_val,
            'Source': 'FDA',
            'GeographyKey': geo_key,
            'OriginGeographyKey': origin_geo_key,
            'ClassificationKey': class_key,
            'ProductKey': product_key,
            'CompanyKey': company_key,
            'DateKey': date_key,
            'ReasonForRecall': reason_text,
            'RecallCategory': recall_category,
            'RecallGroup': recall_group,
            'RecallSubgroup': recall_subgroup,
            'DistributionScope': str(row.get('distribution_pattern', ''))[:200] if pd.notna(row.get('distribution_pattern')) else None,
            'ActionTaken': None
        })
        recall_key += 1

    logger.info(f"Processed {len(fda_df)} FDA records")

    # Process FSIS records
    for _, row in fsis_df.iterrows():
        # Parse date
        date_val = parse_date(row.get('open_date'))
        date_key = int(date_val.strftime('%Y%m%d')) if date_val else None

        # Recall Geography: FSIS is USA only (no state info)
        geo_key = geo_map.get("USA|", 1)

        # Origin Geography: FSIS has no origin info
        origin_geo_key = None

        cls = str(row.get('class', '')) if pd.notna(row.get('class')) else ''
        class_key = class_map.get(f"FSIS|{cls}", 1)

        product = str(row.get('product', ''))[:200]
        product_key = product_map.get(f"FSIS|{product}", 1)

        # FSIS doesn't have company names in this dataset
        company_key = 1

        # Classify recall reason
        reason_text = str(row.get('problem_type', ''))[:500] if pd.notna(row.get('problem_type')) else None
        recall_category, recall_group, recall_subgroup = classify_recall_reason(reason_text)

        facts.append({
            'RecallKey': recall_key,
            'RecallID': str(row.get('recall_number', f'FSIS-{recall_key}')),
            'EventID': str(row.get('recall_number', f'FSIS-{recall_key}')),  # FSIS has no event hierarchy
            'RecallDate': date_val,
            'Source': 'FSIS',
            'GeographyKey': geo_key,
            'OriginGeographyKey': origin_geo_key,
            'ClassificationKey': class_key,
            'ProductKey': product_key,
            'CompanyKey': company_key,
            'DateKey': date_key,
            'ReasonForRecall': reason_text,
            'RecallCategory': recall_category,
            'RecallGroup': recall_group,
            'RecallSubgroup': recall_subgroup,
            'DistributionScope': None,
            'ActionTaken': None
        })
        recall_key += 1

    logger.info(f"Processed {len(fsis_df)} FSIS records")

    # Process RASFF records
    if rasff_df is not None and not rasff_df.empty:
        for _, row in rasff_df.iterrows():
            # Parse date - already converted to datetime in load_rasff_data
            date_val = row.get('date')
            if pd.notna(date_val):
                if isinstance(date_val, str):
                    date_val = pd.to_datetime(date_val, errors='coerce')
                date_key = int(date_val.strftime('%Y%m%d')) if pd.notna(date_val) else None
            else:
                date_val = None
                date_key = None

            # Recall Geography: notifying_country (EU member that reported the issue)
            notifying = str(row.get('notifying_country', '')) if pd.notna(row.get('notifying_country')) else ''
            geo_key = geo_map.get(notifying, 1)

            # Origin Geography: origin (where the product came from)
            origin = str(row.get('origin', '')) if pd.notna(row.get('origin')) else ''
            # Skip comma-separated lists (distribution data that leaked into origin)
            if ',' in origin:
                origin_geo_key = None
            else:
                origin_geo_key = geo_map.get(origin) if origin else None

            # Get classification key based on notification type + risk decision
            notif_type = str(row.get('classification', '')) if pd.notna(row.get('classification')) else 'unknown'
            risk = str(row.get('risk_decision', '')) if pd.notna(row.get('risk_decision')) else 'unknown'
            class_key = class_map.get(f"RASFF|{notif_type}|{risk}", 1)

            # Get product key based on subject
            subject = str(row.get('subject', ''))[:200] if pd.notna(row.get('subject')) else ''
            product_key = product_map.get(f"RASFF|{subject}", 1)

            # RASFF doesn't have company data in standard format
            company_key = 1

            # Build reason from substance/hazard info
            substance = str(row.get('substance', '')) if pd.notna(row.get('substance')) else ''
            hazard_cat = str(row.get('hazard_category', '')) if pd.notna(row.get('hazard_category')) else ''
            reason = f"{substance} ({hazard_cat})" if substance else hazard_cat

            # Classify recall reason
            reason_for_classification = reason[:500] if reason else None
            recall_category, recall_group, recall_subgroup = classify_recall_reason(reason_for_classification)

            facts.append({
                'RecallKey': recall_key,
                'RecallID': str(row.get('reference', f'RASFF-{recall_key}')),
                'EventID': str(row.get('reference', f'RASFF-{recall_key}')),  # RASFF reference is the event level
                'RecallDate': date_val,
                'Source': 'RASFF',
                'GeographyKey': geo_key,
                'OriginGeographyKey': origin_geo_key,
                'ClassificationKey': class_key,
                'ProductKey': product_key,
                'CompanyKey': company_key,
                'DateKey': date_key,
                'ReasonForRecall': reason_for_classification,
                'RecallCategory': recall_category,
                'RecallGroup': recall_group,
                'RecallSubgroup': recall_subgroup,
                'DistributionScope': str(row.get('distribution', ''))[:200] if pd.notna(row.get('distribution')) else None,
                'ActionTaken': str(row.get('action_taken', ''))[:200] if pd.notna(row.get('action_taken')) else None
            })
            recall_key += 1

        logger.info(f"Processed {len(rasff_df)} RASFF records")

    # Process UK FSA records
    if uk_fsa_df is not None and not uk_fsa_df.empty:
        for _, row in uk_fsa_df.iterrows():
            # Parse date - already converted to datetime in load_uk_fsa_data
            date_val = row.get('date')
            if pd.notna(date_val):
                if isinstance(date_val, str):
                    date_val = pd.to_datetime(date_val, errors='coerce')
                date_key = int(date_val.strftime('%Y%m%d')) if pd.notna(date_val) else None
            else:
                date_val = None
                date_key = None

            # Recall Geography: UK is always United Kingdom
            geo_key = geo_map.get("United Kingdom", 1)

            # Origin Geography: UK FSA has no origin info
            origin_geo_key = None

            # Get classification key based on alert type
            alert_type = str(row.get('alert_type', 'Alert')) if pd.notna(row.get('alert_type')) else 'Alert'
            class_key = class_map.get(f"UK_FSA|{alert_type}", 1)

            # Get product key based on product name
            product_name = str(row.get('product_name', ''))[:200] if pd.notna(row.get('product_name')) else ''
            product_key = product_map.get(f"UK_FSA|{product_name}", 1)

            # UK FSA doesn't have company data
            company_key = 1

            # Build reason from risk statement and allergens
            risk_stmt = str(row.get('risk_statement', '')) if pd.notna(row.get('risk_statement')) else ''
            allergens = str(row.get('allergens', '')) if pd.notna(row.get('allergens')) else ''
            reason = risk_stmt if risk_stmt else (f"Allergens: {allergens}" if allergens else None)

            # Classify recall reason
            reason_for_classification = reason[:500] if reason else None
            recall_category, recall_group, recall_subgroup = classify_recall_reason(reason_for_classification)

            facts.append({
                'RecallKey': recall_key,
                'RecallID': str(row.get('reference', f'UK_FSA-{recall_key}')),
                'EventID': str(row.get('reference', f'UK_FSA-{recall_key}')),  # UK_FSA has no event hierarchy
                'RecallDate': date_val,
                'Source': 'UK_FSA',
                'GeographyKey': geo_key,
                'OriginGeographyKey': origin_geo_key,
                'ClassificationKey': class_key,
                'ProductKey': product_key,
                'CompanyKey': company_key,
                'DateKey': date_key,
                'ReasonForRecall': reason_for_classification,
                'RecallCategory': recall_category,
                'RecallGroup': recall_group,
                'RecallSubgroup': recall_subgroup,
                'DistributionScope': str(row.get('countries', 'United Kingdom'))[:200],
                'ActionTaken': None
            })
            recall_key += 1

        logger.info(f"Processed {len(uk_fsa_df)} UK FSA records")

    fact_recalls = pd.DataFrame(facts)
    logger.info(f"fact_recalls: {len(fact_recalls)} total rows created")

    return fact_recalls


def create_fact_health_impact(cdc_df: pd.DataFrame) -> pd.DataFrame:
    """Create fact_health_impact table from CDC NORS data."""
    logger.info("Creating fact_health_impact...")

    if cdc_df.empty:
        logger.warning("No CDC data available")
        return pd.DataFrame()

    facts = []
    impact_key = 1

    for _, row in cdc_df.iterrows():
        year = int(row.get('year', 0)) if pd.notna(row.get('year')) else None
        month = int(row.get('month', 1)) if pd.notna(row.get('month')) else 1

        # Create date key from year and month
        if year:
            date_key = int(f"{year}{month:02d}01")
        else:
            date_key = None

        facts.append({
            'HealthImpactKey': impact_key,
            'OutbreakID': str(row.get('cdcid', f'CDC-{impact_key}')) if pd.notna(row.get('cdcid')) else f'CDC-{impact_key}',
            'Year': year,
            'Month': month,
            'DateKey': date_key,
            'State': str(row.get('state', '')) if pd.notna(row.get('state')) else None,
            'Illnesses': int(row.get('illnesses', 0)) if pd.notna(row.get('illnesses')) else 0,
            'Hospitalizations': int(row.get('hospitalizations', 0)) if pd.notna(row.get('hospitalizations')) else 0,
            'Deaths': int(row.get('deaths', 0)) if pd.notna(row.get('deaths')) else 0,
            'Pathogen': str(row.get('etiology', ''))[:200] if pd.notna(row.get('etiology')) else None,
            'Serotype': str(row.get('serotype_or_genotype', ''))[:200] if pd.notna(row.get('serotype_or_genotype')) else None,
            'FoodVehicle': str(row.get('food_vehicle', ''))[:200] if pd.notna(row.get('food_vehicle')) else None,
            'IFSACCategory': str(row.get('ifsac_category', ''))[:200] if pd.notna(row.get('ifsac_category')) else None,
            'Setting': str(row.get('setting', ''))[:200] if pd.notna(row.get('setting')) else None,
            'PrimaryMode': str(row.get('primary_mode', ''))[:100] if pd.notna(row.get('primary_mode')) else None
        })
        impact_key += 1

    fact_health_impact = pd.DataFrame(facts)
    logger.info(f"fact_health_impact: {len(fact_health_impact)} rows created")

    return fact_health_impact


def save_to_parquet(df: pd.DataFrame, name: str, output_dir: Path):
    """Save DataFrame to Parquet file."""
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / f"{name}.parquet"

    # Convert object columns with mixed types to string
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).replace('None', None).replace('nan', None)

    df.to_parquet(filepath, index=False, engine='pyarrow')
    logger.info(f"Saved {name}: {len(df)} rows to {filepath}")


def main():
    """Main execution function."""
    logger.info("=" * 70)
    logger.info("TASK 2: Data Harmonization & Star Schema Transformation")
    logger.info("=" * 70)

    # Load source data
    fda_df = load_fda_data()
    fsis_df = load_fsis_data()
    cdc_df = load_cdc_data()
    rasff_df = load_rasff_data()
    uk_fsa_df = load_uk_fsa_data()

    # Filter FDA for Food only (product_type = 'Food')
    if 'product_type' in fda_df.columns:
        fda_food = fda_df[fda_df['product_type'] == 'Food'].copy()
        logger.info(f"FDA filtered to Food only: {len(fda_food)} records")
    else:
        fda_food = fda_df.copy()

    # Create dimension tables (now including RASFF and UK FSA)
    dim_date = create_dim_date(min_year=2012, max_year=2026)  # Extended to 2026 for UK FSA
    dim_geography, geo_map = create_dim_geography(fda_food, fsis_df, rasff_df, uk_fsa_df)
    dim_classification, class_map = create_dim_classification(fda_food, fsis_df, rasff_df, uk_fsa_df)
    dim_product, product_map = create_dim_product(fda_food, fsis_df, rasff_df, uk_fsa_df)
    dim_company, company_map = create_dim_company(fda_food)

    # Create fact tables (now including RASFF and UK FSA)
    fact_recalls = create_fact_recalls(
        fda_food, fsis_df, rasff_df, uk_fsa_df,
        geo_map, class_map, product_map, company_map
    )
    fact_health_impact = create_fact_health_impact(cdc_df)

    # Save to Parquet
    logger.info("\nSaving Star Schema to Parquet files...")
    save_to_parquet(dim_date, 'dim_date', OUTPUT_DIR)
    save_to_parquet(dim_geography, 'dim_geography', OUTPUT_DIR)
    save_to_parquet(dim_classification, 'dim_classification', OUTPUT_DIR)
    save_to_parquet(dim_product, 'dim_product', OUTPUT_DIR)
    save_to_parquet(dim_company, 'dim_company', OUTPUT_DIR)
    save_to_parquet(fact_recalls, 'fact_recalls', OUTPUT_DIR)

    if not fact_health_impact.empty:
        save_to_parquet(fact_health_impact, 'fact_health_impact', OUTPUT_DIR)

    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("TASK 2 COMPLETE - Star Schema created successfully!")
    logger.info("=" * 70)
    logger.info("\nSummary:")
    logger.info(f"  fact_recalls:       {len(fact_recalls):,} rows")
    logger.info(f"  fact_health_impact: {len(fact_health_impact):,} rows")
    logger.info(f"  dim_date:           {len(dim_date):,} rows")
    logger.info(f"  dim_geography:      {len(dim_geography):,} rows")
    logger.info(f"  dim_classification: {len(dim_classification):,} rows")
    logger.info(f"  dim_product:        {len(dim_product):,} rows")
    logger.info(f"  dim_company:        {len(dim_company):,} rows")
    logger.info(f"\nOutput directory: {OUTPUT_DIR}")

    # Data quality metrics
    logger.info("\nData Quality Metrics:")
    logger.info(f"  FDA records (Food): {len(fda_food):,}")
    logger.info(f"  FSIS records:       {len(fsis_df):,}")
    logger.info(f"  RASFF records:      {len(rasff_df):,}")
    logger.info(f"  UK FSA records:     {len(uk_fsa_df):,}")
    logger.info(f"  CDC records:        {len(cdc_df):,}")

    # Records by source in fact table
    if 'Source' in fact_recalls.columns:
        source_counts = fact_recalls['Source'].value_counts()
        logger.info("\nRecords by Source in fact_recalls:")
        for source, count in source_counts.items():
            logger.info(f"  {source}: {count:,}")

    if 'RecallDate' in fact_recalls.columns:
        valid_dates = fact_recalls['RecallDate'].notna().sum()
        logger.info(f"\nRecords with valid dates: {valid_dates:,} ({100*valid_dates/len(fact_recalls):.1f}%)")

    # Classification statistics
    if 'RecallCategory' in fact_recalls.columns:
        logger.info("\n" + "=" * 50)
        logger.info("RECALL CLASSIFICATION STATISTICS")
        logger.info("=" * 50)

        # Level 1: RecallCategory
        logger.info("\nLevel 1 - RecallCategory:")
        cat_counts = fact_recalls['RecallCategory'].value_counts()
        for cat, count in cat_counts.items():
            pct = 100 * count / len(fact_recalls)
            logger.info(f"  {cat}: {count:,} ({pct:.1f}%)")

        # Level 2: RecallGroup
        logger.info("\nLevel 2 - RecallGroup:")
        group_counts = fact_recalls['RecallGroup'].value_counts()
        for group, count in group_counts.head(15).items():
            pct = 100 * count / len(fact_recalls)
            logger.info(f"  {group}: {count:,} ({pct:.1f}%)")

        # Level 3: Top RecallSubgroups
        logger.info("\nLevel 3 - Top RecallSubgroups:")
        subgroup_counts = fact_recalls['RecallSubgroup'].dropna().value_counts()
        for subgroup, count in subgroup_counts.head(15).items():
            if subgroup and subgroup != 'None':
                pct = 100 * count / len(fact_recalls)
                logger.info(f"  {subgroup}: {count:,} ({pct:.1f}%)")

        # Other (unclassified) rate
        other_count = (fact_recalls['RecallCategory'] == 'Other').sum()
        logger.info(f"\n'Other' records (not specifically classified): {other_count:,} ({100*other_count/len(fact_recalls):.1f}%)")

    return fact_recalls, fact_health_impact


if __name__ == "__main__":
    main()
