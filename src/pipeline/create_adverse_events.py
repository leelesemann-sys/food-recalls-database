"""
Create fact_adverse_events table from FDA CAERS (Adverse Event Reporting System) data.
Filters to food-related reports only (excludes cosmetics).
"""
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

# Setup paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
INPUT_FILE = PROJECT_ROOT / 'data' / 'input' / 'fda-data-usa' / 'food-event-0001-of-0001.json'
OUTPUT_FILE = PROJECT_ROOT / 'data' / 'output' / 'parquet' / 'fact_adverse_events.parquet'

# Mapping FDA IndustryCategory -> Our ProductType
INDUSTRY_TO_PRODUCTTYPE = {
    # Supplements
    'Vit/Min/Prot/Unconv Diet(Human/Animal)': 'Supplement',
    'Dietary Conventional Foods/Meal Replacements': 'Supplement',
    'Powder Formula': 'Supplement',

    # Fresh Produce
    'Vegetables/Vegetable Products': 'Fresh Produce',
    'Fruit/Fruit Prod': 'Fresh Produce',
    'Prep Salad Prod': 'Fresh Produce',

    # Nuts/Seeds
    'Nuts/Edible Seed': 'Nuts/Seeds',

    # Seafood
    'Fishery/Seafood Prod': 'Seafood',

    # Dairy
    'Milk/Butter/Dried Milk Prod': 'Dairy',
    'Ice Cream Prod': 'Dairy',
    'Cheese/Cheese Prod': 'Dairy',

    # Fresh Protein
    'Egg/Egg Prod': 'Fresh Protein',
    'Meat, Meat Products And Poultry': 'Fresh Protein',

    # Bakery/Grains
    'Bakery Prod/Dough/Mix/Icing': 'Bakery/Grains',
    'Cereal Prep/Breakfast Food': 'Bakery/Grains',
    'Whole Grain/Milled Grain Prod/Starch': 'Bakery/Grains',

    # Beverage
    'Soft Drink/Water': 'Beverage',
    'Coffee/Tea': 'Beverage',

    # Confectionery
    'Candy W/O Choc/Special/Chew Gum': 'Confectionery',
    'Choc/Cocoa Prod': 'Confectionery',

    # Ready-to-Eat
    'Mult Food Dinner/Grav/Sauce/Special': 'Ready-to-Eat',
    'Soup': 'Ready-to-Eat',
    'Baby Food Products': 'Ready-to-Eat',
    'Snack Food Item': 'Ready-to-Eat',

    # Ingredients
    'Spices, Flavors And Salts': 'Ingredients',
    'Food Additives (Human Use)': 'Ingredients',
    'Dressings/Condiments': 'Ingredients',

    # Processed
    'Food Service/Convnce Store': 'Processed',
    'Macaroni/Noodle Prod': 'Processed',
}

def get_product_type(industry_category: str) -> str:
    """Map FDA IndustryCategory to our ProductType."""
    if not industry_category:
        return 'Other'
    return INDUSTRY_TO_PRODUCTTYPE.get(industry_category, 'Other')

print("Loading FDA CAERS data...")
with open(INPUT_FILE, 'r', encoding='utf-8') as f:
    data = json.load(f)

results = data['results']
print(f"Total reports loaded: {len(results):,}")

# Load dim_date for DateKey lookup
dim_date = pd.read_parquet(PROJECT_ROOT / 'data' / 'output' / 'parquet' / 'dim_date.parquet')
date_lookup = dict(zip(dim_date['Date'].astype(str), dim_date['DateKey']))

print("Processing reports (filtering to food only)...")

records = []
key = 1
skipped_cosmetics = 0

for r in results:
    # Get first product info
    products = r.get('products', [])
    if not products:
        continue

    product = products[0]
    industry_name = product.get('industry_name', '')

    # Skip cosmetics
    if 'Cosmetic' in industry_name:
        skipped_cosmetics += 1
        continue

    # Parse date
    date_str = r.get('date_created', '')
    year = None
    date_key = None

    if date_str and len(date_str) >= 8:
        try:
            year = int(date_str[:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])
            date_obj = datetime(year, month, day)
            date_formatted = date_obj.strftime('%Y-%m-%d')
            date_key = date_lookup.get(date_formatted)
        except (ValueError, IndexError):
            pass

    # Extract outcomes as boolean flags
    outcomes = r.get('outcomes', [])

    # Consumer info
    consumer = r.get('consumer', {})
    age = consumer.get('age')
    age_unit = consumer.get('age_unit', '')
    gender = consumer.get('gender')

    # Convert age to years if needed
    age_years = None
    if age:
        try:
            age_num = float(age)
            if 'month' in age_unit.lower():
                age_years = age_num / 12
            elif 'day' in age_unit.lower():
                age_years = age_num / 365
            else:
                age_years = age_num
        except ValueError:
            pass

    # Count reactions
    reactions = r.get('reactions', [])

    record = {
        'AdverseEventKey': key,
        'ReportNumber': r.get('report_number'),
        'DateKey': date_key,
        'Year': year,
        'Month': int(date_str[4:6]) if date_str and len(date_str) >= 6 else None,
        'IndustryCode': product.get('industry_code'),
        'IndustryCategory': industry_name,
        'ProductType': get_product_type(industry_name),
        'ProductName': product.get('name_brand'),
        'ConsumerAge': int(age_years) if age_years and age_years > 0 else None,
        'ConsumerGender': gender,
        'HasHospitalization': 'Hospitalization' in outcomes,
        'HasEmergencyRoom': 'Visited Emergency Room' in outcomes,
        'HasDeath': 'Death' in outcomes,
        'HasLifeThreatening': 'Life Threatening' in outcomes,
        'HasDisability': 'Disability' in outcomes,
        'HasAllergicReaction': 'Allergic Reaction' in outcomes,
        'HasHealthcareVisit': 'Visited a Health Care Provider' in outcomes,
        'ReactionCount': len(reactions),
        'OutcomeCount': len(outcomes)
    }

    records.append(record)
    key += 1

print(f"Skipped cosmetics: {skipped_cosmetics:,}")
print(f"Food reports processed: {len(records):,}")

# Create DataFrame
df = pd.DataFrame(records)

# Summary statistics
print()
print("=== fact_adverse_events Summary ===")
print(f"Total rows: {len(df):,}")
print()

print("Outcomes (Food Only):")
print(f"  Hospitalizations: {df['HasHospitalization'].sum():,}")
print(f"  Emergency Room: {df['HasEmergencyRoom'].sum():,}")
print(f"  Deaths: {df['HasDeath'].sum():,}")
print(f"  Life Threatening: {df['HasLifeThreatening'].sum():,}")
print(f"  Disabilities: {df['HasDisability'].sum():,}")
print(f"  Allergic Reactions: {df['HasAllergicReaction'].sum():,}")
print()

print("Top 10 Industry Categories:")
print(df['IndustryCategory'].value_counts().head(10))
print()

print("ProductType Distribution (mapped to our schema):")
print(df['ProductType'].value_counts())
print()

print("Reports by Year (last 10 years):")
year_counts = df[df['Year'] >= 2015].groupby('Year').size()
print(year_counts)
print()

print("Gender Distribution:")
print(df['ConsumerGender'].value_counts())
print()

# Save to parquet
df.to_parquet(OUTPUT_FILE, index=False)
print(f"Saved to {OUTPUT_FILE}")
