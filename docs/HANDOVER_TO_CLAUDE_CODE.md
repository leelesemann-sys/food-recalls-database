# 🚀 HANDOVER: FOOD RECALL ANALYTICS PROJECT

## 📊 PROJECT OVERVIEW

**Goal:** Multi-nationale Food Recall Analytics Platform (USA vs EU) für 2012-2025 in Azure Synapse + Power BI

**Scope:**
- Timeframe: 2012-2025 (14 Jahre)
- Total Records: ~105,000 recalls/notifications
- Geographic: USA + EU (27 countries) + Switzerland
- Tech Stack: Azure Data Lake Gen2 + Synapse Analytics + Power BI

---

## ✅ CURRENT STATUS

### **COMPLETED:**

1. **Project Planning**
   - Projektplan erstellt (siehe Tabelle unten)
   - Datenquellen analysiert und validiert
   - Klassifikationssysteme dokumentiert
   - Star Schema Design definiert

2. **Azure Infrastructure**
   - ✅ Resource Group: Vorhanden
   - ✅ Data Lake Gen2: `foodrecalls-synapse`
   - ✅ Data Factory: Instance läuft
   - ✅ Synapse Workspace: Deployed und erreichbar
   - ✅ Synapse Studio: User ist eingeloggt

3. **Data Sources Status**

| Source | Status | Location | Records | Years |
|--------|--------|----------|---------|-------|
| **FDA** | ✅ In Data Lake | `raw/fdajson/*.json` | ~20,000 | 2012-2025 |
| **FSIS** | ✅ In Data Lake | `raw/fsis_recalls.json` | 1,047 | 2014-2026 |
| **RASFF** | ✅ Uploaded | `raw/rasff/*.xlsx` (2 files) | 63,938 | 2012-2025 |
| **CDC NORS** | ⏳ TO DO | Need pipeline | ~12,000 | 2012-2023 |

4. **Documentation Created**
   - Classification Systems Comparison (FDA/FSIS/RASFF)
   - RASFF Data Quality Assessment
   - API Historical Data Availability Report
   - Project Plan with Tasks

---

## 🏗️ TECHNICAL ARCHITECTURE

### **Current Data Lake Structure:**

```
Storage Account: foodrecalls-synapse
Container: raw
├── fdajson/
│   ├── fda_part_0.json
│   ├── fda_part_1000.json
│   ├── fda_part_2000.json
│   └── ... (multiple batches, 1000 records each due to API limit)
│
├── fsis_recalls.json  (single file, 1,200 records, English + Spanish)
│
├── rasff/
│   ├── RASFF_notifications_pre-2021_public_information_ab_2012.xlsx  (39,298 records, 2012-2020)
│   └── RASFF_window_results.xlsx  (24,640 records, 2020-2025)
│
├── gold/  (existing Python script outputs - to be replaced)
│   ├── *.parquet
│   └── *.csv
│
└── cdc/  (TO BE CREATED)
    └── cdc_nors_YYYY-MM-DD.json
```

### **Target Architecture:**

```
Bronze Layer (raw/):
└── Raw JSON/Excel files (bereits vorhanden + CDC)

Silver Layer (to be created):
└── Harmonized Parquet files (Python transformation)

Gold Layer (gold/):
├── fact_recalls.parquet
├── dim_geography.parquet
├── dim_classification.parquet
├── dim_product.parquet
├── dim_company.parquet
└── dim_date.parquet

Synapse Serverless SQL Pool:
└── External Tables on gold/*.parquet

Power BI:
└── DirectQuery to Synapse External Tables
```

---

## 🎯 YOUR TASKS (IN ORDER)

### **TASK 1: CDC NORS DATA PIPELINE** ⏳ URGENT

**Create Azure Data Factory Pipeline:**

```yaml
Pipeline Name: Pipeline_CDC_NORS
Activity: Copy Data

Source:
  Type: REST API
  Base URL: https://data.cdc.gov/resource/5xkq-dg7x.json
  Authentication: Anonymous (no API key needed)
  Query Parameters: ?$where=year>=2012&$limit=50000
  Method: GET

Sink:
  Type: Azure Data Lake Gen2
  Container: raw
  Directory: cdc
  Filename: cdc_nors_{utcnow()}.json
  Format: JSON
```

**Expected Output:**
- File: `raw/cdc/cdc_nors_2026-01-22.json`
- Records: ~12,000 (2012-2023)

**Validation:**
```python
import pandas as pd
df = pd.read_json('raw/cdc/cdc_nors_*.json')
print(f"Records: {len(df)}")
print(f"Years: {df['year'].min()} - {df['year'].max()}")
print(f"Columns: {df.columns.tolist()}")
```

---

### **TASK 2: DATA HARMONIZATION & STAR SCHEMA**

**Create Synapse Notebook (Python):** `Notebook_Transform_to_Star_Schema`

**Requirements:**

1. **Load all 4 sources:**
```python
# FDA: Multiple JSON files (batched)
fda_files = list(Path('raw/fdajson/').glob('fda_part_*.json'))
fda_list = [pd.read_json(f) for f in fda_files]
fda_df = pd.concat(fda_list, ignore_index=True)

# FSIS: Single JSON (filter English only)
fsis_df = pd.read_json('raw/fsis_recalls.json')
fsis_df = fsis_df[fsis_df['langcode'] == 'English']
fsis_df = fsis_df[fsis_df['field_recall_type'] != 'Public Health Alert']  # Nur Recalls

# RASFF: 2 Excel files (merge Historical + Current)
rasff_hist = pd.read_excel('raw/rasff/RASFF_notifications_pre-2021_public_information_ab_2012.xlsx')
rasff_curr = pd.read_excel('raw/rasff/RASFF_window_results.xlsx')
# Filter: Use Historical for 2012-2020, Current for 2021-2025 (avoid Dec 2020 overlap)
rasff_hist = rasff_hist[rasff_hist['Date'] < '2021-01-01']
rasff_curr = rasff_curr[rasff_curr['date'] >= '2021-01-01']
rasff_df = pd.concat([rasff_hist, rasff_curr], ignore_index=True)

# CDC: Single JSON
cdc_df = pd.read_json('raw/cdc/cdc_nors_*.json')
```

2. **Column Mapping:**

**FDA Enforcement API Fields:**
```python
fda_mapping = {
    'recall_number': 'RecallID',
    'recall_initiation_date': 'RecallDate',
    'product_description': 'ProductName',
    'recalling_firm': 'CompanyName',
    'reason_for_recall': 'ReasonForRecall',
    'classification': 'ClassLevel',  # 'Class I', 'Class II', 'Class III'
    'distribution_pattern': 'DistributionScope',
    'country': 'OriginCountry',
    'state': 'State'
}
```

**FSIS Fields:**
```python
fsis_mapping = {
    'field_recall_number': 'RecallID',
    'field_recall_date': 'RecallDate',
    'field_product_items': 'ProductName',
    'field_establishment': 'CompanyName',
    'field_recall_reason': 'ReasonForRecall',
    'field_recall_classification': 'ClassLevel',  # 'Class I', 'Class II', 'Class III'
    'field_states': 'DistributionScope',
    'field_year': 'Year'
}
# Add: IsPublicHealthAlert = False (already filtered)
```

**RASFF Historical Fields:**
```python
rasff_hist_mapping = {
    'REFERENCE': 'RecallID',
    'Date': 'RecallDate',
    'product': 'ProductName',
    'subject': 'Subject',
    'notifying': 'NotifyingCountry',
    'origin': 'OriginCountry',
    'type2': 'NotificationType',  # 'alert', 'border rejection', 'information for attention', etc.
    'Action taken': 'ActionTaken',
    'distribution status': 'DistributionScope',
    'hazard category': 'HazardCategory',
    'substance/finding': 'Substance'
}
# Note: No 'risk_decision' in Historical → Set to NULL
```

**RASFF Current Fields:**
```python
rasff_curr_mapping = {
    'reference': 'RecallID',
    'date': 'RecallDate',
    'subject': 'Subject',
    'notifying_country': 'NotifyingCountry',
    'origin': 'OriginCountry',
    'classification': 'NotificationType',  # 'alert notification', 'border rejection notification', etc.
    'risk_decision': 'RiskDecision',  # 'serious', 'potentially serious', 'potential risk', 'not serious', 'undecided'
    'distribution': 'DistributionScope',
    'hazards': 'HazardsRaw'  # Format: "Listeria monocytogenes - {pathogenic micro-organisms}"
}
```

**CDC NORS Fields (for health impact context):**
```python
cdc_mapping = {
    'cdcid': 'OutbreakID',
    'year': 'Year',
    'month': 'Month',
    'state': 'State',
    'illnesses': 'Illnesses',
    'hospitalizations': 'Hospitalizations',
    'deaths': 'Deaths',
    'serotype_or_genotype': 'Pathogen',
    'food_vehicle': 'FoodVehicle'
}
# Note: CDC is separate - not direct recalls, but outbreak context
```

3. **Star Schema Creation:**

**Fact_Recalls Table:**
```python
columns = [
    'RecallKey',           # INT, Auto-increment
    'RecallID',            # NVARCHAR(50), Original ID
    'RecallDate',          # DATE
    'Source',              # NVARCHAR(10): 'FDA', 'FSIS', 'RASFF'
    'GeographyKey',        # INT, FK to Dim_Geography
    'ClassificationKey',   # INT, FK to Dim_Classification
    'ProductKey',          # INT, FK to Dim_Product
    'CompanyKey',          # INT, FK to Dim_Company
    'DateKey',             # INT, FK to Dim_Date (YYYYMMDD)
    'ReasonForRecall',     # NVARCHAR(500)
    'DistributionScope',   # NVARCHAR(200)
    'ActionTaken',         # NVARCHAR(200) (RASFF only)
    'Illnesses',           # INT (if linked to CDC)
    'Hospitalizations',    # INT
    'Deaths'               # INT
]
```

**Dim_Classification:**
```python
columns = [
    'ClassificationKey',        # INT, PK
    'Source',                   # NVARCHAR(10): 'FDA', 'FSIS', 'RASFF'
    'OriginalClassification',   # NVARCHAR(100)
    
    # USA Specific:
    'USAClassLevel',            # NVARCHAR(10): 'Class I', 'Class II', 'Class III'
    'IsPublicHealthAlert',      # BIT (FSIS only)
    
    # EU Specific:
    'NotificationType',         # NVARCHAR(50): 'alert', 'border rejection', 'information for attention', etc.
    'RiskDecision',             # NVARCHAR(30): 'serious', 'potentially serious', 'potential risk', 'not serious', 'undecided'
    
    # Unified (optional):
    'SeverityLevel',            # NVARCHAR(20): 'High', 'Medium', 'Low', 'Undecided'
    'SeverityScore'             # INT (1-10)
]
```

**Dim_Geography:**
```python
columns = [
    'GeographyKey',        # INT, PK
    'Country',             # NVARCHAR(100)
    'CountryCode',         # NVARCHAR(3) (ISO)
    'State',               # NVARCHAR(100) (USA only)
    'Region',              # NVARCHAR(50): 'USA', 'EU', 'Other'
    'IsEUMember',          # BIT
    'IsEFTA'               # BIT (Switzerland, Norway, Iceland)
]

# CRITICAL: Country name harmonization needed!
country_mapping = {
    # RASFF Historical uses ALL CAPS
    'ITALY': 'Italy',
    'GERMANY': 'Germany',
    'THE NETHERLANDS': 'Netherlands',
    'TURKEY': 'Türkiye',
    # ... (see RASFF Data Quality Assessment doc for full list)
}
```

**Dim_Product:**
```python
columns = [
    'ProductKey',          # INT, PK
    'ProductName',         # NVARCHAR(500)
    'ProductCategory',     # NVARCHAR(100): 'fruits and vegetables', 'meat', 'fish', etc.
    'ProductType',         # NVARCHAR(50): 'RTE', 'Raw', 'Processed'
    'HazardCategory'       # NVARCHAR(100): 'pathogenic micro-organisms', 'allergens', etc.
]
```

**Dim_Company:**
```python
columns = [
    'CompanyKey',          # INT, PK
    'CompanyName',         # NVARCHAR(200)
    'Country',             # NVARCHAR(100)
    'EstablishmentNumber'  # NVARCHAR(50) (FSIS only)
]
```

**Dim_Date:**
```python
# Standard date dimension
columns = [
    'DateKey',        # INT, PK (YYYYMMDD)
    'Date',           # DATE
    'Year',           # INT
    'Quarter',        # INT
    'Month',          # INT
    'MonthName',      # NVARCHAR(20)
    'Day',            # INT
    'DayOfWeek',      # INT
    'DayName'         # NVARCHAR(20)
]
# Generate for 2012-2025
```

4. **Write to Gold Layer:**
```python
# Write as Parquet (optimized for analytics)
fact_recalls.to_parquet('gold/fact_recalls.parquet', index=False)
dim_geography.to_parquet('gold/dim_geography.parquet', index=False)
dim_classification.to_parquet('gold/dim_classification.parquet', index=False)
dim_product.to_parquet('gold/dim_product.parquet', index=False)
dim_company.to_parquet('gold/dim_company.parquet', index=False)
dim_date.to_parquet('gold/dim_date.parquet', index=False)

# Optional: CDC as separate fact table
fact_health_impact.to_parquet('gold/fact_health_impact.parquet', index=False)
```

---

### **TASK 3: SYNAPSE SERVERLESS SQL EXTERNAL TABLES**

**Create SQL Script:** `Create_External_Tables.sql`

**1. Create External Data Source:**
```sql
-- Run in Synapse Serverless SQL Pool (Built-in)

-- Create Master Key (if not exists)
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'YourStrongPassword123!';

-- Create Database Scoped Credential (for Data Lake access)
CREATE DATABASE SCOPED CREDENTIAL DataLakeCredential
WITH IDENTITY = 'Managed Identity';

-- Create External Data Source
CREATE EXTERNAL DATA SOURCE DataLakeSource
WITH (
    LOCATION = 'abfss://raw@synapserecalls.dfs.core.windows.net',
    CREDENTIAL = DataLakeCredential
);

-- Create External File Format for Parquet
CREATE EXTERNAL FILE FORMAT ParquetFormat
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);
```

**2. Create External Tables:**
```sql
-- Fact Table
CREATE EXTERNAL TABLE fact_recalls (
    RecallKey INT,
    RecallID NVARCHAR(50),
    RecallDate DATE,
    Source NVARCHAR(10),
    GeographyKey INT,
    ClassificationKey INT,
    ProductKey INT,
    CompanyKey INT,
    DateKey INT,
    ReasonForRecall NVARCHAR(500),
    DistributionScope NVARCHAR(200),
    ActionTaken NVARCHAR(200),
    Illnesses INT,
    Hospitalizations INT,
    Deaths INT
)
WITH (
    LOCATION = 'gold/fact_recalls.parquet',
    DATA_SOURCE = DataLakeSource,
    FILE_FORMAT = ParquetFormat
);

-- Dimension Tables (similar pattern)
CREATE EXTERNAL TABLE dim_geography (
    GeographyKey INT,
    Country NVARCHAR(100),
    CountryCode NVARCHAR(3),
    State NVARCHAR(100),
    Region NVARCHAR(50),
    IsEUMember BIT,
    IsEFTA BIT
)
WITH (
    LOCATION = 'gold/dim_geography.parquet',
    DATA_SOURCE = DataLakeSource,
    FILE_FORMAT = ParquetFormat
);

-- ... repeat for other dimensions
```

**3. Create Views for Power BI:**
```sql
-- Simplified view with JOINs
CREATE VIEW vw_recalls_analysis AS
SELECT 
    f.RecallKey,
    f.RecallID,
    f.RecallDate,
    f.Source,
    g.Country,
    g.Region,
    g.IsEUMember,
    c.USAClassLevel,
    c.NotificationType,
    c.RiskDecision,
    c.SeverityLevel,
    c.SeverityScore,
    p.ProductCategory,
    p.HazardCategory,
    comp.CompanyName,
    f.ReasonForRecall,
    f.Illnesses,
    f.Hospitalizations,
    f.Deaths,
    d.Year,
    d.Quarter,
    d.Month
FROM fact_recalls f
LEFT JOIN dim_geography g ON f.GeographyKey = g.GeographyKey
LEFT JOIN dim_classification c ON f.ClassificationKey = c.ClassificationKey
LEFT JOIN dim_product p ON f.ProductKey = p.ProductKey
LEFT JOIN dim_company comp ON f.CompanyKey = comp.CompanyKey
LEFT JOIN dim_date d ON f.DateKey = d.DateKey;
```

---

### **TASK 4: DATA VALIDATION & TESTING**

**Create Validation Script:** `Validate_Data_Quality.sql`

```sql
-- Record counts per source
SELECT Source, COUNT(*) as RecordCount
FROM fact_recalls
GROUP BY Source;

-- Expected:
-- FDA:   ~20,000
-- FSIS:   ~1,047 (recalls only, no PHA)
-- RASFF: ~63,938

-- Date range validation
SELECT 
    Source,
    MIN(RecallDate) as EarliestDate,
    MAX(RecallDate) as LatestDate
FROM fact_recalls
GROUP BY Source;

-- Classification distribution
SELECT 
    Source,
    USAClassLevel,
    RiskDecision,
    COUNT(*) as Count
FROM fact_recalls f
JOIN dim_classification c ON f.ClassificationKey = c.ClassificationKey
GROUP BY Source, USAClassLevel, RiskDecision
ORDER BY Source, Count DESC;

-- Top countries
SELECT TOP 20
    Country,
    COUNT(*) as RecallCount
FROM fact_recalls f
JOIN dim_geography g ON f.GeographyKey = g.GeographyKey
GROUP BY Country
ORDER BY RecallCount DESC;

-- Year distribution
SELECT 
    Year,
    COUNT(*) as RecallCount
FROM fact_recalls f
JOIN dim_date d ON f.DateKey = d.DateKey
GROUP BY Year
ORDER BY Year;
```

---

### **TASK 5: POWER BI DATASET SETUP**

**Connect Power BI Desktop:**

1. **Get Data** → **Azure** → **Azure Synapse Analytics SQL**
2. **Server:** `foodrecalls-synapse-ondemand.sql.azuresynapse.net`
3. **Database:** `default` (or your database name)
4. **Data Connectivity:** **DirectQuery** (recommended for large datasets)
5. **Select:** `vw_recalls_analysis` view
6. **Load**

**Create Relationships (if using individual tables):**
- fact_recalls[GeographyKey] → dim_geography[GeographyKey]
- fact_recalls[ClassificationKey] → dim_classification[ClassificationKey]
- fact_recalls[ProductKey] → dim_product[ProductKey]
- fact_recalls[CompanyKey] → dim_company[CompanyKey]
- fact_recalls[DateKey] → dim_date[DateKey]

**Create DAX Measures:**
```dax
Total Recalls = COUNT(fact_recalls[RecallKey])

USA Recalls = CALCULATE(
    COUNT(fact_recalls[RecallKey]),
    fact_recalls[Source] IN {"FDA", "FSIS"}
)

EU Recalls = CALCULATE(
    COUNT(fact_recalls[RecallKey]),
    fact_recalls[Source] = "RASFF"
)

High Severity Recalls = CALCULATE(
    COUNT(fact_recalls[RecallKey]),
    dim_classification[SeverityLevel] = "High"
)
```

---

## 📚 REFERENCE DOCUMENTS

**Located in `/mnt/user-data/outputs/`:**

1. **Classification_Systems_Comparison_FDA_FSIS_RASFF.md**
   - Complete comparison of USA vs EU classification systems
   - Mapping guidelines
   - Severity score formulas

2. **RASFF_Data_Quality_Assessment_2012-2025.md**
   - Field mapping Historical vs Current
   - Country name harmonization
   - Data quality metrics

3. **Project Plan Table**
   - Checklist with all tasks
   - Status tracking

---

## ⚠️ CRITICAL NOTES

### **Data Filters:**
1. **FSIS:** Filter `langcode = 'English'` (avoid Spanish duplicates)
2. **FSIS:** Filter out `field_recall_type = 'Public Health Alert'` (nur Recalls)
3. **RASFF:** Merge Historical (2012-2020) + Current (2021-2025), avoid December 2020 overlap

### **FSIS Public Health Alerts (NOT INCLUDED):**
> **Hinweis:** FSIS "Public Health Alerts" sind KEINE Recalls und werden separat veröffentlicht.
> - **Recall**: Produkt wird aktiv vom Markt zurückgerufen (Class I/II/III)
> - **Public Health Alert**: Wird herausgegeben, wenn ein Recall nicht möglich ist (z.B. Produkt bereits verkauft/konsumiert)
>
> Unsere FSIS-Daten (`FSIS_ALL_YEARS_COMPLETE.xlsx`) enthalten **nur Recalls**, keine Public Health Alerts.
> FSIS bietet eine separate API für Public Health Alerts: https://www.fsis.usda.gov/recalls
>
> **Potentielle Erweiterung:** Public Health Alerts könnten später als eigene Datenquelle integriert werden.

### **Country Harmonization:**
- RASFF Historical uses ALL CAPS: "ITALY", "GERMANY"
- RASFF Current uses Title Case: "Italy", "Germany"
- Special case: "TURKEY" → "Türkiye" (both systems)
- See full mapping in RASFF Data Quality Assessment doc

### **RASFF Hazard Parsing:**
```python
# Current format: "Listeria monocytogenes - {pathogenic micro-organisms}"
# Parse to: Substance + HazardCategory
import re
pattern = r'(.+?)\s*-\s*\{(.+?)\}'
match = re.match(pattern, hazards_text)
if match:
    substance = match.group(1).strip()
    category = match.group(2).strip()
```

### **Synapse Serverless SQL Pool:**
- Cost: ~$5 per TB scanned
- For 105k records (~100MB): ~$0.50 per full scan
- **Monthly cost estimate: ~$2-5** (sehr günstig!)
- Connection string: `foodrecalls-synapse-ondemand.sql.azuresynapse.net`

---

## 🎯 SUCCESS CRITERIA

**MVP Complete when:**
- ✅ CDC NORS data in Data Lake
- ✅ Star Schema Parquet files in gold/
- ✅ Synapse External Tables created
- ✅ Validation queries pass (record counts match)
- ✅ Power BI connected and showing data
- ✅ Basic reports working (USA vs EU comparison)

---

## 📞 ESCALATION

**If you encounter issues with:**
- Business logic questions → Ask original Claude
- Classification mapping uncertainties → Refer to Classification Comparison doc
- RASFF data interpretation → Refer to RASFF Data Quality Assessment

**Azure Credentials/Access:**
- User has Owner rights on Resource Group
- Synapse Workspace: Deployed and accessible
- Data Lake: Network = "all networks"

---

## 🚀 START HERE

**Immediate Next Step:**
```bash
# 1. Open Azure Data Factory Studio
# 2. Create Pipeline_CDC_NORS (see TASK 1 details above)
# 3. Test & Execute pipeline
# 4. Validate CDC data in raw/cdc/
# 5. Proceed to TASK 2 (Python transformation)
```

**Estimated Time:**
- TASK 1 (CDC Pipeline): 30 minutes
- TASK 2 (Star Schema): 4-6 hours
- TASK 3 (SQL External Tables): 1-2 hours
- TASK 4 (Validation): 1 hour
- TASK 5 (Power BI): 2-3 hours

**Total MVP:** 1-2 Arbeitstage

---

**Good luck! 🚀 You have all the information needed. Let's build this!**
