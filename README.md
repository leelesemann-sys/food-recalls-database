# Food Recalls Database

A multinational food safety analytics platform that integrates recall data from six government agencies across the US, EU, and UK into a unified star schema data warehouse. Built on Azure (Data Lake Gen2, Synapse Analytics, Data Factory) with a Python ETL pipeline and Power BI reporting layer.

## Architecture

```
                      ┌──────────────┐
                      │  FDA API     │──┐
                      │  (US Food)   │  │
                      └──────────────┘  │
                      ┌──────────────┐  │    ┌──────────────────┐    ┌──────────────────┐
                      │  FSIS API    │──┤    │  Azure Data      │    │  ADLS Gen2       │
                      │  (US Meat)   │  ├───▶│  Factory         │───▶│  raw/ (Bronze)   │
                      └──────────────┘  │    │  PL_Ingest_*     │    │  JSON files      │
                      ┌──────────────┐  │    └──────────────────┘    └────────┬─────────┘
                      │  CDC NORS    │──┘                                     │
                      │  (Outbreaks) │                                        ▼
                      └──────────────┘               ┌───────────────────────────────────┐
                                                     │  Python ETL Pipeline              │
                      ┌──────────────┐               │  transform_to_star_schema.py      │
                      │  RASFF       │──────────────▶│  + 6 supporting scripts           │
                      │  (EU Alerts) │  CSV/Excel    │  (3,400+ lines)                   │
                      └──────────────┘  downloads    └────────────────┬──────────────────┘
                      ┌──────────────┐                                │
                      │  UK FSA      │───────────────────────────────▶│
                      │  (UK Alerts) │  JSON                         │
                      └──────────────┘                                ▼
                                                     ┌──────────────────────────────────┐
                      ┌──────────────┐               │  ADLS Gen2 gold/ (Gold)          │
                      │  FDA CAERS   │──────────────▶│  10 Parquet files (Star Schema)  │
                      │  (Adverse    │  CSV          │  5 Dimensions + 5 Facts          │
                      │   Events)    │               └────────────────┬─────────────────┘
                      └──────────────┘                                │
                                                                      ▼
                                                     ┌──────────────────────────────────┐
                                                     │  Azure Synapse Analytics         │
                                                     │  Serverless SQL Pool             │
                                                     │  External Tables + Views         │
                                                     └────────────────┬─────────────────┘
                                                                      │
                                                                      ▼
                                                     ┌──────────────────────────────────┐
                                                     │  Power BI Dashboard              │
                                                     │  DirectQuery on Synapse          │
                                                     └──────────────────────────────────┘
```

## Data Sources

| Source | Agency | Region | Records | Timespan |
|--------|--------|--------|--------:|----------|
| **FDA Enforcement** | U.S. Food & Drug Administration | USA | ~28,000 | 2012 -- 2026 |
| **FSIS** | USDA Food Safety Inspection Service | USA | ~1,000 | 2012 -- 2024 |
| **RASFF** | Rapid Alert System for Food and Feed | EU | ~56,000 | 2002 -- 2026 |
| **UK FSA** | Food Standards Agency | UK | ~1,000 | 2019 -- 2026 |
| **CDC NORS** | National Outbreak Reporting System | USA | ~27,000 | 1998 -- 2023 |
| **FDA CAERS** | Center for Adverse Event Reporting | USA | ~108,000 | 2004 -- 2024 |

**Total: ~221,000 records** from six agencies across three regulatory jurisdictions.

RASFF data is filtered to food-only notifications (excluding feed and food contact materials). CDC NORS is filtered to food-borne outbreaks only. FSIS entries are deduplicated for language (English-only).

## Star Schema

The data warehouse follows a Kimball-style star schema with five dimension tables and five fact tables.

### Dimensions

| Table | Description | Key Design Decisions |
|-------|-------------|----------------------|
| `dim_date` | Calendar dimension (2012--2026) | Includes FDA Fiscal Year (Oct--Sep cycle), fiscal quarters |
| `dim_geography` | 150+ locations across US, EU, UK | `IsEUMember` / `IsEFTA` flags enable pre/post-Brexit analysis |
| `dim_classification` | Severity levels across all sources | Maps FDA Class I--III, RASFF Risk Decisions, and UK FSA Alert Types onto a unified 1--10 severity scale |
| `dim_product` | Product descriptions and categories | 60+ categories mapped to 12 high-level product types |
| `dim_company` | Recalling firms (FDA/FSIS) | ~5,000 distinct companies |

### Facts

| Table | Rows | Description |
|-------|-----:|-------------|
| `fact_recalls` | ~87,000 | Core fact table -- all recalls from FDA, FSIS, RASFF, and UK FSA with dual geography keys (recall location vs. product origin) |
| `fact_adverse_events` | ~108,000 | FDA CAERS consumer complaint reports, food-only (excludes cosmetics) |
| `fact_health_impact` | ~27,000 | CDC NORS outbreak data with illness, hospitalization, and death counts |
| `fact_yearly_summary` | ~120 | Aggregated recall counts by year and source for trend analysis |
| `fact_fsis_species` | ~50 | USDA meat/poultry recalls broken down by animal species |

### Three-Level Recall Classification

Every recall in `fact_recalls` is classified through a custom three-level taxonomy:

```
RecallCategory          RecallGroup                 RecallSubgroup
─────────────────────────────────────────────────────────────────────
Product Contaminant     Biological Contamination    Listeria monocytogenes
                                                    Salmonella
                                                    E. coli O157:H7
                                                    Hepatitis A
                                                    Clostridium botulinum
                                                    ...
                        Allergens                   Milk
                                                    Peanuts
                                                    Tree Nuts
                                                    Soy
                                                    Wheat
                                                    ...
                        Chemical Contamination      Pesticides
                                                    Heavy Metals
                                                    Mycotoxins
                                                    Veterinary Drug Residues
                                                    ...
                        Foreign Objects             Metal Fragments
                                                    Glass
                                                    Plastic
                                                    ...

Process Issue           cGMP Issues                 Insanitary Conditions
                                                    Temperature Control
                                                    ...
                        Labeling Issues             Undeclared Allergens
                                                    Mislabeling
                                                    ...
```

The classification engine covers 50+ pathogens, 90+ allergen keywords (FDA Big 9 + EU-specific allergens), 70+ chemical substances, and common foreign object types. It handles source-specific formats -- including RASFF hazard notation like `Listeria monocytogenes - {pathogenic micro-organisms}` -- and correctly distinguishes between contamination events and process/labeling failures.

This taxonomy draws on published food safety classification methodologies (DeBeer et al. 2024, Blickem et al. 2025) and the IFSAC food categorization scheme.

## Project Structure

```
food-recalls-database/
├── src/
│   ├── pipeline/
│   │   ├── transform_to_star_schema.py   # Core ETL: 1,910 lines
│   │   ├── create_adverse_events.py      # FDA CAERS processing
│   │   ├── create_fsis_species.py        # USDA species breakdown
│   │   ├── create_yearly_summary.py      # Cross-source aggregation
│   │   ├── fetch_cdc_nors_data.py        # CDC NORS API client
│   │   ├── fetch_fsis_data.py            # USDA FSIS API client
│   │   └── upload_parquets_to_azure.py   # Azure Data Lake upload
│   ├── validation/
│   │   ├── validate_star_schema.py       # Referential integrity checks
│   │   └── export_classification_review.py
│   ├── notifications/
│   │   ├── email_service.py              # SMTP alerts (Jinja2 templates)
│   │   ├── state_manager.py              # Duplicate notification prevention
│   │   └── templates/
│   └── utils/
│       └── logger.py                     # Rotating file logger
├── config/
│   ├── Create_External_Tables.sql        # Synapse DDL (star schema)
│   ├── Refresh_External_Tables.sql       # Schema refresh
│   ├── Validate_Data_Quality.sql         # SQL-level quality checks
│   ├── DAX_Measures.txt                  # Power BI measures
│   └── email_settings.json
├── data/
│   ├── input/                            # Source data (not in repo)
│   └── output/
│       └── parquet/                      # Star schema Parquet files
├── docs/
│   ├── AZURE_SOLUTION_DOKU.md
│   └── articles-academic/                # Reference papers
└── requirements.txt
```

**3,400+ lines of Python** across 14 modules. **1,600+ lines of SQL** across 5 scripts. **236 lines of DAX** measures for Power BI.

## Data Harmonization

The central challenge of this project is merging six structurally different data sources into one consistent analytical model. Each source has its own schema, date format, severity system, and geographic encoding.

**Schema reconciliation** -- RASFF alone changed its export format in 2021 (different column names, added fields, restructured notification types). The pipeline handles both the pre-2021 and post-2021 schemas transparently, filling missing columns with null values to maintain a consistent output.

**Severity unification** -- FDA uses a three-class system (Class I = life-threatening, Class II = temporary health consequences, Class III = unlikely adverse health effects). RASFF uses a combination of risk decision and notification type. UK FSA has its own alert categories. All of these are mapped onto `dim_classification` with a unified severity level and a numeric score from 1 to 10.

**Geographic normalization** -- Country names appear in different casings and spellings across sources ("THE NETHERLANDS", "Netherlands", "Italy", "ITALY"). The pipeline normalizes these and maps them to a shared geography dimension, distinguishing between EU members, EFTA states, and third countries.

**Date parsing** -- Handles YYYYMMDD (FDA), YYYY-MM-DD (FSIS), and DD-MM-YYYY HH:MM:SS (RASFF) formats. The date dimension includes both calendar year and FDA Fiscal Year (October--September) to support regulatory reporting.

## Azure Infrastructure

| Resource | Name | Configuration |
|----------|------|---------------|
| Resource Group | `rg-food-recalls` | Germany West Central |
| Storage Account | `foodrecallsdata` | ADLS Gen2 (HNS), Standard_LRS, Hot tier |
| Data Factory | `foodrecalls-adf` | 2 pipelines, 7 datasets, weekly trigger |
| Synapse Analytics | `foodrecalls-synapse` | Serverless SQL Pool, External Tables on Parquet |

Data Factory handles automated ingestion from the FDA and FSIS APIs with paginated requests (1,000 records per call, until-loop with offset tracking). The remaining sources (RASFF, UK FSA, CDC NORS, FDA CAERS) are ingested through Python API clients and file downloads.

Synapse Serverless SQL exposes the Parquet files as external tables, allowing Power BI to run DirectQuery without copying data into a dedicated SQL pool -- keeping costs below $5/month.

## Validation & Quality

`validate_star_schema.py` runs automated checks after each ETL execution:

- **Record count verification** per source against expected ranges
- **Referential integrity** -- detects orphan foreign keys across all fact-to-dimension relationships
- **Date range validation** -- ensures no records fall outside the expected 2012--2026 window
- **Null threshold checks** -- flags columns exceeding 5% null values
- **Classification distribution** -- reports the split between Product Contaminant and Process Issue categories

SQL-level validation (`Validate_Data_Quality.sql`) independently verifies the same rules against the Synapse external tables to catch any Parquet-to-SQL type mapping issues.

### Parquet Type Mapping

One non-obvious production issue this project solved: Pandas nullable integers (`Int64` with capital I) are stored as `float64` in Parquet, not as `INT64`. This means Synapse external table definitions must use `FLOAT` instead of `INT` or `BIGINT` for any column that contains null values in the source data -- otherwise Power BI throws `OLE DB` type mismatch errors. The SQL scripts reflect these corrected mappings.

## Alerting

The notification system sends email alerts for new Class I recalls (life-threatening). It uses Jinja2 templates (HTML + plain text fallback), SMTP with TLS, and exponential backoff retry logic. A JSON-based state manager tracks which recalls have already been reported to prevent duplicate notifications across pipeline runs.

## Setup

```bash
# Clone and install
git clone https://github.com/leelesemann-sys/food-recalls-database.git
cd food-recalls-database
pip install -r requirements.txt

# Configure Azure credentials
cp .env.example .env
# Edit .env with your Storage Account name and key

# Run the ETL pipeline
python src/pipeline/transform_to_star_schema.py

# Generate supplementary fact tables
python src/pipeline/create_adverse_events.py
python src/pipeline/create_fsis_species.py
python src/pipeline/create_yearly_summary.py

# Upload to Azure Data Lake
python src/pipeline/upload_parquets_to_azure.py

# Validate output
python src/validation/validate_star_schema.py
```

## Requirements

- Python 3.10+
- Azure subscription (free tier sufficient)
- Source data files in `data/input/` (not included in repo due to size)

See `requirements.txt` for Python dependencies.

## References

- DeBeer, J. et al. (2024). Analyzing FDA Food Recall Patterns Using Machine Learning.
- Blickem, C. et al. (2025). Food Safety Categorization and Risk Assessment Frameworks.
- IFSAC (Interagency Food Safety Analytics Collaboration). Food Categorization Scheme.
- FDA 21 CFR Part 7 -- Enforcement Policy (Recall Classification).
- EU RASFF -- Annual Reports and Notification Guidelines.

## License

This project is provided for academic and research purposes.
