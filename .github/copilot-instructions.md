# FDA Food Recall Data Pipeline - Copilot Instructions

## Project Overview
This is an **Azure-based ETL pipeline** that ingests FDA food recall data from the FDA API, transforms it, and exports it in multiple formats (Parquet, CSV) for downstream analytics and Power BI reporting. The pipeline uses Azure Storage (Data Lake Gen2 / Blob Storage) for all data operations.

## Architecture & Data Flow

**Pipeline Stages:**
1. **Ingestion**: FDA API data arrives as JSON files (named `part_*.json`) in Azure Storage container `raw/`
2. **Validation**: Check scripts (`check*.py`, `datacheck.py`) inspect structure, dates, and quality
3. **Transformation**: Core ETL in `parquets.py` and `csvdat.py` that:
   - Deduplicates records by `recall_number` (keep first occurrence)
   - Removes problematic `openfda` column (causes ArrowNotImplementedError in Parquet)
   - Converts date strings (format: `YYYYMMDD`) to proper datetime objects
   - Exports to gold-layer files: `gold/fda_recalls_final.parquet` and `gold/fda_recalls_final.csv`
4. **Export**: Separate optimized outputs:
   - **Parquet**: For Power BI performance (compressed, fast reads)
   - **CSV**: UTF-8-sig with `;` separator for Excel compatibility (German locale)

## Key Patterns & Conventions

### Azure Storage Authentication
All scripts load credentials from `.env` via `python-dotenv`:
```python
from dotenv import load_dotenv
load_dotenv()
STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")  # foodrecallsdata
STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
```
⚠️ **Important**: Never hardcode credentials. Use `.env` (excluded via `.gitignore`).

### Data Transformation Rules
- **Deduplication**: Keep `first` occurrence of duplicates by `recall_number`
- **Date Handling**: Convert `report_date` from `YYYYMMDD` string → `datetime64[ns]` using `pd.to_datetime(..., format='%Y%m%d', errors='coerce')`
- **Null Handling**: Coerce invalid dates to `NaT` (Not a Time) rather than failing
- **Column Cleanup**: Drop `openfda` column before Parquet export (nested structure incompatibility)

### File Organization
```
raw/
  ├── part_0.json, part_1.json, ... (FDA API results)
gold/
  ├── fda_recalls_final.parquet     (optimized for BI)
  ├── fda_recalls_final.csv         (Excel-compatible)
parquet/ & csv/                      (legacy output locations, some scripts use these)
```

## Critical Developer Workflows

### Running Validation Checks
- **`check2.py`**: Analyzes individual JSON structure, counts records, shows sample fields
- **`check3.py`**: Loads Parquet, shows column types, null distributions, classification stats
- **`check4.py`**: Inspects date fields across source JSON records, lists all available fields
- **`datacheck.py`**: Lists entire container structure recursively (folders, files, sizes)

### Running ETL
- **`parquets.py`**: Main ETL → produces Parquet file in `gold/`
- **`csvdat.py`**: Alternative ETL → produces both Parquet and CSV with UTF-8-sig encoding

Both scripts follow identical transformation logic; `csvdat.py` additionally exports CSV. Choose either; both are safe to re-run (overwrite=True).

### Common Issues & Fixes
| Issue | Cause | Solution |
|-------|-------|----------|
| `ArrowNotImplementedError` | `openfda` column has nested structures Parquet can't serialize | Drop `openfda` before export (done in transform) |
| Null dates in Power BI | Date strings weren't converted to datetime | Use `pd.to_datetime(..., errors='coerce')` |
| Excel encoding issues | CSV exported as UTF-8 without BOM | Use UTF-8-sig encoding and `;` separator |

## Dependencies & Imports
- **`azure.storage.filedatalake`**: DataLakeServiceClient for hierarchical filesystem operations
- **`azure.storage.blob`**: BlobServiceClient for blob-level operations
- **`pandas`**: DataFrame manipulation, Parquet/CSV I/O
- **`pyarrow`**: Parquet engine for pandas
- **`json`**: Loading FDA API JSON responses
- **`io.BytesIO`**: In-memory buffers for upload

## Key Files to Know
- [parquets.py](parquets.py) - Primary transformation logic (reference this for ETL patterns)
- [csvdat.py](csvdat.py) - Alternative ETL with CSV export
- [check3.py](check3.py) - Best reference for understanding Parquet output schema
- [check4.py](check4.py) - Best reference for understanding JSON input schema

## When Adding New Features
- Maintain the deduplication-by-`recall_number` pattern
- Test date conversions; invalid dates should coerce to `NaT`, not fail
- Always drop `openfda` before Parquet export
- Use UTF-8-sig for CSV exports targeting Excel
- Add new scripts in the root; follow existing credential pattern but consider parameterization
