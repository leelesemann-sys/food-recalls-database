# TASK 5: Power BI Dataset Setup Guide

## Overview
This guide explains how to connect Power BI to the Food Recall Analytics Star Schema.

## Connection Options

### Option 1: Connect to Synapse Serverless SQL Pool (Recommended for Production)

1. **Open Power BI Desktop**
2. **Get Data** → **Azure** → **Azure Synapse Analytics SQL**
3. **Enter connection details:**
   - Server: `foodrecalls-synapse-ondemand.sql.azuresynapse.net`
   - Database: `FoodRecallsDB`
4. **Data Connectivity Mode:** DirectQuery (recommended for large datasets)
5. **Select tables/views:**
   - Use `vw_recalls_analysis` for recalls analysis
   - Use `vw_health_impact_analysis` for CDC outbreak analysis

### Option 2: Connect to Local Parquet Files (For Development)

1. **Open Power BI Desktop**
2. **Get Data** → **Files** → **Parquet**
3. **Navigate to:** `files_parquet/` folder
4. **Load all Parquet files:**
   - fact_recalls.parquet
   - fact_health_impact.parquet
   - dim_date.parquet
   - dim_geography.parquet
   - dim_classification.parquet
   - dim_product.parquet
   - dim_company.parquet

---

## Data Model Relationships

Create the following relationships (all Many-to-One):

| From Table | From Column | To Table | To Column | Cardinality |
|------------|-------------|----------|-----------|-------------|
| fact_recalls | GeographyKey | dim_geography | GeographyKey | Many:1 |
| fact_recalls | ClassificationKey | dim_classification | ClassificationKey | Many:1 |
| fact_recalls | ProductKey | dim_product | ProductKey | Many:1 |
| fact_recalls | CompanyKey | dim_company | CompanyKey | Many:1 |
| fact_recalls | DateKey | dim_date | DateKey | Many:1 |
| fact_health_impact | DateKey | dim_date | DateKey | Many:1 |

---

## DAX Measures

### Copy these measures into Power BI:

```dax
// ============================================================
// RECALL METRICS
// ============================================================

Total Recalls =
COUNTROWS(fact_recalls)

USA Recalls =
CALCULATE(
    COUNTROWS(fact_recalls),
    fact_recalls[Source] IN {"FDA", "FSIS"}
)

FDA Recalls =
CALCULATE(
    COUNTROWS(fact_recalls),
    fact_recalls[Source] = "FDA"
)

FSIS Recalls =
CALCULATE(
    COUNTROWS(fact_recalls),
    fact_recalls[Source] = "FSIS"
)

// ============================================================
// SEVERITY METRICS
// ============================================================

Class I Recalls =
CALCULATE(
    COUNTROWS(fact_recalls),
    dim_classification[USAClassLevel] = "Class I"
)

Class II Recalls =
CALCULATE(
    COUNTROWS(fact_recalls),
    dim_classification[USAClassLevel] = "Class II"
)

Class III Recalls =
CALCULATE(
    COUNTROWS(fact_recalls),
    dim_classification[USAClassLevel] = "Class III"
)

High Severity % =
DIVIDE(
    [Class I Recalls],
    [Total Recalls],
    0
)

// ============================================================
// TIME INTELLIGENCE
// ============================================================

Recalls YoY Growth =
VAR CurrentYear = [Total Recalls]
VAR PreviousYear =
    CALCULATE(
        [Total Recalls],
        SAMEPERIODLASTYEAR(dim_date[Date])
    )
RETURN
    DIVIDE(CurrentYear - PreviousYear, PreviousYear, 0)

Recalls MTD =
CALCULATE(
    [Total Recalls],
    DATESMTD(dim_date[Date])
)

Recalls YTD =
CALCULATE(
    [Total Recalls],
    DATESYTD(dim_date[Date])
)

Average Monthly Recalls =
AVERAGEX(
    VALUES(dim_date[Year]),
    CALCULATE([Total Recalls]) / 12
)

// ============================================================
// CDC HEALTH IMPACT METRICS
// ============================================================

Total Outbreaks =
COUNTROWS(fact_health_impact)

Total Illnesses =
SUM(fact_health_impact[Illnesses])

Total Hospitalizations =
SUM(fact_health_impact[Hospitalizations])

Total Deaths =
SUM(fact_health_impact[Deaths])

Hospitalization Rate =
DIVIDE(
    [Total Hospitalizations],
    [Total Illnesses],
    0
)

Mortality Rate =
DIVIDE(
    [Total Deaths],
    [Total Illnesses],
    0
)

Avg Illnesses per Outbreak =
DIVIDE(
    [Total Illnesses],
    [Total Outbreaks],
    0
)

// ============================================================
// GEOGRAPHIC METRICS
// ============================================================

Recalls by Top State =
CALCULATE(
    [Total Recalls],
    TOPN(1, ALL(dim_geography[State]), [Total Recalls])
)

States with Recalls =
DISTINCTCOUNT(
    CALCULATETABLE(
        VALUES(dim_geography[State]),
        fact_recalls
    )
)

// ============================================================
// PRODUCT METRICS
// ============================================================

Product Categories Affected =
DISTINCTCOUNT(
    CALCULATETABLE(
        VALUES(dim_product[ProductCategory]),
        fact_recalls
    )
)

Most Common Category =
FIRSTNONBLANK(
    TOPN(
        1,
        VALUES(dim_product[ProductCategory]),
        [Total Recalls]
    ),
    1
)

// ============================================================
// COMPANY METRICS
// ============================================================

Companies with Recalls =
DISTINCTCOUNT(fact_recalls[CompanyKey])

Repeat Offenders =
COUNTROWS(
    FILTER(
        SUMMARIZE(
            fact_recalls,
            fact_recalls[CompanyKey],
            "RecallCount", [Total Recalls]
        ),
        [RecallCount] > 1
    )
)
```

---

## Suggested Visualizations

### Page 1: Executive Summary
- **Card:** Total Recalls, Class I %, YoY Growth
- **Line Chart:** Recalls by Year (FDA vs FSIS)
- **Donut Chart:** Classification Distribution
- **Map:** Recalls by State

### Page 2: Recall Details
- **Table:** Top 10 Companies by Recall Count
- **Bar Chart:** Product Categories
- **Treemap:** Reason for Recall (word frequency)
- **Slicer:** Year, Source, Classification

### Page 3: Health Impact (CDC)
- **Card:** Total Outbreaks, Illnesses, Deaths
- **Line Chart:** Outbreaks by Year
- **Bar Chart:** Top Pathogens
- **Table:** Food Vehicles by Illness Count

### Page 4: Geographic Analysis
- **Map:** Recalls by State (bubble size = count)
- **Bar Chart:** Top 15 States
- **Matrix:** State x Year

---

## Refresh Schedule

For DirectQuery:
- No refresh needed (real-time)

For Import Mode:
- Schedule daily refresh
- Time: After Data Factory pipeline completes (e.g., 6:00 AM)

---

## Performance Tips

1. **Use DirectQuery** for datasets > 100MB
2. **Limit visuals** to 10 per page
3. **Avoid DISTINCT counts** on large tables when possible
4. **Use aggregations** for frequently used metrics
5. **Enable query folding** by using native SQL views

---

## Troubleshooting

### "Cannot connect to Synapse"
- Verify firewall allows Power BI IP ranges
- Check Managed Identity permissions on Storage Account

### "Query timeout"
- Increase timeout in Options → Data Load
- Use smaller date ranges with slicers
- Consider switching to Import mode

### "Refresh failed"
- Verify credentials are current
- Check Synapse workspace is running
- Verify Parquet files exist in gold/ folder
