/*
=============================================================================
MINIMAL Schema Update - Azure Synapse Serverless SQL
=============================================================================
Only recreates tables/views where the SCHEMA changed.
Data changes are automatic (External Tables read from Parquet files).

Changes:
- dim_product: HazardCategory column removed
- vw_recalls_analysis: Updated to not reference HazardCategory

Run in: Azure Synapse Studio > FoodRecallsDB
=============================================================================
*/

USE FoodRecallsDB;
GO

-- ============================================================================
-- STEP 1: Drop view first (depends on dim_product)
-- ============================================================================
PRINT 'Dropping view that references HazardCategory...';
IF OBJECT_ID('dbo.vw_recalls_analysis', 'V') IS NOT NULL
    DROP VIEW dbo.vw_recalls_analysis;
GO

-- ============================================================================
-- STEP 2: Recreate dim_product (schema changed)
-- ============================================================================
PRINT 'Recreating dim_product (HazardCategory removed)...';
IF OBJECT_ID('dbo.dim_product', 'U') IS NOT NULL
    DROP EXTERNAL TABLE dbo.dim_product;
GO

CREATE EXTERNAL TABLE dbo.dim_product (
    ProductKey INT,
    ProductName NVARCHAR(500),
    ProductCategory NVARCHAR(100),
    ProductType NVARCHAR(50)
)
WITH (
    LOCATION = 'dim_product.parquet',
    DATA_SOURCE = GoldDataLake,
    FILE_FORMAT = ParquetFormat
);
GO

-- ============================================================================
-- STEP 3: Recreate the view (without HazardCategory)
-- ============================================================================
PRINT 'Recreating vw_recalls_analysis...';
GO

CREATE VIEW dbo.vw_recalls_analysis AS
SELECT
    f.RecallKey,
    f.RecallID,
    f.RecallDate,
    f.[Source],

    -- Geography (Recall Location)
    g.Country,
    g.CountryCode,
    g.[State],
    g.Region,
    g.IsEUMember,
    g.IsEFTA,

    -- Origin Geography (Product Origin)
    og.Country AS OriginCountry,
    og.Region AS OriginRegion,

    -- Classification
    c.OriginalClassification,
    c.USAClassLevel,
    c.IsPublicHealthAlert,
    c.NotificationType,
    c.RiskDecision,
    c.SeverityLevel,
    c.SeverityScore,

    -- Recall Reason Classification (3-level hierarchy)
    f.RecallCategory,
    f.RecallGroup,
    f.RecallSubgroup,

    -- Product (HazardCategory removed)
    p.ProductName,
    p.ProductCategory,
    p.ProductType,

    -- Company
    comp.CompanyName,
    comp.City AS CompanyCity,
    comp.[State] AS CompanyState,

    -- Recall Details
    f.ReasonForRecall,
    f.DistributionScope,
    f.ActionTaken,
    f.Illnesses,
    f.Hospitalizations,
    f.Deaths,

    -- Date Details
    d.[Year],
    d.[Quarter],
    d.[Month],
    d.MonthName,
    d.WeekOfYear

FROM dbo.fact_recalls f
LEFT JOIN dbo.dim_geography g ON f.GeographyKey = g.GeographyKey
LEFT JOIN dbo.dim_geography og ON f.OriginGeographyKey = og.GeographyKey
LEFT JOIN dbo.dim_classification c ON f.ClassificationKey = c.ClassificationKey
LEFT JOIN dbo.dim_product p ON f.ProductKey = p.ProductKey
LEFT JOIN dbo.dim_company comp ON f.CompanyKey = comp.CompanyKey
LEFT JOIN dbo.dim_date d ON f.DateKey = d.DateKey;
GO

-- ============================================================================
-- STEP 4: Verify
-- ============================================================================
PRINT 'Verifying...';

SELECT 'dim_product' AS TableName, COUNT(*) AS RecordCount FROM dbo.dim_product;
SELECT 'fact_recalls (RASFF filtered)' AS TableName, COUNT(*) AS RecordCount FROM dbo.fact_recalls;
GO

-- Check RASFF reduction worked
SELECT [Source], COUNT(*) AS RecallCount
FROM dbo.fact_recalls
GROUP BY [Source]
ORDER BY RecallCount DESC;
GO

-- ============================================================================
-- STEP 5: Fix fact_yearly_summary data type (FLOAT -> BIGINT)
-- ============================================================================
PRINT 'Fixing fact_yearly_summary...';
IF OBJECT_ID('dbo.fact_yearly_summary', 'U') IS NOT NULL
    DROP EXTERNAL TABLE dbo.fact_yearly_summary;
GO

CREATE EXTERNAL TABLE dbo.fact_yearly_summary (
    YearlySummaryKey INT,
    [Year] INT,
    [Source] NVARCHAR(20),
    RecallCount INT,
    PoundsRecalled BIGINT  -- Changed from FLOAT to BIGINT
)
WITH (
    LOCATION = 'fact_yearly_summary.parquet',
    DATA_SOURCE = GoldDataLake,
    FILE_FORMAT = ParquetFormat
);
GO

-- ============================================================================
-- STEP 6: Fix dim_company data type (NVARCHAR -> INT)
-- ============================================================================
PRINT 'Fixing dim_company...';
IF OBJECT_ID('dbo.dim_company', 'U') IS NOT NULL
    DROP EXTERNAL TABLE dbo.dim_company;
GO

CREATE EXTERNAL TABLE dbo.dim_company (
    CompanyKey INT,
    CompanyName NVARCHAR(200),
    City NVARCHAR(100),
    [State] NVARCHAR(100),
    Country NVARCHAR(100),
    EstablishmentNumber INT  -- Changed from NVARCHAR(50) to INT
)
WITH (
    LOCATION = 'dim_company.parquet',
    DATA_SOURCE = GoldDataLake,
    FILE_FORMAT = ParquetFormat
);
GO

-- ============================================================================
-- STEP 7: Fix fact_adverse_events data type (INT -> FLOAT for nullable DateKey)
-- ============================================================================
PRINT 'Fixing fact_adverse_events...';
IF OBJECT_ID('dbo.fact_adverse_events', 'U') IS NOT NULL
    DROP EXTERNAL TABLE dbo.fact_adverse_events;
GO

CREATE EXTERNAL TABLE dbo.fact_adverse_events (
    AdverseEventKey INT,
    ReportNumber NVARCHAR(50),
    DateKey FLOAT,  -- Changed from INT to FLOAT (nullable int in Pandas = float64)
    [Year] INT,
    [Month] INT,
    IndustryCode NVARCHAR(20),
    IndustryCategory NVARCHAR(200),
    ProductType NVARCHAR(50),
    ProductName NVARCHAR(500),
    ConsumerAge INT,
    ConsumerGender NVARCHAR(20),
    HasHospitalization BIT,
    HasEmergencyRoom BIT,
    HasDeath BIT,
    HasLifeThreatening BIT,
    HasDisability BIT,
    HasAllergicReaction BIT,
    HasHealthcareVisit BIT,
    ReactionCount INT,
    OutcomeCount INT
)
WITH (
    LOCATION = 'fact_adverse_events.parquet',
    DATA_SOURCE = GoldDataLake,
    FILE_FORMAT = ParquetFormat
);
GO

-- ============================================================================
-- STEP 8: Fix fact_fsis_species data type (BIGINT -> FLOAT for nullable PoundsRecalled)
-- ============================================================================
PRINT 'Fixing fact_fsis_species...';
IF OBJECT_ID('dbo.fact_fsis_species', 'U') IS NOT NULL
    DROP EXTERNAL TABLE dbo.fact_fsis_species;
GO

CREATE EXTERNAL TABLE dbo.fact_fsis_species (
    FsisSpeciesKey INT,
    [Year] INT,
    Species NVARCHAR(100),
    RecallCount INT,
    PoundsRecalled FLOAT  -- Changed from BIGINT to FLOAT (nullable int in Pandas = float64)
)
WITH (
    LOCATION = 'fact_fsis_species.parquet',
    DATA_SOURCE = GoldDataLake,
    FILE_FORMAT = ParquetFormat
);
GO

-- ============================================================================
-- STEP 9: Fix fact_adverse_events ConsumerAge (INT -> FLOAT for nullable)
-- ============================================================================
PRINT 'Fixing fact_adverse_events ConsumerAge...';
IF OBJECT_ID('dbo.fact_adverse_events', 'U') IS NOT NULL
    DROP EXTERNAL TABLE dbo.fact_adverse_events;
GO

CREATE EXTERNAL TABLE dbo.fact_adverse_events (
    AdverseEventKey INT,
    ReportNumber NVARCHAR(50),
    DateKey FLOAT,  -- Nullable int in Pandas = float64
    [Year] INT,
    [Month] INT,
    IndustryCode NVARCHAR(20),
    IndustryCategory NVARCHAR(200),
    ProductType NVARCHAR(50),
    ProductName NVARCHAR(500),
    ConsumerAge FLOAT,  -- Changed from INT to FLOAT (nullable int in Pandas = float64)
    ConsumerGender NVARCHAR(20),
    HasHospitalization BIT,
    HasEmergencyRoom BIT,
    HasDeath BIT,
    HasLifeThreatening BIT,
    HasDisability BIT,
    HasAllergicReaction BIT,
    HasHealthcareVisit BIT,
    ReactionCount INT,
    OutcomeCount INT
)
WITH (
    LOCATION = 'fact_adverse_events.parquet',
    DATA_SOURCE = GoldDataLake,
    FILE_FORMAT = ParquetFormat
);
GO

PRINT 'MINIMAL UPDATE COMPLETE!';
GO
