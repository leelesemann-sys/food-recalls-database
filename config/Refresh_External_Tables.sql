/*
=============================================================================
REFRESH External Tables - Azure Synapse Serverless SQL
=============================================================================
Run this script in Azure Synapse Studio after uploading new parquet files.
This drops and recreates all external tables to reflect schema changes.

Last updated: 2026-01-23
Changes:
- RASFF filtered to food only (excluded feed & food contact materials)
- HazardCategory removed from dim_product
- fact_recalls reduced from ~94k to ~87k rows

Target Database: FoodRecallsDB
Storage: abfss://raw@datafactory123999.dfs.core.windows.net/gold/
=============================================================================
*/

USE FoodRecallsDB;
GO

-- ============================================================================
-- STEP 1: Drop existing views first (they depend on tables)
-- ============================================================================
PRINT 'Dropping existing views...';
IF OBJECT_ID('dbo.vw_recalls_analysis', 'V') IS NOT NULL DROP VIEW dbo.vw_recalls_analysis;
IF OBJECT_ID('dbo.vw_health_impact_analysis', 'V') IS NOT NULL DROP VIEW dbo.vw_health_impact_analysis;
GO

-- ============================================================================
-- STEP 2: Drop all external tables
-- ============================================================================
PRINT 'Dropping existing external tables...';
IF OBJECT_ID('dbo.dim_date', 'U') IS NOT NULL DROP EXTERNAL TABLE dbo.dim_date;
IF OBJECT_ID('dbo.dim_geography', 'U') IS NOT NULL DROP EXTERNAL TABLE dbo.dim_geography;
IF OBJECT_ID('dbo.dim_classification', 'U') IS NOT NULL DROP EXTERNAL TABLE dbo.dim_classification;
IF OBJECT_ID('dbo.dim_product', 'U') IS NOT NULL DROP EXTERNAL TABLE dbo.dim_product;
IF OBJECT_ID('dbo.dim_company', 'U') IS NOT NULL DROP EXTERNAL TABLE dbo.dim_company;
IF OBJECT_ID('dbo.fact_recalls', 'U') IS NOT NULL DROP EXTERNAL TABLE dbo.fact_recalls;
IF OBJECT_ID('dbo.fact_health_impact', 'U') IS NOT NULL DROP EXTERNAL TABLE dbo.fact_health_impact;
IF OBJECT_ID('dbo.fact_yearly_summary', 'U') IS NOT NULL DROP EXTERNAL TABLE dbo.fact_yearly_summary;
IF OBJECT_ID('dbo.fact_fsis_species', 'U') IS NOT NULL DROP EXTERNAL TABLE dbo.fact_fsis_species;
IF OBJECT_ID('dbo.fact_adverse_events', 'U') IS NOT NULL DROP EXTERNAL TABLE dbo.fact_adverse_events;
GO

-- ============================================================================
-- STEP 3: Create Dimension Tables
-- ============================================================================
PRINT 'Creating dimension tables...';

-- Dim_Date
CREATE EXTERNAL TABLE dbo.dim_date (
    DateKey INT,
    [Date] VARCHAR(10),
    [Year] INT,
    [Quarter] INT,
    [Month] INT,
    MonthName NVARCHAR(20),
    [Day] INT,
    DayOfWeek INT,
    DayName NVARCHAR(20),
    WeekOfYear INT
)
WITH (
    LOCATION = 'dim_date.parquet',
    DATA_SOURCE = GoldDataLake,
    FILE_FORMAT = ParquetFormat
);
GO

-- Dim_Geography
CREATE EXTERNAL TABLE dbo.dim_geography (
    GeographyKey INT,
    Country NVARCHAR(100),
    CountryCode NVARCHAR(10),
    [State] NVARCHAR(100),
    Region NVARCHAR(50),
    IsEUMember BIT,
    IsEFTA BIT
)
WITH (
    LOCATION = 'dim_geography.parquet',
    DATA_SOURCE = GoldDataLake,
    FILE_FORMAT = ParquetFormat
);
GO

-- Dim_Classification
CREATE EXTERNAL TABLE dbo.dim_classification (
    ClassificationKey INT,
    [Source] NVARCHAR(10),
    OriginalClassification NVARCHAR(100),
    USAClassLevel NVARCHAR(20),
    IsPublicHealthAlert BIT,
    NotificationType NVARCHAR(50),
    RiskDecision NVARCHAR(30),
    SeverityLevel NVARCHAR(20),
    SeverityScore INT
)
WITH (
    LOCATION = 'dim_classification.parquet',
    DATA_SOURCE = GoldDataLake,
    FILE_FORMAT = ParquetFormat
);
GO

-- Dim_Product (HazardCategory removed)
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

-- Dim_Company
CREATE EXTERNAL TABLE dbo.dim_company (
    CompanyKey INT,
    CompanyName NVARCHAR(200),
    City NVARCHAR(100),
    [State] NVARCHAR(100),
    Country NVARCHAR(100),
    EstablishmentNumber INT
)
WITH (
    LOCATION = 'dim_company.parquet',
    DATA_SOURCE = GoldDataLake,
    FILE_FORMAT = ParquetFormat
);
GO

-- ============================================================================
-- STEP 4: Create Fact Tables
-- ============================================================================
PRINT 'Creating fact tables...';

-- Fact_Recalls
CREATE EXTERNAL TABLE dbo.fact_recalls (
    RecallKey INT,
    RecallID NVARCHAR(50),
    EventID NVARCHAR(50),
    RecallDate VARCHAR(50),
    [Source] NVARCHAR(10),
    GeographyKey INT,
    OriginGeographyKey FLOAT,
    ClassificationKey INT,
    ProductKey INT,
    CompanyKey INT,
    DateKey INT,
    ReasonForRecall NVARCHAR(500),
    RecallCategory NVARCHAR(50),
    RecallGroup NVARCHAR(100),
    RecallSubgroup NVARCHAR(100),
    DistributionScope NVARCHAR(200),
    ActionTaken NVARCHAR(200),
    Illnesses INT,
    Hospitalizations INT,
    Deaths INT
)
WITH (
    LOCATION = 'fact_recalls.parquet',
    DATA_SOURCE = GoldDataLake,
    FILE_FORMAT = ParquetFormat
);
GO

-- Fact_Health_Impact (CDC NORS)
CREATE EXTERNAL TABLE dbo.fact_health_impact (
    HealthImpactKey INT,
    OutbreakID NVARCHAR(50),
    [Year] INT,
    [Month] INT,
    DateKey INT,
    [State] NVARCHAR(50),
    Illnesses INT,
    Hospitalizations INT,
    Deaths INT,
    Pathogen NVARCHAR(200),
    Serotype NVARCHAR(200),
    FoodVehicle NVARCHAR(200),
    IFSACCategory NVARCHAR(200),
    Setting NVARCHAR(200),
    PrimaryMode NVARCHAR(100)
)
WITH (
    LOCATION = 'fact_health_impact.parquet',
    DATA_SOURCE = GoldDataLake,
    FILE_FORMAT = ParquetFormat
);
GO

-- Fact_Yearly_Summary
CREATE EXTERNAL TABLE dbo.fact_yearly_summary (
    YearlySummaryKey INT,
    [Year] INT,
    [Source] NVARCHAR(20),
    RecallCount INT,
    PoundsRecalled BIGINT  -- INT64 in Parquet
)
WITH (
    LOCATION = 'fact_yearly_summary.parquet',
    DATA_SOURCE = GoldDataLake,
    FILE_FORMAT = ParquetFormat
);
GO

-- Fact_FSIS_Species (USDA meat recalls by species)
CREATE EXTERNAL TABLE dbo.fact_fsis_species (
    FsisSpeciesKey INT,
    [Year] INT,
    Species NVARCHAR(100),
    RecallCount INT,
    PoundsRecalled FLOAT  -- Nullable int in Pandas = float64
)
WITH (
    LOCATION = 'fact_fsis_species.parquet',
    DATA_SOURCE = GoldDataLake,
    FILE_FORMAT = ParquetFormat
);
GO

-- Fact_Adverse_Events (FDA CAERS)
CREATE EXTERNAL TABLE dbo.fact_adverse_events (
    AdverseEventKey INT,
    ReportNumber NVARCHAR(50),
    DateKey FLOAT,  -- Nullable INT in Pandas = float64
    [Year] INT,
    [Month] INT,
    IndustryCode NVARCHAR(20),
    IndustryCategory NVARCHAR(200),
    ProductType NVARCHAR(50),
    ProductName NVARCHAR(500),
    ConsumerAge FLOAT,  -- Nullable int in Pandas = float64
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
-- STEP 5: Create Views for Power BI
-- ============================================================================
PRINT 'Creating views...';

-- Recalls Analysis View (Star Schema denormalized)
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

    -- Product
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

-- Health Impact Analysis View (CDC NORS)
CREATE VIEW dbo.vw_health_impact_analysis AS
SELECT
    h.HealthImpactKey,
    h.OutbreakID,
    h.[Year],
    h.[Month],
    h.[State],
    h.Illnesses,
    h.Hospitalizations,
    h.Deaths,
    h.Pathogen,
    h.Serotype,
    h.FoodVehicle,
    h.IFSACCategory,
    h.Setting,
    h.PrimaryMode,

    -- Date Details
    d.[Quarter],
    d.MonthName,
    d.WeekOfYear

FROM dbo.fact_health_impact h
LEFT JOIN dbo.dim_date d ON h.DateKey = d.DateKey;
GO

-- ============================================================================
-- STEP 6: Verification
-- ============================================================================
PRINT 'Verifying tables...';

SELECT 'dim_date' AS TableName, COUNT(*) AS RowCount FROM dbo.dim_date
UNION ALL SELECT 'dim_geography', COUNT(*) FROM dbo.dim_geography
UNION ALL SELECT 'dim_classification', COUNT(*) FROM dbo.dim_classification
UNION ALL SELECT 'dim_product', COUNT(*) FROM dbo.dim_product
UNION ALL SELECT 'dim_company', COUNT(*) FROM dbo.dim_company
UNION ALL SELECT 'fact_recalls', COUNT(*) FROM dbo.fact_recalls
UNION ALL SELECT 'fact_health_impact', COUNT(*) FROM dbo.fact_health_impact
UNION ALL SELECT 'fact_yearly_summary', COUNT(*) FROM dbo.fact_yearly_summary
UNION ALL SELECT 'fact_fsis_species', COUNT(*) FROM dbo.fact_fsis_species
UNION ALL SELECT 'fact_adverse_events', COUNT(*) FROM dbo.fact_adverse_events
ORDER BY TableName;
GO

-- Quick validation: RASFF should now only have food records
SELECT
    [Source],
    COUNT(*) AS RecallCount
FROM dbo.fact_recalls
GROUP BY [Source]
ORDER BY RecallCount DESC;
GO

PRINT 'REFRESH COMPLETE!';
PRINT 'Expected row counts:';
PRINT '  fact_recalls: ~86,796 (was ~94,000 before RASFF filter)';
PRINT '  fact_adverse_events: ~108,000 (food only, no cosmetics)';
GO
