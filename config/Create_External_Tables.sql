/*
=============================================================================
TASK 3: Synapse Serverless SQL External Tables
=============================================================================
Run this script in Azure Synapse Studio > Develop > SQL Script
Target: Built-in Serverless SQL Pool

Prerequisites:
1. Parquet files uploaded to Azure Data Lake Gen2 (gold/ folder)
2. Storage Account: datafactory123999
3. Container: raw
4. Folder: gold/

Storage Account Details (from AZURE_SOLUTION_DOKU.md):
- Name: datafactory123999
- URL: https://datafactory123999.dfs.core.windows.net/
=============================================================================
*/

-- ============================================================================
-- PART 1: Database Setup
-- ============================================================================

-- Create database if not exists
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'FoodRecallsDB')
BEGIN
    CREATE DATABASE FoodRecallsDB;
END
GO

USE FoodRecallsDB;
GO

-- ============================================================================
-- PART 2: External Data Source and File Format
-- ============================================================================

-- Create Master Key (only if not exists)
IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
BEGIN
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'FoodRecalls2024!Secure';
END
GO

-- Create Database Scoped Credential for Managed Identity
IF NOT EXISTS (SELECT * FROM sys.database_scoped_credentials WHERE name = 'DataLakeCredential')
BEGIN
    CREATE DATABASE SCOPED CREDENTIAL DataLakeCredential
    WITH IDENTITY = 'Managed Identity';
END
GO

-- Create External Data Source pointing to gold folder
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'GoldDataLake')
BEGIN
    CREATE EXTERNAL DATA SOURCE GoldDataLake
    WITH (
        LOCATION = 'abfss://raw@datafactory123999.dfs.core.windows.net/gold',
        CREDENTIAL = DataLakeCredential
    );
END
GO

-- Create External File Format for Parquet
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'ParquetFormat')
BEGIN
    CREATE EXTERNAL FILE FORMAT ParquetFormat
    WITH (
        FORMAT_TYPE = PARQUET,
        DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
    );
END
GO

-- ============================================================================
-- PART 3: External Tables - Dimension Tables
-- ============================================================================

-- Drop existing external tables if they exist
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

-- Dim_Date
CREATE EXTERNAL TABLE dbo.dim_date (
    DateKey INT,
    [Date] VARCHAR(10),  -- String format YYYY-MM-DD for Parquet compatibility
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

-- Dim_Product (HazardCategory removed - inconsistent data quality)
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
-- PART 4: External Tables - Fact Tables
-- ============================================================================

-- Fact_Recalls
-- Note: RecallDate as VARCHAR because Parquet stores as BYTE_ARRAY (string)
-- Note: OriginGeographyKey as FLOAT because Pandas stores nullable INT as float64
CREATE EXTERNAL TABLE dbo.fact_recalls (
    RecallKey INT,
    RecallID NVARCHAR(50),
    EventID NVARCHAR(50),  -- Groups multiple products into one event (FDA: event_id, others: same as RecallID)
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

-- Fact_Yearly_Summary (Aggregated recalls per year per source)
IF OBJECT_ID('dbo.fact_yearly_summary', 'U') IS NOT NULL
    DROP EXTERNAL TABLE dbo.fact_yearly_summary;
GO

CREATE EXTERNAL TABLE dbo.fact_yearly_summary (
    YearlySummaryKey INT,
    [Year] INT,
    [Source] NVARCHAR(20),
    RecallCount INT,
    PoundsRecalled BIGINT  -- INT64 in Parquet, only available for FSIS
)
WITH (
    LOCATION = 'fact_yearly_summary.parquet',
    DATA_SOURCE = GoldDataLake,
    FILE_FORMAT = ParquetFormat
);
GO

-- Fact_FSIS_Species (USDA FSIS meat recalls by species)
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

-- Fact_Adverse_Events (FDA CAERS - food-related adverse event reports)
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
-- PART 5: Views for Power BI
-- ============================================================================

-- Drop existing views
IF OBJECT_ID('dbo.vw_recalls_analysis', 'V') IS NOT NULL DROP VIEW dbo.vw_recalls_analysis;
IF OBJECT_ID('dbo.vw_health_impact_analysis', 'V') IS NOT NULL DROP VIEW dbo.vw_health_impact_analysis;
GO

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
-- PART 6: Verification Queries
-- ============================================================================

-- Test queries to verify data
PRINT 'Verifying External Tables...';
GO

SELECT 'dim_date' AS TableName, COUNT(*) AS RowCount FROM dbo.dim_date;
SELECT 'dim_geography' AS TableName, COUNT(*) AS RowCount FROM dbo.dim_geography;
SELECT 'dim_classification' AS TableName, COUNT(*) AS RowCount FROM dbo.dim_classification;
SELECT 'dim_product' AS TableName, COUNT(*) AS RowCount FROM dbo.dim_product;
SELECT 'dim_company' AS TableName, COUNT(*) AS RowCount FROM dbo.dim_company;
SELECT 'fact_recalls' AS TableName, COUNT(*) AS RowCount FROM dbo.fact_recalls;
SELECT 'fact_health_impact' AS TableName, COUNT(*) AS RowCount FROM dbo.fact_health_impact;
SELECT 'fact_yearly_summary' AS TableName, COUNT(*) AS RowCount FROM dbo.fact_yearly_summary;
SELECT 'fact_fsis_species' AS TableName, COUNT(*) AS RowCount FROM dbo.fact_fsis_species;
SELECT 'fact_adverse_events' AS TableName, COUNT(*) AS RowCount FROM dbo.fact_adverse_events;
GO

-- Sample data from views
SELECT TOP 10 * FROM dbo.vw_recalls_analysis ORDER BY RecallDate DESC;
SELECT TOP 10 * FROM dbo.vw_health_impact_analysis ORDER BY [Year] DESC, [Month] DESC;
GO

PRINT 'TASK 3 COMPLETE: External Tables and Views created successfully!';
GO
