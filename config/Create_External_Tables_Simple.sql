/*
=============================================================================
TASK 3: Synapse Serverless SQL External Tables (SIMPLIFIED)
=============================================================================
Run each section separately in Azure Synapse Studio
=============================================================================
*/

-- STEP 1: Use the database
USE FoodRecallsDB;
GO

-- STEP 2: Check if Data Source exists, if not create it
-- Run this first to see what exists:
SELECT * FROM sys.external_data_sources;
SELECT * FROM sys.external_file_formats;
GO

-- STEP 3: Create External Data Source (skip if already exists from previous run)
-- Uncomment only if the query above shows no 'GoldDataLake' entry
/*
CREATE EXTERNAL DATA SOURCE GoldDataLake
WITH (
    LOCATION = 'abfss://raw@datafactory123999.dfs.core.windows.net/gold'
);
*/
GO

-- STEP 4: Create External File Format (skip if already exists)
-- Uncomment only if the query above shows no 'ParquetFormat' entry
/*
CREATE EXTERNAL FILE FORMAT ParquetFormat
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);
*/
GO

-- =============================================================================
-- STEP 5: Create External Tables one by one
-- Run each CREATE statement separately
-- =============================================================================

-- 5a: dim_date
CREATE EXTERNAL TABLE dbo.dim_date (
    DateKey INT,
    [Date] DATE,
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

-- 5b: dim_geography
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

-- 5c: dim_classification
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

-- 5d: dim_product (HazardCategory removed)
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

-- 5e: dim_company
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

-- 5f: fact_recalls
CREATE EXTERNAL TABLE dbo.fact_recalls (
    RecallKey INT,
    RecallID NVARCHAR(50),
    RecallDate DATE,
    [Source] NVARCHAR(10),
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
    LOCATION = 'fact_recalls.parquet',
    DATA_SOURCE = GoldDataLake,
    FILE_FORMAT = ParquetFormat
);
GO

-- 5g: fact_health_impact
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

-- 5h: fact_yearly_summary
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

-- 5i: fact_fsis_species
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

-- 5j: fact_adverse_events
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

-- =============================================================================
-- STEP 6: Verify tables were created
-- =============================================================================
SELECT name, type_desc FROM sys.tables WHERE type = 'U';
GO

-- STEP 7: Test query
SELECT 'dim_date' AS TableName, COUNT(*) AS RecordCount FROM dbo.dim_date
UNION ALL
SELECT 'dim_geography', COUNT(*) FROM dbo.dim_geography
UNION ALL
SELECT 'dim_classification', COUNT(*) FROM dbo.dim_classification
UNION ALL
SELECT 'dim_product', COUNT(*) FROM dbo.dim_product
UNION ALL
SELECT 'dim_company', COUNT(*) FROM dbo.dim_company
UNION ALL
SELECT 'fact_recalls', COUNT(*) FROM dbo.fact_recalls
UNION ALL
SELECT 'fact_health_impact', COUNT(*) FROM dbo.fact_health_impact
UNION ALL
SELECT 'fact_yearly_summary', COUNT(*) FROM dbo.fact_yearly_summary
UNION ALL
SELECT 'fact_fsis_species', COUNT(*) FROM dbo.fact_fsis_species
UNION ALL
SELECT 'fact_adverse_events', COUNT(*) FROM dbo.fact_adverse_events;
GO
