/*
=============================================================================
TASK 4: Data Validation & Testing
=============================================================================
Run this script in Azure Synapse Studio after creating External Tables.
Target: Built-in Serverless SQL Pool, Database: FoodRecallsDB
=============================================================================
*/

USE FoodRecallsDB;
GO

PRINT '============================================================';
PRINT 'TASK 4: DATA VALIDATION & TESTING';
PRINT '============================================================';
GO

-- ============================================================================
-- 1. Record Counts by Source
-- ============================================================================

PRINT '';
PRINT '1. RECORD COUNTS BY SOURCE';
PRINT '-----------------------------------------------------------';

SELECT
    [Source],
    COUNT(*) AS RecordCount
FROM dbo.fact_recalls
GROUP BY [Source]
ORDER BY RecordCount DESC;

-- Expected:
-- FDA:   ~28,000
-- FSIS:  ~1,000

-- ============================================================================
-- 2. Date Range Validation
-- ============================================================================

PRINT '';
PRINT '2. DATE RANGE VALIDATION';
PRINT '-----------------------------------------------------------';

SELECT
    [Source],
    MIN(RecallDate) AS EarliestDate,
    MAX(RecallDate) AS LatestDate,
    DATEDIFF(YEAR, MIN(RecallDate), MAX(RecallDate)) AS YearSpan
FROM dbo.fact_recalls
WHERE RecallDate IS NOT NULL
GROUP BY [Source];

-- ============================================================================
-- 3. Classification Distribution
-- ============================================================================

PRINT '';
PRINT '3. CLASSIFICATION DISTRIBUTION';
PRINT '-----------------------------------------------------------';

SELECT
    f.[Source],
    c.USAClassLevel,
    c.SeverityLevel,
    COUNT(*) AS Count,
    CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY f.[Source]) AS DECIMAL(5,2)) AS Percentage
FROM dbo.fact_recalls f
JOIN dbo.dim_classification c ON f.ClassificationKey = c.ClassificationKey
GROUP BY f.[Source], c.USAClassLevel, c.SeverityLevel
ORDER BY f.[Source], Count DESC;

-- ============================================================================
-- 4. Geographic Distribution (Top 20 States)
-- ============================================================================

PRINT '';
PRINT '4. TOP 20 STATES BY RECALL COUNT';
PRINT '-----------------------------------------------------------';

SELECT TOP 20
    g.[State],
    g.Country,
    COUNT(*) AS RecallCount,
    SUM(CASE WHEN c.USAClassLevel = 'Class I' THEN 1 ELSE 0 END) AS ClassI_Count
FROM dbo.fact_recalls f
JOIN dbo.dim_geography g ON f.GeographyKey = g.GeographyKey
JOIN dbo.dim_classification c ON f.ClassificationKey = c.ClassificationKey
WHERE g.[State] IS NOT NULL AND g.[State] != 'None'
GROUP BY g.[State], g.Country
ORDER BY RecallCount DESC;

-- ============================================================================
-- 5. Yearly Distribution
-- ============================================================================

PRINT '';
PRINT '5. YEARLY DISTRIBUTION';
PRINT '-----------------------------------------------------------';

SELECT
    d.[Year],
    f.[Source],
    COUNT(*) AS RecallCount
FROM dbo.fact_recalls f
JOIN dbo.dim_date d ON f.DateKey = d.DateKey
GROUP BY d.[Year], f.[Source]
ORDER BY d.[Year], f.[Source];

-- ============================================================================
-- 6. Product Category Distribution
-- ============================================================================

PRINT '';
PRINT '6. TOP 15 PRODUCT CATEGORIES';
PRINT '-----------------------------------------------------------';

SELECT TOP 15
    p.ProductCategory,
    COUNT(*) AS RecallCount,
    CAST(100.0 * COUNT(*) / (SELECT COUNT(*) FROM dbo.fact_recalls) AS DECIMAL(5,2)) AS Percentage
FROM dbo.fact_recalls f
JOIN dbo.dim_product p ON f.ProductKey = p.ProductKey
WHERE p.ProductCategory IS NOT NULL AND p.ProductCategory != 'None'
GROUP BY p.ProductCategory
ORDER BY RecallCount DESC;

-- ============================================================================
-- 7. Top Companies by Recall Count
-- ============================================================================

PRINT '';
PRINT '7. TOP 15 COMPANIES BY RECALL COUNT';
PRINT '-----------------------------------------------------------';

SELECT TOP 15
    comp.CompanyName,
    comp.[State],
    COUNT(*) AS RecallCount,
    SUM(CASE WHEN c.USAClassLevel = 'Class I' THEN 1 ELSE 0 END) AS ClassI_Count
FROM dbo.fact_recalls f
JOIN dbo.dim_company comp ON f.CompanyKey = comp.CompanyKey
JOIN dbo.dim_classification c ON f.ClassificationKey = c.ClassificationKey
WHERE comp.CompanyName IS NOT NULL AND comp.CompanyName != 'None'
GROUP BY comp.CompanyName, comp.[State]
ORDER BY RecallCount DESC;

-- ============================================================================
-- 8. CDC Health Impact Summary
-- ============================================================================

PRINT '';
PRINT '8. CDC NORS HEALTH IMPACT SUMMARY BY YEAR';
PRINT '-----------------------------------------------------------';

SELECT
    [Year],
    COUNT(*) AS OutbreakCount,
    SUM(Illnesses) AS TotalIllnesses,
    SUM(Hospitalizations) AS TotalHospitalizations,
    SUM(Deaths) AS TotalDeaths
FROM dbo.fact_health_impact
WHERE [Year] IS NOT NULL
GROUP BY [Year]
ORDER BY [Year];

-- ============================================================================
-- 9. Top Pathogens (CDC)
-- ============================================================================

PRINT '';
PRINT '9. TOP 15 PATHOGENS (CDC NORS)';
PRINT '-----------------------------------------------------------';

SELECT TOP 15
    Pathogen,
    COUNT(*) AS OutbreakCount,
    SUM(Illnesses) AS TotalIllnesses,
    SUM(Hospitalizations) AS TotalHospitalizations,
    SUM(Deaths) AS TotalDeaths
FROM dbo.fact_health_impact
WHERE Pathogen IS NOT NULL AND Pathogen != 'None'
GROUP BY Pathogen
ORDER BY TotalIllnesses DESC;

-- ============================================================================
-- 10. Data Quality Checks
-- ============================================================================

PRINT '';
PRINT '10. DATA QUALITY CHECKS';
PRINT '-----------------------------------------------------------';

-- Check for NULL values in key fields
SELECT
    'fact_recalls' AS TableName,
    SUM(CASE WHEN RecallID IS NULL OR RecallID = 'None' THEN 1 ELSE 0 END) AS NULL_RecallID,
    SUM(CASE WHEN RecallDate IS NULL THEN 1 ELSE 0 END) AS NULL_RecallDate,
    SUM(CASE WHEN [Source] IS NULL OR [Source] = 'None' THEN 1 ELSE 0 END) AS NULL_Source,
    COUNT(*) AS TotalRows
FROM dbo.fact_recalls;

-- Orphan keys check (should be 0)
SELECT
    'Orphan GeographyKey' AS CheckType,
    COUNT(*) AS OrphanCount
FROM dbo.fact_recalls f
WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_geography g WHERE g.GeographyKey = f.GeographyKey)

UNION ALL

SELECT
    'Orphan ClassificationKey' AS CheckType,
    COUNT(*) AS OrphanCount
FROM dbo.fact_recalls f
WHERE NOT EXISTS (SELECT 1 FROM dbo.dim_classification c WHERE c.ClassificationKey = f.ClassificationKey);

-- ============================================================================
-- 11. Monthly Trend (Recent 24 months)
-- ============================================================================

PRINT '';
PRINT '11. MONTHLY TREND (RECENT 24 MONTHS)';
PRINT '-----------------------------------------------------------';

SELECT TOP 24
    d.[Year],
    d.[Month],
    d.MonthName,
    COUNT(*) AS RecallCount
FROM dbo.fact_recalls f
JOIN dbo.dim_date d ON f.DateKey = d.DateKey
WHERE d.[Date] IS NOT NULL
GROUP BY d.[Year], d.[Month], d.MonthName
ORDER BY d.[Year] DESC, d.[Month] DESC;

-- ============================================================================
-- Summary
-- ============================================================================

PRINT '';
PRINT '============================================================';
PRINT 'TASK 4 COMPLETE: Data Validation Finished';
PRINT '============================================================';
PRINT '';
PRINT 'SUMMARY:';

SELECT
    (SELECT COUNT(*) FROM dbo.fact_recalls) AS TotalRecalls,
    (SELECT COUNT(*) FROM dbo.fact_health_impact) AS TotalOutbreaks,
    (SELECT COUNT(DISTINCT g.[State]) FROM dbo.dim_geography g WHERE g.[State] IS NOT NULL) AS UniqueStates,
    (SELECT COUNT(*) FROM dbo.dim_company) AS UniqueCompanies,
    (SELECT COUNT(*) FROM dbo.dim_product) AS UniqueProducts,
    (SELECT COUNT(DISTINCT c.USAClassLevel) FROM dbo.dim_classification c) AS ClassificationLevels;
GO
