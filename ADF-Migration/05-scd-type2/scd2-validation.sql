-- =============================================
-- SCD Type 2 Validation Queries
-- Verify correctness of DimCustomer after SCD2 processing
-- Use after running the ADF data flow df_SCD2_CustomerDimension
-- =============================================

USE BrightfieldDW;
GO

-- =============================================
-- VALIDATION 1: No Overlapping Date Ranges
-- Each CustomerID should have non-overlapping [StartDate, EndDate) intervals.
-- Any rows returned indicate a data integrity problem.
-- =============================================
PRINT '=== Validation 1: Overlapping Date Ranges ===';

SELECT
    a.CustomerID,
    a.CustomerKey AS KeyA,
    a.StartDate AS StartA,
    a.EndDate AS EndA,
    b.CustomerKey AS KeyB,
    b.StartDate AS StartB,
    b.EndDate AS EndB
FROM dw.DimCustomer a
INNER JOIN dw.DimCustomer b
    ON a.CustomerID = b.CustomerID
    AND a.CustomerKey < b.CustomerKey
WHERE
    a.StartDate < ISNULL(b.EndDate, '9999-12-31')
    AND b.StartDate < ISNULL(a.EndDate, '9999-12-31');

-- Result: Should return 0 rows if no overlapping ranges exist.

-- =============================================
-- VALIDATION 2: IsCurrent Flag Consistency
-- Each CustomerID must have exactly one row with IsCurrent = 1.
-- Returns customers with zero or more than one current row.
-- =============================================
PRINT '=== Validation 2: IsCurrent Flag Consistency ===';

SELECT
    CustomerID,
    SUM(CAST(IsCurrent AS INT)) AS CurrentRowCount,
    CASE
        WHEN SUM(CAST(IsCurrent AS INT)) = 0 THEN 'ERROR: No current row'
        WHEN SUM(CAST(IsCurrent AS INT)) > 1 THEN 'ERROR: Multiple current rows'
        ELSE 'OK'
    END AS ValidationResult
FROM dw.DimCustomer
GROUP BY CustomerID
HAVING SUM(CAST(IsCurrent AS INT)) <> 1;

-- Result: Should return 0 rows if every customer has exactly one current record.

-- =============================================
-- VALIDATION 3: Current Row Should Have NULL or Far-Future EndDate
-- The active row (IsCurrent=1) must not have a past EndDate.
-- =============================================
PRINT '=== Validation 3: Current Row EndDate Check ===';

SELECT
    CustomerKey,
    CustomerID,
    FullName,
    StartDate,
    EndDate,
    IsCurrent
FROM dw.DimCustomer
WHERE IsCurrent = 1
    AND EndDate IS NOT NULL
    AND EndDate < '9999-12-31';

-- Result: Should return 0 rows. Current rows must have EndDate=NULL or 9999-12-31.

-- =============================================
-- VALIDATION 4: Expired Rows Must Have EndDate Set
-- Non-current rows (IsCurrent=0) must have a concrete EndDate.
-- =============================================
PRINT '=== Validation 4: Expired Row EndDate Check ===';

SELECT
    CustomerKey,
    CustomerID,
    FullName,
    StartDate,
    EndDate,
    IsCurrent
FROM dw.DimCustomer
WHERE IsCurrent = 0
    AND (EndDate IS NULL OR EndDate >= '9999-12-31');

-- Result: Should return 0 rows. Expired rows must have a real EndDate.

-- =============================================
-- VALIDATION 5: Row Count Comparison (Staging vs Dimension)
-- Number of distinct CustomerIDs in staging should match
-- number of distinct CustomerIDs with IsCurrent=1 in dim.
-- =============================================
PRINT '=== Validation 5: Row Count Comparison ===';

DECLARE @StagingDistinct INT, @DimCurrent INT;

SELECT @StagingDistinct = COUNT(DISTINCT CustomerID)
FROM staging.CRMCustomers;

SELECT @DimCurrent = COUNT(DISTINCT CustomerID)
FROM dw.DimCustomer
WHERE IsCurrent = 1;

SELECT
    @StagingDistinct AS StagingDistinctCustomers,
    @DimCurrent AS DimCurrentCustomers,
    CASE
        WHEN @StagingDistinct <= @DimCurrent THEN 'OK - Dim has all staging customers'
        ELSE 'WARNING - Staging has customers missing from dimension'
    END AS ValidationResult;

-- =============================================
-- VALIDATION 6: StartDate < EndDate for All Expired Rows
-- Ensures temporal ordering is correct on all historical records.
-- =============================================
PRINT '=== Validation 6: Temporal Order Check ===';

SELECT
    CustomerKey,
    CustomerID,
    FullName,
    StartDate,
    EndDate
FROM dw.DimCustomer
WHERE IsCurrent = 0
    AND EndDate IS NOT NULL
    AND StartDate >= EndDate;

-- Result: Should return 0 rows. StartDate must always be before EndDate.

-- =============================================
-- VALIDATION 7: Summary Statistics
-- Provides an overview of the dimension health.
-- =============================================
PRINT '=== Validation 7: Summary Statistics ===';

SELECT
    COUNT(*) AS TotalRows,
    COUNT(DISTINCT CustomerID) AS UniqueCustomers,
    SUM(CASE WHEN IsCurrent = 1 THEN 1 ELSE 0 END) AS CurrentRows,
    SUM(CASE WHEN IsCurrent = 0 THEN 1 ELSE 0 END) AS HistoricalRows,
    MIN(StartDate) AS EarliestStartDate,
    MAX(StartDate) AS LatestStartDate,
    CAST(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT CustomerID), 0) AS DECIMAL(5,2)) AS AvgVersionsPerCustomer
FROM dw.DimCustomer;

PRINT '=== SCD2 validation complete ===';
GO
