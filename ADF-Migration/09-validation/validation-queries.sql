-- ============================================================================
-- Brightfield Retail Group: SSIS-to-ADF Migration Validation Queries
-- Database: BrightfieldDW (Azure Synapse / Azure SQL)
-- Purpose: Compare SSIS legacy output vs ADF migrated output for parity
-- ============================================================================

-- ============================================================================
-- SECTION 1: ROW COUNT COMPARISONS
-- Run against both SSIS-loaded and ADF-loaded environments to compare totals.
-- ============================================================================

-- 1.1 Staging table row counts
SELECT
    'staging.POSSales'        AS TableName, COUNT(*) AS RowCount FROM staging.POSSales
UNION ALL SELECT
    'staging.InventoryLevels' AS TableName, COUNT(*) AS RowCount FROM staging.InventoryLevels
UNION ALL SELECT
    'staging.CRMCustomers'    AS TableName, COUNT(*) AS RowCount FROM staging.CRMCustomers
UNION ALL SELECT
    'staging.EcommerceOrders' AS TableName, COUNT(*) AS RowCount FROM staging.EcommerceOrders
UNION ALL SELECT
    'staging.SupplierPricing' AS TableName, COUNT(*) AS RowCount FROM staging.SupplierPricing
ORDER BY TableName;

-- 1.2 Dimension table row counts
SELECT
    'dw.DimDate'     AS TableName, COUNT(*) AS RowCount FROM dw.DimDate
UNION ALL SELECT
    'dw.DimStore'    AS TableName, COUNT(*) AS RowCount FROM dw.DimStore
UNION ALL SELECT
    'dw.DimProduct'  AS TableName, COUNT(*) AS RowCount FROM dw.DimProduct
UNION ALL SELECT
    'dw.DimCustomer' AS TableName, COUNT(*) AS RowCount FROM dw.DimCustomer
ORDER BY TableName;

-- 1.3 Fact table row counts
SELECT
    'dw.FactSales'     AS TableName, COUNT(*) AS RowCount FROM dw.FactSales
UNION ALL SELECT
    'dw.FactInventory' AS TableName, COUNT(*) AS RowCount FROM dw.FactInventory
ORDER BY TableName;

-- 1.4 Row count comparison: SSIS watermark vs ADF watermark
-- Run after both systems process the same date range
SELECT
    w.TableName,
    w.WatermarkValue          AS ADF_Watermark,
    w.RowsProcessed           AS ADF_RowsProcessed,
    w.LastRunStatus            AS ADF_LastStatus
FROM adf.IncrementalWatermark w
ORDER BY w.TableName;


-- ============================================================================
-- SECTION 2: CHECKSUM / HASH COMPARISONS
-- Binary checksums detect value-level differences between SSIS and ADF output.
-- ============================================================================

-- 2.1 FactSales aggregate checksum by date
-- Run in both environments and compare results
SELECT
    d.Date                                         AS SalesDate,
    COUNT(*)                                       AS RowCount,
    SUM(CAST(f.Revenue AS DECIMAL(18,2)))          AS TotalRevenue,
    SUM(CAST(f.Cost AS DECIMAL(18,2)))             AS TotalCost,
    SUM(CAST(f.GrossProfit AS DECIMAL(18,2)))      AS TotalGrossProfit,
    SUM(CAST(f.NetRevenue AS DECIMAL(18,2)))       AS TotalNetRevenue,
    CHECKSUM_AGG(CHECKSUM(
        f.DateKey, f.StoreKey, f.ProductKey, f.CustomerKey,
        f.Revenue, f.Cost, f.GrossProfit, f.NetRevenue
    ))                                             AS RowChecksum
FROM dw.FactSales f
INNER JOIN dw.DimDate d ON f.DateKey = d.DateKey
GROUP BY d.Date
ORDER BY d.Date;

-- 2.2 FactInventory aggregate checksum by snapshot date
SELECT
    d.Date                                              AS SnapshotDate,
    COUNT(*)                                            AS RowCount,
    SUM(CAST(f.QuantityOnHand AS BIGINT))               AS TotalQtyOnHand,
    SUM(CAST(f.QuantityOnOrder AS BIGINT))               AS TotalQtyOnOrder,
    SUM(CAST(f.InventoryValue AS DECIMAL(18,2)))         AS TotalInventoryValue,
    CHECKSUM_AGG(CHECKSUM(
        f.DateKey, f.StoreKey, f.ProductKey,
        f.QuantityOnHand, f.QuantityOnOrder,
        f.DaysOfSupply, f.InventoryValue
    ))                                                   AS RowChecksum
FROM dw.FactInventory f
INNER JOIN dw.DimDate d ON f.DateKey = d.DateKey
GROUP BY d.Date
ORDER BY d.Date;

-- 2.3 DimCustomer checksum (current records only)
SELECT
    COUNT(*)                                        AS CurrentCustomerCount,
    CHECKSUM_AGG(CHECKSUM(
        CustomerID, FullName, Email, Phone,
        Address, City, State, LoyaltyTier, Segment
    ))                                              AS CurrentRecordChecksum
FROM dw.DimCustomer
WHERE IsCurrent = 1;

-- 2.4 DimProduct checksum
SELECT
    COUNT(*)                                        AS ProductCount,
    CHECKSUM_AGG(CHECKSUM(
        ProductKey, Category, Brand, UnitCost
    ))                                              AS ProductChecksum
FROM dw.DimProduct
WHERE IsCurrent = 1;

-- 2.5 Staging row-level hash comparison for POSSales
-- Detects individual row mismatches between legacy and ADF staging
SELECT
    TransactionID,
    HASHBYTES('SHA2_256',
        CONCAT_WS('|',
            CAST(TransactionID AS NVARCHAR(50)),
            CAST(StoreID AS NVARCHAR(50)),
            CAST(ProductID AS NVARCHAR(50)),
            CAST(CustomerID AS NVARCHAR(50)),
            CAST(TransactionDate AS NVARCHAR(50)),
            CAST(Quantity AS NVARCHAR(50)),
            CAST(UnitPrice AS NVARCHAR(50)),
            CAST(DiscountAmount AS NVARCHAR(50))
        )
    ) AS RowHash
FROM staging.POSSales
ORDER BY TransactionID;


-- ============================================================================
-- SECTION 3: SCD TYPE 2 INTEGRITY CHECKS (dw.DimCustomer)
-- These queries validate that the SCD2 implementation produces correct history.
-- ============================================================================

-- 3.1 Every CustomerID must have exactly one current record
SELECT
    CustomerID,
    COUNT(*) AS CurrentRecordCount
FROM dw.DimCustomer
WHERE IsCurrent = 1
GROUP BY CustomerID
HAVING COUNT(*) <> 1;
-- Expected: 0 rows (each customer has exactly one current version)

-- 3.2 Current records must have NULL EndDate
SELECT
    CustomerKey,
    CustomerID,
    StartDate,
    EndDate,
    IsCurrent
FROM dw.DimCustomer
WHERE IsCurrent = 1
  AND EndDate IS NOT NULL;
-- Expected: 0 rows

-- 3.3 Historical records must have a non-NULL EndDate
SELECT
    CustomerKey,
    CustomerID,
    StartDate,
    EndDate,
    IsCurrent
FROM dw.DimCustomer
WHERE IsCurrent = 0
  AND EndDate IS NULL;
-- Expected: 0 rows

-- 3.4 StartDate must be strictly before EndDate for historical records
SELECT
    CustomerKey,
    CustomerID,
    StartDate,
    EndDate
FROM dw.DimCustomer
WHERE IsCurrent = 0
  AND StartDate >= EndDate;
-- Expected: 0 rows

-- 3.5 No overlapping date ranges per CustomerID
-- Uses a self-join to detect any two versions with overlapping validity
SELECT
    a.CustomerKey  AS KeyA,
    b.CustomerKey  AS KeyB,
    a.CustomerID,
    a.StartDate    AS StartA,
    a.EndDate      AS EndA,
    b.StartDate    AS StartB,
    b.EndDate      AS EndB
FROM dw.DimCustomer a
INNER JOIN dw.DimCustomer b
    ON  a.CustomerID  = b.CustomerID
    AND a.CustomerKey <> b.CustomerKey
WHERE a.StartDate < ISNULL(b.EndDate, '9999-12-31')
  AND b.StartDate < ISNULL(a.EndDate, '9999-12-31');
-- Expected: 0 rows (no overlaps)

-- 3.6 Temporal continuity: for each customer, the EndDate of one version
--     should equal the StartDate of the next version (no gaps)
WITH Ordered AS (
    SELECT
        CustomerID,
        CustomerKey,
        StartDate,
        EndDate,
        ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY StartDate) AS VersionNum
    FROM dw.DimCustomer
)
SELECT
    cur.CustomerID,
    cur.CustomerKey   AS CurrentVersionKey,
    cur.EndDate       AS CurrentEndDate,
    nxt.CustomerKey   AS NextVersionKey,
    nxt.StartDate     AS NextStartDate
FROM Ordered cur
INNER JOIN Ordered nxt
    ON  cur.CustomerID  = nxt.CustomerID
    AND cur.VersionNum  = nxt.VersionNum - 1
WHERE cur.EndDate <> nxt.StartDate;
-- Expected: 0 rows (seamless version chain)

-- 3.7 Version count distribution: how many versions per customer
SELECT
    VersionCount,
    COUNT(*) AS NumberOfCustomers
FROM (
    SELECT CustomerID, COUNT(*) AS VersionCount
    FROM dw.DimCustomer
    GROUP BY CustomerID
) sub
GROUP BY VersionCount
ORDER BY VersionCount;


-- ============================================================================
-- SECTION 4: REFERENTIAL INTEGRITY CHECKS
-- Ensure fact-dimension relationships are intact after ADF migration.
-- ============================================================================

-- 4.1 FactSales orphan check: DateKey
SELECT f.SalesKey, f.DateKey
FROM dw.FactSales f
LEFT JOIN dw.DimDate d ON f.DateKey = d.DateKey
WHERE d.DateKey IS NULL;

-- 4.2 FactSales orphan check: StoreKey
SELECT f.SalesKey, f.StoreKey
FROM dw.FactSales f
LEFT JOIN dw.DimStore s ON f.StoreKey = s.StoreKey
WHERE s.StoreKey IS NULL;

-- 4.3 FactSales orphan check: ProductKey
SELECT f.SalesKey, f.ProductKey
FROM dw.FactSales f
LEFT JOIN dw.DimProduct p ON f.ProductKey = p.ProductKey
WHERE p.ProductKey IS NULL;

-- 4.4 FactSales orphan check: CustomerKey
SELECT f.SalesKey, f.CustomerKey
FROM dw.FactSales f
LEFT JOIN dw.DimCustomer c ON f.CustomerKey = c.CustomerKey
WHERE c.CustomerKey IS NULL;

-- 4.5 FactInventory orphan check: all dimension keys
SELECT f.InventoryKey, f.DateKey, f.StoreKey, f.ProductKey
FROM dw.FactInventory f
LEFT JOIN dw.DimDate    d ON f.DateKey    = d.DateKey
LEFT JOIN dw.DimStore   s ON f.StoreKey   = s.StoreKey
LEFT JOIN dw.DimProduct p ON f.ProductKey = p.ProductKey
WHERE d.DateKey IS NULL
   OR s.StoreKey IS NULL
   OR p.ProductKey IS NULL;


-- ============================================================================
-- SECTION 5: INCREMENTAL LOAD VALIDATION
-- Verify watermark progression and delta correctness.
-- ============================================================================

-- 5.1 Watermark audit trail: ensure watermarks only move forward
SELECT
    TableName,
    WatermarkValue,
    PipelineName,
    LastRunStatus,
    RowsProcessed,
    LastUpdated
FROM adf.IncrementalWatermark
ORDER BY TableName;

-- 5.2 Detect duplicate rows in staging after incremental load
-- POS Sales should not have duplicate TransactionIDs
SELECT TransactionID, COUNT(*) AS DuplicateCount
FROM staging.POSSales
GROUP BY TransactionID
HAVING COUNT(*) > 1;

-- 5.3 Detect duplicate inventory snapshots (same Store+Product+Date)
SELECT StoreID, ProductID, SnapshotDate, COUNT(*) AS DuplicateCount
FROM staging.InventoryLevels
GROUP BY StoreID, ProductID, SnapshotDate
HAVING COUNT(*) > 1;

-- 5.4 Verify no data loss: count rows per date after incremental load
SELECT
    CAST(TransactionDate AS DATE) AS TxnDate,
    COUNT(*)                      AS RowCount
FROM staging.POSSales
WHERE TransactionDate >= DATEADD(DAY, -7, GETDATE())
GROUP BY CAST(TransactionDate AS DATE)
ORDER BY TxnDate;


-- ============================================================================
-- SECTION 6: DATA QUALITY VALIDATION
-- Cross-check business rules and data quality after transformation.
-- ============================================================================

-- 6.1 Revenue calculation: Revenue = Quantity * UnitPrice - DiscountAmount
SELECT
    f.SalesKey,
    f.Revenue,
    (s.Quantity * s.UnitPrice - s.DiscountAmount) AS ExpectedRevenue
FROM dw.FactSales f
INNER JOIN staging.POSSales s ON f.SalesKey = s.TransactionID
WHERE ABS(f.Revenue - (s.Quantity * s.UnitPrice - s.DiscountAmount)) > 0.01;
-- Expected: 0 rows

-- 6.2 GrossProfit = Revenue - Cost
SELECT SalesKey, Revenue, Cost, GrossProfit
FROM dw.FactSales
WHERE ABS(GrossProfit - (Revenue - Cost)) > 0.01;
-- Expected: 0 rows

-- 6.3 Null checks on required fact columns
SELECT 'FactSales' AS TableName, COUNT(*) AS NullCount
FROM dw.FactSales
WHERE DateKey IS NULL OR StoreKey IS NULL OR ProductKey IS NULL
UNION ALL
SELECT 'FactInventory', COUNT(*)
FROM dw.FactInventory
WHERE DateKey IS NULL OR StoreKey IS NULL OR ProductKey IS NULL;
-- Expected: all NullCount = 0

-- 6.4 Negative value checks
SELECT 'NegativeRevenue'  AS Issue, COUNT(*) AS Cnt FROM dw.FactSales WHERE Revenue < 0
UNION ALL
SELECT 'NegativeCost',     COUNT(*) FROM dw.FactSales WHERE Cost < 0
UNION ALL
SELECT 'NegativeQtyOnHand', COUNT(*) FROM dw.FactInventory WHERE QuantityOnHand < 0;

-- 6.5 Summary comparison: SSIS batch audit vs ADF pipeline run
-- Join legacy audit table with ADF watermark to compare processed counts
SELECT
    a.BatchID,
    a.PackageName       AS SSIS_Package,
    a.RowsProcessed     AS SSIS_Rows,
    w.PipelineName      AS ADF_Pipeline,
    w.RowsProcessed     AS ADF_Rows,
    (w.RowsProcessed - a.RowsProcessed) AS RowDifference
FROM audit.PackageExecution a
INNER JOIN adf.IncrementalWatermark w
    ON a.TargetTable = w.TableName
WHERE a.BatchID = (SELECT MAX(BatchID) FROM audit.PackageExecution)
ORDER BY a.PackageName;
