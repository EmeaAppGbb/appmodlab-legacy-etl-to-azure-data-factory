-- =============================================
-- Incremental Load Queries
-- Watermark-based change tracking patterns
-- =============================================

USE BrightfieldDW;
GO

-- =============================================
-- Get Last Extract Watermark for POS Sales
-- =============================================
-- Used by Extract_POS_Sales package to determine incremental load start point

SELECT 
    ISNULL(MAX(TransactionDate), '1900-01-01') AS LastExtractDate
FROM staging.POSSales
WHERE SourceSystem = 'POS';
GO

-- =============================================
-- Get Last Extract Watermark for Inventory
-- =============================================

SELECT 
    ISNULL(MAX(SnapshotDate), '1900-01-01') AS LastSnapshotDate
FROM staging.InventoryLevels;
GO

-- =============================================
-- Change Tracking Query for Customer Updates
-- Identifies modified customers since last extraction
-- =============================================

-- Example using SQL Server Change Tracking
-- (Assumes Change Tracking is enabled on source CRM database)

/*
SELECT 
    c.CustomerID,
    c.FirstName,
    c.LastName,
    c.Email,
    c.LastModifiedDate,
    ct.SYS_CHANGE_OPERATION AS ChangeType -- 'I' = Insert, 'U' = Update, 'D' = Delete
FROM CHANGETABLE(CHANGES dbo.Customers, @LastSyncVersion) AS ct
INNER JOIN dbo.Customers c ON c.CustomerID = ct.CustomerID
WHERE ct.SYS_CHANGE_VERSION <= @CurrentSyncVersion;
*/

-- Alternative: Timestamp-based incremental load
SELECT 
    CustomerID,
    FirstName,
    LastName,
    Email,
    Phone,
    LastModifiedDate
FROM CRM.dbo.Customers
WHERE LastModifiedDate > ?  -- Parameter from SSIS variable
ORDER BY LastModifiedDate;
GO

-- =============================================
-- Incremental Load Pattern: High Water Mark
-- =============================================

-- Create watermark tracking table
IF OBJECT_ID('audit.WatermarkTable', 'U') IS NOT NULL
    DROP TABLE audit.WatermarkTable;
GO

CREATE TABLE audit.WatermarkTable
(
    SourceTable VARCHAR(255) NOT NULL PRIMARY KEY,
    WatermarkColumn VARCHAR(100) NOT NULL,
    WatermarkValue SQL_VARIANT NULL,
    LastUpdateDate DATETIME NOT NULL DEFAULT GETDATE()
);
GO

-- Initialize watermarks for each source
INSERT INTO audit.WatermarkTable (SourceTable, WatermarkColumn, WatermarkValue)
VALUES 
    ('POSSales', 'TransactionDate', CAST('2024-01-01' AS DATETIME)),
    ('InventoryLevels', 'SnapshotDate', CAST('2024-01-01' AS DATE)),
    ('CRMCustomers', 'LastModifiedDate', CAST('2024-01-01' AS DATETIME));
GO

-- =============================================
-- Stored Procedure: Get Watermark
-- =============================================

IF OBJECT_ID('audit.GetWatermark', 'P') IS NOT NULL
    DROP PROCEDURE audit.GetWatermark;
GO

CREATE PROCEDURE audit.GetWatermark
    @SourceTable VARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        WatermarkValue,
        LastUpdateDate
    FROM audit.WatermarkTable
    WHERE SourceTable = @SourceTable;
END
GO

-- =============================================
-- Stored Procedure: Update Watermark
-- =============================================

IF OBJECT_ID('audit.UpdateWatermark', 'P') IS NOT NULL
    DROP PROCEDURE audit.UpdateWatermark;
GO

CREATE PROCEDURE audit.UpdateWatermark
    @SourceTable VARCHAR(255),
    @NewWatermark SQL_VARIANT
AS
BEGIN
    SET NOCOUNT ON;
    
    UPDATE audit.WatermarkTable
    SET 
        WatermarkValue = @NewWatermark,
        LastUpdateDate = GETDATE()
    WHERE SourceTable = @SourceTable;
    
    PRINT 'Watermark updated for ' + @SourceTable;
END
GO
