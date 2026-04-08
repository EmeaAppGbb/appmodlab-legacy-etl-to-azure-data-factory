-- =============================================
-- Stored Procedure: Process Customer Dimension SCD Type 2
-- Handles inserts and updates with historical tracking
-- =============================================

USE BrightfieldDW;
GO

IF OBJECT_ID('dw.ProcessCustomerDimensionSCD2', 'P') IS NOT NULL
    DROP PROCEDURE dw.ProcessCustomerDimensionSCD2;
GO

CREATE PROCEDURE dw.ProcessCustomerDimensionSCD2
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Step 1: Expire changed records (set EndDate and IsCurrent = 0)
    UPDATE t
    SET 
        t.EndDate = GETDATE(),
        t.IsCurrent = 0,
        t.RecordModifiedDate = GETDATE()
    FROM dw.DimCustomer t
    INNER JOIN staging.CRMCustomers s ON t.CustomerID = s.CustomerID
    WHERE 
        t.IsCurrent = 1
        AND (
            t.FullName <> s.FullName OR
            t.Email <> s.Email OR
            t.Phone <> s.Phone OR
            t.Address <> s.Address OR
            t.City <> s.City OR
            t.State <> s.State OR
            t.LoyaltyTier <> s.LoyaltyTier OR
            t.Segment <> s.Segment
        );
    
    -- Step 2: Insert new versions of changed records
    INSERT INTO dw.DimCustomer 
    (
        CustomerID, FullName, Email, Phone, Address, City, State, ZipCode, Country,
        Segment, LoyaltyTier, LoyaltyPoints, StartDate, EndDate, IsCurrent, 
        RecordCreatedDate, RecordModifiedDate
    )
    SELECT 
        s.CustomerID, s.FullName, s.Email, s.Phone, s.Address, s.City, s.State, s.ZipCode, s.Country,
        s.Segment, s.LoyaltyTier, s.LoyaltyPoints, GETDATE(), NULL, 1,
        GETDATE(), GETDATE()
    FROM staging.CRMCustomers s
    INNER JOIN dw.DimCustomer t ON s.CustomerID = t.CustomerID
    WHERE 
        t.IsCurrent = 0
        AND t.EndDate = (SELECT MAX(EndDate) FROM dw.DimCustomer WHERE CustomerID = s.CustomerID);
    
    -- Step 3: Insert completely new customers
    INSERT INTO dw.DimCustomer 
    (
        CustomerID, FullName, Email, Phone, Address, City, State, ZipCode, Country,
        Segment, LoyaltyTier, LoyaltyPoints, StartDate, EndDate, IsCurrent, 
        RecordCreatedDate, RecordModifiedDate
    )
    SELECT 
        s.CustomerID, s.FullName, s.Email, s.Phone, s.Address, s.City, s.State, s.ZipCode, s.Country,
        s.Segment, s.LoyaltyTier, s.LoyaltyPoints, GETDATE(), NULL, 1,
        GETDATE(), GETDATE()
    FROM staging.CRMCustomers s
    WHERE NOT EXISTS (
        SELECT 1 FROM dw.DimCustomer t WHERE t.CustomerID = s.CustomerID
    );
    
    PRINT 'Customer Dimension SCD Type 2 processing complete';
END
GO

-- =============================================
-- Stored Procedure: Merge Sales Facts
-- =============================================

IF OBJECT_ID('dw.MergeFactSales', 'P') IS NOT NULL
    DROP PROCEDURE dw.MergeFactSales;
GO

CREATE PROCEDURE dw.MergeFactSales
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Merge staging sales data into FactSales with dimension lookups
    INSERT INTO dw.FactSales
    (
        DateKey, StoreKey, ProductKey, CustomerKey, TransactionID,
        Quantity, UnitPrice, DiscountPercent, DiscountAmount, Revenue, 
        Cost, GrossProfit, NetRevenue, TransactionDate, LoadDate
    )
    SELECT 
        d.DateKey,
        st.StoreKey,
        p.ProductKey,
        c.CustomerKey,
        s.TransactionID,
        s.Quantity,
        s.UnitPrice,
        s.Discount,
        (s.Quantity * s.UnitPrice) * (s.Discount / 100.0) AS DiscountAmount,
        s.Quantity * s.UnitPrice AS Revenue,
        p.UnitCost * s.Quantity AS Cost,
        (s.Quantity * s.UnitPrice) - (p.UnitCost * s.Quantity) AS GrossProfit,
        s.TotalAmount AS NetRevenue,
        s.TransactionDate,
        GETDATE()
    FROM staging.POSSales s
    INNER JOIN dw.DimDate d ON CAST(s.TransactionDate AS DATE) = d.Date
    INNER JOIN dw.DimStore st ON s.StoreID = st.StoreID
    INNER JOIN dw.DimProduct p ON s.ProductID = p.ProductID AND p.IsCurrent = 1
    LEFT JOIN dw.DimCustomer c ON s.CustomerID = c.CustomerID AND c.IsCurrent = 1
    WHERE NOT EXISTS (
        SELECT 1 FROM dw.FactSales f WHERE f.TransactionID = s.TransactionID
    )
    AND s.Quantity > 0
    AND s.UnitPrice > 0;
    
    PRINT 'Fact Sales merge complete';
END
GO

-- =============================================
-- Stored Procedure: Drop Fact Sales Indexes
-- =============================================

IF OBJECT_ID('dw.DropFactSalesIndexes', 'P') IS NOT NULL
    DROP PROCEDURE dw.DropFactSalesIndexes;
GO

CREATE PROCEDURE dw.DropFactSalesIndexes
AS
BEGIN
    SET NOCOUNT ON;
    
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('dw.FactSales') AND name = 'IX_FactSales_DateKey')
        DROP INDEX IX_FactSales_DateKey ON dw.FactSales;
    
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('dw.FactSales') AND name = 'IX_FactSales_StoreKey')
        DROP INDEX IX_FactSales_StoreKey ON dw.FactSales;
    
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('dw.FactSales') AND name = 'IX_FactSales_ProductKey')
        DROP INDEX IX_FactSales_ProductKey ON dw.FactSales;
    
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('dw.FactSales') AND name = 'IX_FactSales_CustomerKey')
        DROP INDEX IX_FactSales_CustomerKey ON dw.FactSales;
    
    PRINT 'Fact Sales indexes dropped';
END
GO

-- =============================================
-- Stored Procedure: Rebuild Fact Sales Indexes
-- =============================================

IF OBJECT_ID('dw.RebuildFactSalesIndexes', 'P') IS NOT NULL
    DROP PROCEDURE dw.RebuildFactSalesIndexes;
GO

CREATE PROCEDURE dw.RebuildFactSalesIndexes
AS
BEGIN
    SET NOCOUNT ON;
    
    CREATE INDEX IX_FactSales_DateKey ON dw.FactSales(DateKey);
    CREATE INDEX IX_FactSales_StoreKey ON dw.FactSales(StoreKey);
    CREATE INDEX IX_FactSales_ProductKey ON dw.FactSales(ProductKey);
    CREATE INDEX IX_FactSales_CustomerKey ON dw.FactSales(CustomerKey);
    CREATE INDEX IX_FactSales_TransactionDate ON dw.FactSales(TransactionDate);
    
    PRINT 'Fact Sales indexes rebuilt';
END
GO

-- =============================================
-- Stored Procedure: Initialize Batch
-- =============================================

IF OBJECT_ID('audit.InitializeBatch', 'P') IS NOT NULL
    DROP PROCEDURE audit.InitializeBatch;
GO

CREATE PROCEDURE audit.InitializeBatch
AS
BEGIN
    SET NOCOUNT ON;
    
    INSERT INTO audit.PackageExecution (PackageName, StartTime, Status, ServerName, UserName)
    VALUES ('MasterOrchestrator', GETDATE(), 'Running', @@SERVERNAME, SUSER_NAME());
    
    SELECT SCOPE_IDENTITY() AS BatchID;
END
GO

-- =============================================
-- Stored Procedure: Complete Batch
-- =============================================

IF OBJECT_ID('audit.CompleteBatch', 'P') IS NOT NULL
    DROP PROCEDURE audit.CompleteBatch;
GO

CREATE PROCEDURE audit.CompleteBatch
AS
BEGIN
    SET NOCOUNT ON;
    
    UPDATE audit.PackageExecution
    SET EndTime = GETDATE(), Status = 'Success'
    WHERE ExecutionID = (SELECT MAX(ExecutionID) FROM audit.PackageExecution);
    
    PRINT 'Batch completed successfully';
END
GO
