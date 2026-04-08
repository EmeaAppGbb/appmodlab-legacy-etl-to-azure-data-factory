-- =============================================
-- Staging Tables DDL for Brightfield Retail Analytics
-- =============================================

USE BrightfieldDW;
GO

-- Create staging schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'staging')
BEGIN
    EXEC('CREATE SCHEMA staging');
END
GO

-- =============================================
-- Staging Table: POS Sales Transactions
-- =============================================
IF OBJECT_ID('staging.POSSales', 'U') IS NOT NULL
    DROP TABLE staging.POSSales;
GO

CREATE TABLE staging.POSSales
(
    TransactionID VARCHAR(50) NOT NULL,
    StoreID VARCHAR(20) NOT NULL,
    ProductID VARCHAR(50) NOT NULL,
    CustomerID VARCHAR(50) NULL,
    TransactionDate DATETIME NOT NULL,
    Quantity INT NOT NULL,
    UnitPrice DECIMAL(18,2) NOT NULL,
    Discount DECIMAL(5,2) NULL,
    TotalAmount DECIMAL(18,2) NOT NULL,
    ExtractDate DATETIME NOT NULL DEFAULT GETDATE(),
    SourceSystem VARCHAR(50) NOT NULL DEFAULT 'POS'
);
GO

CREATE INDEX IX_POSSales_TransactionDate ON staging.POSSales(TransactionDate);
GO

-- =============================================
-- Staging Table: Inventory Levels
-- =============================================
IF OBJECT_ID('staging.InventoryLevels', 'U') IS NOT NULL
    DROP TABLE staging.InventoryLevels;
GO

CREATE TABLE staging.InventoryLevels
(
    StoreID VARCHAR(20) NOT NULL,
    ProductID VARCHAR(50) NOT NULL,
    QuantityOnHand INT NOT NULL,
    QuantityOnOrder INT NOT NULL,
    LastRestockDate DATETIME NULL,
    ReorderPoint INT NULL,
    SnapshotDate DATE NOT NULL,
    ExtractDate DATETIME NOT NULL DEFAULT GETDATE()
);
GO

CREATE INDEX IX_InventoryLevels_SnapshotDate ON staging.InventoryLevels(SnapshotDate);
GO

-- =============================================
-- Staging Table: CRM Customers
-- =============================================
IF OBJECT_ID('staging.CRMCustomers', 'U') IS NOT NULL
    DROP TABLE staging.CRMCustomers;
GO

CREATE TABLE staging.CRMCustomers
(
    CustomerID VARCHAR(50) NOT NULL,
    FirstName VARCHAR(100) NOT NULL,
    LastName VARCHAR(100) NOT NULL,
    FullName VARCHAR(200) NOT NULL,
    Email VARCHAR(255) NULL,
    Phone VARCHAR(50) NULL,
    Address VARCHAR(255) NULL,
    City VARCHAR(100) NULL,
    State VARCHAR(50) NULL,
    ZipCode VARCHAR(20) NULL,
    Country VARCHAR(50) NULL,
    LoyaltyTier VARCHAR(50) NULL,
    LoyaltyPoints INT NULL,
    Segment VARCHAR(50) NULL,
    CreatedDate DATETIME NOT NULL,
    LastModifiedDate DATETIME NOT NULL,
    IsActive BIT NOT NULL,
    ExtractTimestamp DATETIME NOT NULL DEFAULT GETDATE()
);
GO

CREATE INDEX IX_CRMCustomers_CustomerID ON staging.CRMCustomers(CustomerID);
GO

-- =============================================
-- Staging Table: E-commerce Orders
-- =============================================
IF OBJECT_ID('staging.EcommerceOrders', 'U') IS NOT NULL
    DROP TABLE staging.EcommerceOrders;
GO

CREATE TABLE staging.EcommerceOrders
(
    OrderID VARCHAR(50) NOT NULL,
    CustomerID VARCHAR(50) NOT NULL,
    OrderDate DATETIME NOT NULL,
    ShipDate DATETIME NULL,
    OrderStatus VARCHAR(50) NOT NULL,
    TotalAmount DECIMAL(18,2) NOT NULL,
    ShippingAddress VARCHAR(500) NULL,
    PaymentMethod VARCHAR(50) NULL,
    ExtractDate DATETIME NOT NULL DEFAULT GETDATE()
);
GO

-- =============================================
-- Staging Table: Supplier Pricing
-- =============================================
IF OBJECT_ID('staging.SupplierPricing', 'U') IS NOT NULL
    DROP TABLE staging.SupplierPricing;
GO

CREATE TABLE staging.SupplierPricing
(
    SupplierID VARCHAR(50) NOT NULL,
    ProductID VARCHAR(50) NOT NULL,
    UnitCost DECIMAL(18,2) NOT NULL,
    EffectiveDate DATE NOT NULL,
    ExtractDate DATETIME NOT NULL DEFAULT GETDATE()
);
GO

CREATE INDEX IX_SupplierPricing_ProductID ON staging.SupplierPricing(ProductID);
GO
