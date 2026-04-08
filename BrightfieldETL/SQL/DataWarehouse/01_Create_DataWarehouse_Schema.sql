-- =============================================
-- Data Warehouse Schema (Star Schema)
-- Brightfield Retail Analytics
-- =============================================

USE BrightfieldDW;
GO

-- Create data warehouse schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dw')
BEGIN
    EXEC('CREATE SCHEMA dw');
END
GO

-- =============================================
-- Dimension Table: DimDate
-- =============================================
IF OBJECT_ID('dw.DimDate', 'U') IS NOT NULL
    DROP TABLE dw.DimDate;
GO

CREATE TABLE dw.DimDate
(
    DateKey INT NOT NULL PRIMARY KEY,
    Date DATE NOT NULL,
    DayOfWeek VARCHAR(20) NOT NULL,
    DayOfMonth INT NOT NULL,
    DayOfYear INT NOT NULL,
    WeekOfYear INT NOT NULL,
    Month INT NOT NULL,
    MonthName VARCHAR(20) NOT NULL,
    Quarter INT NOT NULL,
    Year INT NOT NULL,
    IsHoliday BIT NOT NULL DEFAULT 0,
    HolidayName VARCHAR(100) NULL,
    IsWeekend BIT NOT NULL DEFAULT 0,
    FiscalPeriod VARCHAR(20) NULL,
    FiscalQuarter VARCHAR(20) NULL,
    FiscalYear INT NULL
);
GO

-- =============================================
-- Dimension Table: DimStore
-- =============================================
IF OBJECT_ID('dw.DimStore', 'U') IS NOT NULL
    DROP TABLE dw.DimStore;
GO

CREATE TABLE dw.DimStore
(
    StoreKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    StoreID VARCHAR(20) NOT NULL,
    StoreName VARCHAR(200) NOT NULL,
    Address VARCHAR(255) NULL,
    City VARCHAR(100) NOT NULL,
    State VARCHAR(50) NOT NULL,
    ZipCode VARCHAR(20) NULL,
    Country VARCHAR(50) NOT NULL,
    Region VARCHAR(50) NOT NULL,
    StoreFormat VARCHAR(50) NULL, -- Mall, Standalone, Strip Center
    OpenDate DATE NULL,
    CloseDate DATE NULL,
    IsActive BIT NOT NULL DEFAULT 1,
    SquareFootage INT NULL,
    ManagerName VARCHAR(200) NULL
);
GO

CREATE UNIQUE INDEX UIX_DimStore_StoreID ON dw.DimStore(StoreID);
GO

-- =============================================
-- Dimension Table: DimProduct (SCD Type 1)
-- =============================================
IF OBJECT_ID('dw.DimProduct', 'U') IS NOT NULL
    DROP TABLE dw.DimProduct;
GO

CREATE TABLE dw.DimProduct
(
    ProductKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    ProductID VARCHAR(50) NOT NULL,
    ProductName VARCHAR(255) NOT NULL,
    Category VARCHAR(100) NOT NULL,
    SubCategory VARCHAR(100) NULL,
    Brand VARCHAR(100) NULL,
    UnitPrice DECIMAL(18,2) NOT NULL,
    UnitCost DECIMAL(18,2) NULL,
    Color VARCHAR(50) NULL,
    Size VARCHAR(50) NULL,
    Weight DECIMAL(10,2) NULL,
    IsActive BIT NOT NULL DEFAULT 1,
    StartDate DATETIME NOT NULL DEFAULT GETDATE(),
    EndDate DATETIME NULL,
    IsCurrent BIT NOT NULL DEFAULT 1
);
GO

CREATE INDEX IX_DimProduct_ProductID ON dw.DimProduct(ProductID, IsCurrent);
GO

-- =============================================
-- Dimension Table: DimCustomer (SCD Type 2)
-- Tracks historical changes to customer attributes
-- =============================================
IF OBJECT_ID('dw.DimCustomer', 'U') IS NOT NULL
    DROP TABLE dw.DimCustomer;
GO

CREATE TABLE dw.DimCustomer
(
    CustomerKey INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    CustomerID VARCHAR(50) NOT NULL,
    FullName VARCHAR(200) NOT NULL,
    Email VARCHAR(255) NULL,
    Phone VARCHAR(50) NULL,
    Address VARCHAR(255) NULL,
    City VARCHAR(100) NULL,
    State VARCHAR(50) NULL,
    ZipCode VARCHAR(20) NULL,
    Country VARCHAR(50) NULL,
    Segment VARCHAR(50) NULL,         -- Bronze, Silver, Gold, Platinum
    LoyaltyTier VARCHAR(50) NULL,     -- Member, VIP, Elite
    LoyaltyPoints INT NULL,
    StartDate DATETIME NOT NULL DEFAULT GETDATE(),
    EndDate DATETIME NULL,
    IsCurrent BIT NOT NULL DEFAULT 1,
    RecordCreatedDate DATETIME NOT NULL DEFAULT GETDATE(),
    RecordModifiedDate DATETIME NOT NULL DEFAULT GETDATE()
);
GO

CREATE INDEX IX_DimCustomer_CustomerID ON dw.DimCustomer(CustomerID, IsCurrent);
CREATE INDEX IX_DimCustomer_IsCurrent ON dw.DimCustomer(IsCurrent) WHERE IsCurrent = 1;
GO

-- =============================================
-- Fact Table: FactSales
-- =============================================
IF OBJECT_ID('dw.FactSales', 'U') IS NOT NULL
    DROP TABLE dw.FactSales;
GO

CREATE TABLE dw.FactSales
(
    SalesKey BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    DateKey INT NOT NULL,
    StoreKey INT NOT NULL,
    ProductKey INT NOT NULL,
    CustomerKey INT NOT NULL,
    TransactionID VARCHAR(50) NOT NULL,
    Quantity INT NOT NULL,
    UnitPrice DECIMAL(18,2) NOT NULL,
    DiscountPercent DECIMAL(5,2) NULL,
    DiscountAmount DECIMAL(18,2) NULL,
    Revenue DECIMAL(18,2) NOT NULL,
    Cost DECIMAL(18,2) NULL,
    GrossProfit DECIMAL(18,2) NULL,
    NetRevenue DECIMAL(18,2) NOT NULL,
    TransactionDate DATETIME NOT NULL,
    LoadDate DATETIME NOT NULL DEFAULT GETDATE(),
    
    CONSTRAINT FK_FactSales_DimDate FOREIGN KEY (DateKey) REFERENCES dw.DimDate(DateKey),
    CONSTRAINT FK_FactSales_DimStore FOREIGN KEY (StoreKey) REFERENCES dw.DimStore(StoreKey),
    CONSTRAINT FK_FactSales_DimProduct FOREIGN KEY (ProductKey) REFERENCES dw.DimProduct(ProductKey),
    CONSTRAINT FK_FactSales_DimCustomer FOREIGN KEY (CustomerKey) REFERENCES dw.DimCustomer(CustomerKey)
);
GO

CREATE INDEX IX_FactSales_DateKey ON dw.FactSales(DateKey);
CREATE INDEX IX_FactSales_StoreKey ON dw.FactSales(StoreKey);
CREATE INDEX IX_FactSales_ProductKey ON dw.FactSales(ProductKey);
CREATE INDEX IX_FactSales_CustomerKey ON dw.FactSales(CustomerKey);
CREATE INDEX IX_FactSales_TransactionDate ON dw.FactSales(TransactionDate);
GO

-- =============================================
-- Fact Table: FactInventory
-- =============================================
IF OBJECT_ID('dw.FactInventory', 'U') IS NOT NULL
    DROP TABLE dw.FactInventory;
GO

CREATE TABLE dw.FactInventory
(
    InventoryKey BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    DateKey INT NOT NULL,
    StoreKey INT NOT NULL,
    ProductKey INT NOT NULL,
    QuantityOnHand INT NOT NULL,
    QuantityOnOrder INT NOT NULL,
    ReorderPoint INT NULL,
    DaysOfSupply INT NULL,
    InventoryValue DECIMAL(18,2) NULL,
    SnapshotDate DATE NOT NULL,
    LoadDate DATETIME NOT NULL DEFAULT GETDATE(),
    
    CONSTRAINT FK_FactInventory_DimDate FOREIGN KEY (DateKey) REFERENCES dw.DimDate(DateKey),
    CONSTRAINT FK_FactInventory_DimStore FOREIGN KEY (StoreKey) REFERENCES dw.DimStore(StoreKey),
    CONSTRAINT FK_FactInventory_DimProduct FOREIGN KEY (ProductKey) REFERENCES dw.DimProduct(ProductKey)
);
GO

CREATE INDEX IX_FactInventory_DateKey ON dw.FactInventory(DateKey);
CREATE INDEX IX_FactInventory_StoreKey ON dw.FactInventory(StoreKey);
CREATE INDEX IX_FactInventory_ProductKey ON dw.FactInventory(ProductKey);
GO
