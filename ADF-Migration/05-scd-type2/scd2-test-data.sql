-- =============================================
-- SCD Type 2 Test Data
-- Test scenarios for DimCustomer SCD2 processing
-- Migrated from: dw.ProcessCustomerDimensionSCD2
-- =============================================

USE BrightfieldDW;
GO

-- =============================================
-- SETUP: Clean test environment
-- =============================================
PRINT '=== Setting up SCD2 test environment ===';

DELETE FROM dw.DimCustomer WHERE CustomerID IN (90001, 90002, 90003, 90004);
DELETE FROM staging.CRMCustomers WHERE CustomerID IN (90001, 90002, 90003, 90004);
GO

-- =============================================
-- SCENARIO 1: New Customer Insert
-- A customer that does not exist in DimCustomer.
-- Expected: One new row inserted with IsCurrent=1,
--           StartDate=today, EndDate=NULL or 9999-12-31.
-- =============================================
PRINT '=== Scenario 1: New Customer Insert ===';

INSERT INTO staging.CRMCustomers
    (CustomerID, FullName, Email, Phone, Address, City, State, ZipCode, Country, Segment, LoyaltyTier, LoyaltyPoints, LastModifiedDate)
VALUES
    (90001, 'Alice Johnson', 'alice@example.com', '555-0101', '123 Main St', 'Seattle', 'WA', '98101', 'US', 'Premium', 'Gold', 1500, GETDATE());

-- Run SCD2 processing
EXEC dw.ProcessCustomerDimensionSCD2;

-- Verify: exactly 1 row, IsCurrent=1
SELECT 'Scenario 1 - New Customer' AS TestScenario,
       COUNT(*) AS RowCount,
       SUM(CAST(IsCurrent AS INT)) AS CurrentRows
FROM dw.DimCustomer
WHERE CustomerID = 90001;

-- =============================================
-- SCENARIO 2: Existing Customer Attribute Change
-- Change the LoyaltyTier and Address for an existing customer.
-- Expected: Old row expires (EndDate=today, IsCurrent=0),
--           new row inserted (IsCurrent=1, StartDate=today).
--           Total rows for this customer = 2.
-- =============================================
PRINT '=== Scenario 2: Existing Customer Attribute Change ===';

-- Pre-load an existing customer into DimCustomer
INSERT INTO dw.DimCustomer
    (CustomerID, FullName, Email, Phone, Address, City, State, ZipCode, Country, Segment, LoyaltyTier, LoyaltyPoints, StartDate, EndDate, IsCurrent, RecordCreatedDate, RecordModifiedDate)
VALUES
    (90002, 'Bob Smith', 'bob@example.com', '555-0202', '456 Oak Ave', 'Portland', 'OR', '97201', 'US', 'Standard', 'Silver', 800,
     '2024-01-15', NULL, 1, '2024-01-15', '2024-01-15');

-- Stage an updated version with changed LoyaltyTier and Address
INSERT INTO staging.CRMCustomers
    (CustomerID, FullName, Email, Phone, Address, City, State, ZipCode, Country, Segment, LoyaltyTier, LoyaltyPoints, LastModifiedDate)
VALUES
    (90002, 'Bob Smith', 'bob@example.com', '555-0202', '789 Pine Rd', 'Portland', 'OR', '97201', 'US', 'Standard', 'Gold', 1200, GETDATE());

-- Run SCD2 processing
EXEC dw.ProcessCustomerDimensionSCD2;

-- Verify: 2 rows total, 1 expired (IsCurrent=0), 1 current (IsCurrent=1)
SELECT 'Scenario 2 - Changed Customer' AS TestScenario,
       COUNT(*) AS TotalRows,
       SUM(CASE WHEN IsCurrent = 1 THEN 1 ELSE 0 END) AS CurrentRows,
       SUM(CASE WHEN IsCurrent = 0 THEN 1 ELSE 0 END) AS ExpiredRows
FROM dw.DimCustomer
WHERE CustomerID = 90002;

-- Verify the old row has EndDate set
SELECT 'Scenario 2 - Expired Row' AS TestScenario,
       CustomerKey, LoyaltyTier, Address, StartDate, EndDate, IsCurrent
FROM dw.DimCustomer
WHERE CustomerID = 90002 AND IsCurrent = 0;

-- Verify the new row has current attributes
SELECT 'Scenario 2 - Current Row' AS TestScenario,
       CustomerKey, LoyaltyTier, Address, StartDate, EndDate, IsCurrent
FROM dw.DimCustomer
WHERE CustomerID = 90002 AND IsCurrent = 1;

-- =============================================
-- SCENARIO 3: Unchanged Customer
-- Customer exists in both staging and dim with identical attributes.
-- Expected: No changes. Row count stays the same, IsCurrent=1.
-- =============================================
PRINT '=== Scenario 3: Unchanged Customer ===';

-- Pre-load a customer into DimCustomer
INSERT INTO dw.DimCustomer
    (CustomerID, FullName, Email, Phone, Address, City, State, ZipCode, Country, Segment, LoyaltyTier, LoyaltyPoints, StartDate, EndDate, IsCurrent, RecordCreatedDate, RecordModifiedDate)
VALUES
    (90003, 'Carol Davis', 'carol@example.com', '555-0303', '321 Elm Blvd', 'Austin', 'TX', '73301', 'US', 'Premium', 'Platinum', 3000,
     '2024-06-01', NULL, 1, '2024-06-01', '2024-06-01');

-- Stage the same customer with identical attributes
INSERT INTO staging.CRMCustomers
    (CustomerID, FullName, Email, Phone, Address, City, State, ZipCode, Country, Segment, LoyaltyTier, LoyaltyPoints, LastModifiedDate)
VALUES
    (90003, 'Carol Davis', 'carol@example.com', '555-0303', '321 Elm Blvd', 'Austin', 'TX', '73301', 'US', 'Premium', 'Platinum', 3000, GETDATE());

-- Capture row count before processing
DECLARE @BeforeCount INT;
SELECT @BeforeCount = COUNT(*) FROM dw.DimCustomer WHERE CustomerID = 90003;

-- Run SCD2 processing
EXEC dw.ProcessCustomerDimensionSCD2;

-- Verify: row count unchanged, still 1 current row
DECLARE @AfterCount INT;
SELECT @AfterCount = COUNT(*) FROM dw.DimCustomer WHERE CustomerID = 90003;

SELECT 'Scenario 3 - Unchanged Customer' AS TestScenario,
       @BeforeCount AS RowsBefore,
       @AfterCount AS RowsAfter,
       CASE WHEN @BeforeCount = @AfterCount THEN 'PASS' ELSE 'FAIL' END AS Result;

-- =============================================
-- SCENARIO 4: Reactivation (Multiple Changes Over Time)
-- A customer goes through two successive attribute changes,
-- resulting in 3 historical rows in the dimension.
-- Expected: 3 total rows, only 1 with IsCurrent=1.
-- =============================================
PRINT '=== Scenario 4: Reactivation / Multiple Changes ===';

-- Pre-load customer with initial state
INSERT INTO dw.DimCustomer
    (CustomerID, FullName, Email, Phone, Address, City, State, ZipCode, Country, Segment, LoyaltyTier, LoyaltyPoints, StartDate, EndDate, IsCurrent, RecordCreatedDate, RecordModifiedDate)
VALUES
    (90004, 'Dan Wilson', 'dan@example.com', '555-0404', '100 River Dr', 'Denver', 'CO', '80201', 'US', 'Standard', 'Bronze', 200,
     '2023-01-01', NULL, 1, '2023-01-01', '2023-01-01');

-- First change: upgrade tier
DELETE FROM staging.CRMCustomers WHERE CustomerID = 90004;
INSERT INTO staging.CRMCustomers
    (CustomerID, FullName, Email, Phone, Address, City, State, ZipCode, Country, Segment, LoyaltyTier, LoyaltyPoints, LastModifiedDate)
VALUES
    (90004, 'Dan Wilson', 'dan@example.com', '555-0404', '100 River Dr', 'Denver', 'CO', '80201', 'US', 'Standard', 'Silver', 600, GETDATE());

EXEC dw.ProcessCustomerDimensionSCD2;

-- Second change: address and segment change
DELETE FROM staging.CRMCustomers WHERE CustomerID = 90004;
INSERT INTO staging.CRMCustomers
    (CustomerID, FullName, Email, Phone, Address, City, State, ZipCode, Country, Segment, LoyaltyTier, LoyaltyPoints, LastModifiedDate)
VALUES
    (90004, 'Dan Wilson', 'dan@example.com', '555-0404', '200 Mountain Ave', 'Boulder', 'CO', '80301', 'US', 'Premium', 'Silver', 600, GETDATE());

EXEC dw.ProcessCustomerDimensionSCD2;

-- Verify: 3 rows total, exactly 1 current
SELECT 'Scenario 4 - Reactivation' AS TestScenario,
       COUNT(*) AS TotalRows,
       SUM(CASE WHEN IsCurrent = 1 THEN 1 ELSE 0 END) AS CurrentRows,
       SUM(CASE WHEN IsCurrent = 0 THEN 1 ELSE 0 END) AS ExpiredRows
FROM dw.DimCustomer
WHERE CustomerID = 90004;

-- Show full history
SELECT 'Scenario 4 - Full History' AS TestScenario,
       CustomerKey, LoyaltyTier, Segment, Address, City, StartDate, EndDate, IsCurrent
FROM dw.DimCustomer
WHERE CustomerID = 90004
ORDER BY StartDate;

-- =============================================
-- CLEANUP
-- =============================================
PRINT '=== Cleaning up test data ===';

DELETE FROM dw.DimCustomer WHERE CustomerID IN (90001, 90002, 90003, 90004);
DELETE FROM staging.CRMCustomers WHERE CustomerID IN (90001, 90002, 90003, 90004);

PRINT '=== SCD2 test scenarios complete ===';
GO
