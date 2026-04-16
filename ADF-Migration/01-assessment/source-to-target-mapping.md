# Source-to-Target Column Mapping — Brightfield Retail Analytics

> **Assessment Date:** 2026-04-16
> **Scope:** Staging tables → Data Warehouse (star schema) column-level mappings
> **Derived from:** SSIS .dtsx packages, SQL DDL, and stored procedures

---

## 1. staging.POSSales → dw.FactSales

| # | Source Column (staging.POSSales) | Source Type | Target Column (dw.FactSales) | Target Type | Transformation / Derivation |
|---|---|---|---|---|---|
| 1 | TransactionID | VARCHAR(50) | TransactionID | VARCHAR(50) | Direct map |
| 2 | StoreID | VARCHAR(20) | StoreKey | INT | **Lookup** → `dw.DimStore` ON `StoreID = StoreID` → return `StoreKey` |
| 3 | ProductID | VARCHAR(50) | ProductKey | INT | **Lookup** → `dw.DimProduct` ON `ProductID = ProductID AND IsCurrent = 1` → return `ProductKey` |
| 4 | CustomerID | VARCHAR(50) | CustomerKey | INT | **Lookup** → `dw.DimCustomer` ON `CustomerID = CustomerID AND IsCurrent = 1` → return `CustomerKey` (LEFT JOIN — nullable) |
| 5 | TransactionDate | DATETIME | DateKey | INT | **Lookup** → `dw.DimDate` ON `CAST(TransactionDate AS DATE) = Date` → return `DateKey` |
| 6 | TransactionDate | DATETIME | TransactionDate | DATETIME | Direct map |
| 7 | Quantity | INT | Quantity | INT | Direct map; filtered `Quantity > 0` |
| 8 | UnitPrice | DECIMAL(18,2) | UnitPrice | DECIMAL(18,2) | Direct map; filtered `UnitPrice > 0` |
| 9 | Discount | DECIMAL(5,2) | DiscountPercent | DECIMAL(5,2) | Direct map (rename) |
| 10 | — (derived) | — | DiscountAmount | DECIMAL(18,2) | `(Quantity × UnitPrice) × (Discount / 100.0)` |
| 11 | — (derived) | — | Revenue | DECIMAL(18,2) | `Quantity × UnitPrice` |
| 12 | — (derived) | — | Cost | DECIMAL(18,2) | `DimProduct.UnitCost × Quantity` (from product lookup) |
| 13 | — (derived) | — | GrossProfit | DECIMAL(18,2) | `Revenue − Cost` → `(Quantity × UnitPrice) − (UnitCost × Quantity)` |
| 14 | TotalAmount | DECIMAL(18,2) | NetRevenue | DECIMAL(18,2) | Direct map (rename) |
| 15 | — (system) | — | LoadDate | DATETIME | `GETDATE()` at insert time |
| 16 | — (system) | — | SalesKey | BIGINT IDENTITY | Auto-generated surrogate key |

### Data Quality Filters Applied

- `Quantity > 0` — rows with zero/negative quantity are rejected
- `UnitPrice > 0` — rows with zero/negative price are rejected
- Duplicate check: `NOT EXISTS (SELECT 1 FROM dw.FactSales WHERE TransactionID = s.TransactionID)`

---

## 2. staging.CRMCustomers → dw.DimCustomer (SCD Type 2)

| # | Source Column (staging.CRMCustomers) | Source Type | Target Column (dw.DimCustomer) | Target Type | Transformation / SCD Behaviour |
|---|---|---|---|---|---|
| 1 | CustomerID | VARCHAR(50) | CustomerID | VARCHAR(50) | **Business Key** — used for SCD matching |
| 2 | FullName | VARCHAR(200) | FullName | VARCHAR(200) | Direct map; derived in extract as `FirstName + " " + LastName` |
| 3 | Email | VARCHAR(255) | Email | VARCHAR(255) | Direct map — **Fixed Attribute** (SCD Type 1 overwrite) |
| 4 | Phone | VARCHAR(50) | Phone | VARCHAR(50) | Direct map — **Fixed Attribute** (SCD Type 1 overwrite) |
| 5 | Address | VARCHAR(255) | Address | VARCHAR(255) | Direct map — **Changing Attribute** (SCD Type 2 new row) |
| 6 | City | VARCHAR(100) | City | VARCHAR(100) | Direct map — **Changing Attribute** (SCD Type 2 new row) |
| 7 | State | VARCHAR(50) | State | VARCHAR(50) | Direct map — **Changing Attribute** (SCD Type 2 new row) |
| 8 | ZipCode | VARCHAR(20) | ZipCode | VARCHAR(20) | Direct map |
| 9 | Country | VARCHAR(50) | Country | VARCHAR(50) | Direct map |
| 10 | Segment | VARCHAR(50) | Segment | VARCHAR(50) | Direct map — **Changing Attribute** (SCD Type 2 new row) |
| 11 | LoyaltyTier | VARCHAR(50) | LoyaltyTier | VARCHAR(50) | Direct map — **Changing Attribute** (SCD Type 2 new row) |
| 12 | LoyaltyPoints | INT | LoyaltyPoints | INT | Direct map |
| 13 | — (system) | — | CustomerKey | INT IDENTITY | Auto-generated surrogate key |
| 14 | — (system) | — | StartDate | DATETIME | `GETDATE()` when row is inserted |
| 15 | — (system) | — | EndDate | DATETIME | NULL for current; `GETDATE()` when expired |
| 16 | — (system) | — | IsCurrent | BIT | `1` for active version; `0` when superseded |
| 17 | — (system) | — | RecordCreatedDate | DATETIME | `GETDATE()` at first insert |
| 18 | — (system) | — | RecordModifiedDate | DATETIME | `GETDATE()` at each change |

### SCD Type 2 Logic (from `dw.ProcessCustomerDimensionSCD2`)

1. **Expire** — UPDATE existing current row (`IsCurrent=1`) → set `EndDate=GETDATE()`, `IsCurrent=0` where any of: FullName, Email, Phone, Address, City, State, LoyaltyTier, Segment differ
2. **Insert new version** — INSERT a new row with `IsCurrent=1`, `StartDate=GETDATE()`, `EndDate=NULL` for changed records
3. **Insert new customer** — INSERT where `CustomerID` does not exist in dimension at all

---

## 3. staging.InventoryLevels → dw.FactInventory

| # | Source Column (staging.InventoryLevels) | Source Type | Target Column (dw.FactInventory) | Target Type | Transformation / Derivation |
|---|---|---|---|---|---|
| 1 | StoreID | VARCHAR(20) | StoreKey | INT | **Lookup** → `dw.DimStore` ON `StoreID` → return `StoreKey` |
| 2 | ProductID | VARCHAR(50) | ProductKey | INT | **Lookup** → `dw.DimProduct` ON `ProductID AND IsCurrent = 1` → return `ProductKey` |
| 3 | SnapshotDate | DATE | DateKey | INT | **Lookup** → `dw.DimDate` ON `SnapshotDate = Date` → return `DateKey` |
| 4 | QuantityOnHand | INT | QuantityOnHand | INT | Direct map (Oracle NUMBER → SQL INT via Data Conversion) |
| 5 | QuantityOnOrder | INT | QuantityOnOrder | INT | Direct map (Oracle NUMBER → SQL INT via Data Conversion) |
| 6 | ReorderPoint | INT | ReorderPoint | INT | Direct map |
| 7 | — (derived) | — | DaysOfSupply | INT | Calculated (not in current packages — placeholder for future) |
| 8 | — (derived) | — | InventoryValue | DECIMAL(18,2) | Calculated: `QuantityOnHand × DimProduct.UnitCost` (expected) |
| 9 | SnapshotDate | DATE | SnapshotDate | DATE | Direct map |
| 10 | — (system) | — | LoadDate | DATETIME | `GETDATE()` at insert time |
| 11 | — (system) | — | InventoryKey | BIGINT IDENTITY | Auto-generated surrogate key |

---

## 4. staging.SupplierPricing → dw.DimProduct (Cost Update)

| # | Source Column (staging.SupplierPricing) | Source Type | Target Column (dw.DimProduct) | Target Type | Transformation |
|---|---|---|---|---|---|
| 1 | ProductID | VARCHAR(50) | ProductID | VARCHAR(50) | **Join key** — match to existing product |
| 2 | UnitCost | DECIMAL(18,2) | UnitCost | DECIMAL(18,2) | UPDATE on match (latest `EffectiveDate` wins) |
| 3 | EffectiveDate | DATE | — | — | Used as filter: only apply if `EffectiveDate <= GETDATE()` |
| 4 | SupplierID | VARCHAR(50) | — | — | Not directly mapped to DimProduct (reference only) |

---

## 5. staging.EcommerceOrders → dw.FactSales (via transformation)

| # | Source Column (staging.EcommerceOrders) | Source Type | Target Column (dw.FactSales) | Target Type | Transformation |
|---|---|---|---|---|---|
| 1 | OrderID | VARCHAR(50) | TransactionID | VARCHAR(50) | Direct map (rename) |
| 2 | CustomerID | VARCHAR(50) | CustomerKey | INT | **Lookup** → `dw.DimCustomer` ON `CustomerID AND IsCurrent=1` |
| 3 | OrderDate | DATETIME | DateKey | INT | **Lookup** → `dw.DimDate` ON `CAST(OrderDate AS DATE) = Date` |
| 4 | OrderDate | DATETIME | TransactionDate | DATETIME | Direct map (rename) |
| 5 | TotalAmount | DECIMAL(18,2) | NetRevenue | DECIMAL(18,2) | Direct map |
| 6 | — (line items) | — | Quantity, UnitPrice, etc. | — | Requires order-line-item expansion (not fully defined in current packages) |

> **Note:** The e-commerce extract uses a C# Script Task that calls a REST API and bulk-inserts JSON. The exact field mapping is embedded in the script logic, not declaratively in the data flow. Full column mapping will be confirmed during Wave 4 migration when the script is replaced with an ADF REST connector.

---

## 6. POS Source System → staging.POSSales (Extract Mapping)

| # | Source Column (POS `dbo.SalesTransactions`) | Target Column (staging.POSSales) | Transformation |
|---|---|---|---|
| 1 | TransactionID | TransactionID | Direct |
| 2 | StoreID | StoreID | Direct |
| 3 | ProductID | ProductID | Direct |
| 4 | CustomerID | CustomerID | Direct |
| 5 | TransactionDate | TransactionDate | Direct; used as incremental watermark (`> LastExtractDate`) |
| 6 | Quantity | Quantity | Direct |
| 7 | UnitPrice | UnitPrice | Direct |
| 8 | Discount | Discount | Direct |
| 9 | TotalAmount | TotalAmount | Direct |
| 10 | — (derived) | ExtractDate | `GETDATE()` — Derived Column transform |
| 11 | — (derived) | SourceSystem | `"POS"` — Derived Column transform |

---

## 7. CRM Source System → staging.CRMCustomers (Extract Mapping)

| # | Source Column (CRM `dbo.Customers`) | Target Column (staging.CRMCustomers) | Transformation |
|---|---|---|---|
| 1 | CustomerID | CustomerID | Direct |
| 2 | FirstName | FirstName | Direct |
| 3 | LastName | LastName | Direct |
| 4 | — (derived) | FullName | `FirstName + " " + LastName` — Derived Column |
| 5 | Email | Email | Direct |
| 6 | Phone | Phone | Direct |
| 7 | Address | Address | Direct |
| 8 | City | City | Direct |
| 9 | State | State | Direct |
| 10 | ZipCode | ZipCode | Direct |
| 11 | Country | Country | Direct |
| 12 | LoyaltyTier | LoyaltyTier | Direct |
| 13 | LoyaltyPoints | LoyaltyPoints | Direct |
| 14 | Segment | Segment | Direct |
| 15 | CreatedDate | CreatedDate | Direct |
| 16 | LastModifiedDate | LastModifiedDate | Direct |
| 17 | IsActive | IsActive | Source filter: `WHERE IsActive = 1` |
| 18 | — (derived) | ExtractTimestamp | `GETDATE()` — Derived Column |

---

## 8. Oracle Inventory → staging.InventoryLevels (Extract Mapping)

| # | Source Column (Oracle `INVENTORY.STOCK_LEVELS`) | Target Column (staging.InventoryLevels) | Transformation |
|---|---|---|---|
| 1 | STORE_ID | StoreID | Direct + **Data Conversion** (Oracle VARCHAR2 → SQL VARCHAR) |
| 2 | PRODUCT_ID | ProductID | Direct + **Data Conversion** |
| 3 | QUANTITY_ON_HAND | QuantityOnHand | **Data Conversion** (Oracle NUMBER → SQL INT) |
| 4 | QUANTITY_ON_ORDER | QuantityOnOrder | **Data Conversion** (Oracle NUMBER → SQL INT) |
| 5 | LAST_RESTOCK_DATE | LastRestockDate | **Data Conversion** (Oracle DATE → SQL DATETIME) |
| 6 | REORDER_POINT | ReorderPoint | **Data Conversion** (Oracle NUMBER → SQL INT) |
| 7 | SNAPSHOT_DATE | SnapshotDate | Direct; source filter: `WHERE SNAPSHOT_DATE = TRUNC(SYSDATE)` |

---

## 9. Supplier CSV Files → staging.SupplierPricing (Extract Mapping)

| # | Source Column (CSV `pricing_*.csv`) | Target Column (staging.SupplierPricing) | Transformation |
|---|---|---|---|
| 1 | SupplierID | SupplierID | Direct (String, length 50) |
| 2 | ProductID | ProductID | Direct (String, length 50) |
| 3 | UnitCost | UnitCost | Direct (Decimal 18,2) |
| 4 | EffectiveDate | EffectiveDate | Direct (DateTime) |

> CSV properties: Delimited format, column names in first row, text qualifier `"`, code page 1252.
> Loaded via ForEach File Loop iterating `pricing_*.csv` from UNC share.

---

## 10. Data Lineage Summary

```
 ┌─────────────────────── SOURCE SYSTEMS ───────────────────────┐
 │                                                               │
 │  POS SQL Server    Oracle Inventory    CRM SQL Server         │
 │  (SalesTransactions) (STOCK_LEVELS)   (Customers)            │
 │        │                  │                │                  │
 │  REST API           UNC File Share                            │
 │  (E-commerce)       (Supplier CSVs)                           │
 └───┬──────────────┬───────┬────────┬────────┬─────────────────┘
     │              │       │        │        │
     ▼              ▼       ▼        ▼        ▼
 ┌─────────────────────── STAGING LAYER ────────────────────────┐
 │                                                               │
 │  staging.POSSales    staging.InventoryLevels                  │
 │  staging.CRMCustomers   staging.EcommerceOrders               │
 │  staging.SupplierPricing                                      │
 │                                                               │
 └───┬──────────────┬───────┬────────┬────────┬─────────────────┘
     │              │       │        │        │
     ▼              ▼       ▼        ▼        ▼
 ┌──────────────── DATA QUALITY / TRANSFORM ────────────────────┐
 │                                                               │
 │  DQ Rules (Script) → Address Standardization → Deduplication  │
 │  Surrogate Key Lookups (Date, Store, Product, Customer)       │
 │  SCD Type 2 Processing (Customer Dimension)                   │
 │  Metric Derivation (Revenue, Cost, Profit)                    │
 │                                                               │
 └───┬──────────────┬───────────────────────────────────────────┘
     │              │
     ▼              ▼
 ┌─────────────────────── DATA WAREHOUSE ───────────────────────┐
 │                                                               │
 │  dw.DimDate    dw.DimStore    dw.DimProduct    dw.DimCustomer│
 │                                                               │
 │  dw.FactSales                 dw.FactInventory               │
 │                                                               │
 └──────────────────────────────────────────────────────────────┘
```
