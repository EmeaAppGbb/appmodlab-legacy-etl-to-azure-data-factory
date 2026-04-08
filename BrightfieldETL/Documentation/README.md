# Brightfield Retail Analytics - Legacy SSIS ETL System

## Overview
This legacy ETL system extracts, transforms, and loads retail analytics data from 5 source systems into a SQL Server data warehouse. The system runs nightly via SQL Server Agent and uses 30+ SSIS packages orchestrated by a master package.

## Architecture

### Source Systems
1. **POS (Point of Sale)** - SQL Server - Sales transactions
2. **Inventory Management** - Oracle - Stock levels and replenishment
3. **CRM** - SQL Server - Customer master data
4. **E-commerce Platform** - REST API - Online orders (JSON)
5. **Supplier Portal** - CSV files via FTP - Pricing updates

### Data Warehouse (Star Schema)
- **Fact Tables**: FactSales, FactInventory
- **Dimension Tables**: DimCustomer (SCD Type 2), DimProduct, DimStore, DimDate
- **Staging Area**: Temporary landing zone for raw extracted data
- **Audit Framework**: Execution logging, row counts, error tracking

## SSIS Package Structure

### Master Package
- **MasterOrchestrator.dtsx** - Sequentially executes Extract → Transform → Load phases

### Extract Packages
- **Extract_POS_Sales.dtsx** - Incremental load based on transaction date watermark
- **Extract_Inventory.dtsx** - Full daily snapshot from Oracle
- **Extract_CRM_Customers.dtsx** - Full load for SCD Type 2 processing
- **Extract_Ecommerce_Orders.dtsx** - REST API call with C# Script Task (anti-pattern)
- **Extract_Supplier_Files.dtsx** - ForEach loop to process CSV files from FTP share

### Transform Packages
- **Transform_DataQuality.dtsx** - Business rules validation (C# Script Task)
- **Transform_CustomerDim.dtsx** - SCD Type 2 processing with SSIS SCD wizard
- **Transform_ProductDim.dtsx** - SCD Type 1 dimension updates
- **Transform_TimeDim.dtsx** - Date dimension population
- **Transform_SalesFacts.dtsx** - Lookups, derived columns, data quality splits
- **Transform_InventoryFacts.dtsx** - Aggregation and enrichment

### Load Packages
- **Load_Dimensions.dtsx** - Bulk load dimension tables
- **Load_FactSales.dtsx** - Merge fact data with index drop/rebuild strategy
- **Load_FactInventory.dtsx** - Insert fact snapshots
- **Load_Aggregations.dtsx** - Pre-computed summary tables for reporting

### Utility Packages
- **Audit_Framework.dtsx** - Batch execution logging
- **ErrorHandling.dtsx** - Error row routing to flat files

## Legacy Anti-Patterns (Migration Challenges)

### 1. Monolithic Sequential Execution
- Master package runs all 30+ packages sequentially
- No parallelism between independent data flows (Extract POS and Extract Inventory could run simultaneously)
- Total runtime: 4+ hours

### 2. C# Script Tasks
- Complex business logic embedded in Script Tasks
- Hard to maintain, no unit testing
- Examples: REST API calls, complex data quality rules

### 3. Hardcoded File Paths
- FTP connections use UNC paths (\\FTP-SHARE\suppliers\)
- Error logs written to local disk (C:\ETLErrors\)
- Makes cloud migration challenging

### 4. Windows Authentication
- All SQL connections use Windows Authentication
- Requires on-premises Active Directory integration
- Not cloud-native

### 5. SSIS Package Configurations
- Connection strings in XML files (Connections.dtsConfig)
- Environment-specific settings scattered across config files
- Not source-control friendly

### 6. No Incremental Load for Some Sources
- CRM customers: full reload every night (inefficient)
- Inventory: full snapshot (acceptable for small datasets)

### 7. Error Handling
- Error rows written to flat files
- No automated alerting or monitoring
- Manual review required

### 8. Custom SCD Type 2 Implementation
- Mix of SSIS SCD wizard and stored procedures
- Inconsistent patterns across dimensions

## Database Objects

### SQL Scripts Included
- `StagingTables/01_Create_Staging_Tables.sql` - All staging DDL
- `DataWarehouse/01_Create_DataWarehouse_Schema.sql` - Star schema DDL
- `DataWarehouse/02_Create_Audit_Tables.sql` - Audit/logging tables
- `StoredProcedures/01_ETL_Stored_Procedures.sql` - SCD Type 2, merge logic, index management
- `IncrementalLoad/01_Incremental_Load_Patterns.sql` - Watermark tracking, change data capture queries

### Key Stored Procedures
- `dw.ProcessCustomerDimensionSCD2` - Slowly Changing Dimension Type 2 logic
- `dw.MergeFactSales` - Insert new sales facts with dimension lookups
- `dw.DropFactSalesIndexes` / `dw.RebuildFactSalesIndexes` - Performance optimization
- `audit.InitializeBatch` / `audit.CompleteBatch` - Execution tracking

## Sample Data
- `POSSales_Sample.csv` - 20 sample transactions
- `CRMCustomers_Sample.csv` - 14 customer records
- `SupplierPricing_Sample.csv` - Supplier cost data
- `InventorySnapshot_Sample.csv` - Stock levels

## Deployment (Legacy On-Premises)

### Prerequisites
- SQL Server 2016 with Integration Services
- SSISDB catalog configured
- SQL Server Agent for scheduling
- Access to all 5 source systems

### Deployment Steps
1. Deploy all .dtsx packages to SSISDB catalog
2. Create SSISDB environment with connection strings
3. Run SQL scripts to create DW schema (DataWarehouse/*.sql)
4. Run SQL scripts to create staging tables (StagingTables/*.sql)
5. Populate Date dimension (Transform_TimeDim.dtsx)
6. Create SQL Server Agent job for nightly execution of MasterOrchestrator.dtsx

## Scheduled Execution
- **Frequency**: Nightly at 2:00 AM
- **SQL Server Agent Job**: "Brightfield_ETL_Nightly"
- **Execution Order**: Master → Extract (all) → Transform (all) → Load (all)
- **Average Runtime**: 4 hours 15 minutes
- **Notifications**: Email on failure to ETL team

## Monitoring
- Query `audit.PackageExecution` for batch history
- Check `audit.ExtractLog` for source row counts
- Review flat files in C:\ETLErrors\ for data quality issues

## Migration to Azure Data Factory
This legacy system will be modernized to Azure Data Factory. Key transformation:
- SSIS packages → ADF pipelines with Mapping Data Flows
- Sequential execution → Parallel execution with dependencies
- C# Script Tasks → Azure Databricks notebooks or Azure Functions
- FTP flat files → SFTP connector to Azure Data Lake Storage
- Windows Auth → Managed Identity or Key Vault secrets
- SSIS SCD wizard → ADF Alter Row transformation
- On-premises → Self-hosted Integration Runtime for hybrid connectivity

## Contact
For questions about the legacy system, contact the Brightfield ETL Team.
