# SSIS Package Inventory — Brightfield Retail Analytics

> **Assessment Date:** 2026-04-16
> **Source System:** SSIS 2019 (PackageFormatVersion 8) on `DW-SQL-SERVER`
> **Target Platform:** Azure Data Factory (ADF) / Synapse Pipelines

---

## 1. Package Inventory

| # | Package Name | Category | Source System | Destination | Key Transformations | Dependencies | Complexity |
|---|---|---|---|---|---|---|---|
| 1 | **MasterOrchestrator.dtsx** | Orchestration | — | — | Execute Package Tasks (chained E→T→L), Audit Init/Complete | All other packages | **High** |
| 2 | **Extract_POS_Sales.dtsx** | Extract | POS SQL Server (`POSTransactions`) | `staging.POSSales` | Incremental watermark query, Derived Column (audit cols), OLE DB bulk insert | audit.ExtractLog | **Med** |
| 3 | **Extract_Inventory.dtsx** | Extract | Oracle (`INVENTORY.STOCK_LEVELS`) | `staging.InventoryLevels` | Data Conversion (Oracle NUMBER → SQL INT/DECIMAL), ADO.NET source, OLE DB dest | — | **Med** |
| 4 | **Extract_CRM_Customers.dtsx** | Extract | CRM SQL Server (`CustomerCRM`) | `staging.CRMCustomers` | Truncate-and-load, Derived Column (FullName concat, timestamp), OLE DB bulk | — | **Low** |
| 5 | **Extract_Ecommerce_Orders.dtsx** | Extract | REST API (`api.brightfield-ecommerce.com`) | `staging.EcommerceOrders` | **C# Script Task** (HttpClient → JSON parse → bulk insert), SQL validation SP | staging.ValidateEcommerceOrders | **High** |
| 6 | **Extract_Supplier_Files.dtsx** | Extract | UNC File Share (`\\FTP-SHARE\suppliers\`) | `staging.SupplierPricing` | ForEach File Loop (CSV wildcard `pricing_*.csv`), Flat File Source | File system availability | **Med** |
| 7 | **Transform_DataQuality.dtsx** | Transform | `staging.*` (all staging tables) | `staging.*` (in-place) | **C# Script Task** (DQ rules), SP `dq.StandardizeAddresses`, SP `dq.DeduplicateCustomers` | All Extract packages | **High** |
| 8 | **Transform_CustomerDim.dtsx** | Transform | `staging.CRMCustomers` | `dw.DimCustomer` | SCD Type 2 (Slowly Changing Dimension wizard), OLE DB Command (expire rows), SP `dw.ProcessCustomerDimensionSCD2` | Extract_CRM_Customers, Transform_DataQuality | **High** |
| 9 | **Transform_SalesFacts.dtsx** | Transform | `staging.POSSales` | `dw.FactSales` + error flat file | 4× Lookup (Date, Store, Product, Customer keys), Derived Column (Revenue, Discount, NetRevenue), Conditional Split (DQ check) | All Dim transforms, Extract_POS_Sales | **High** |
| 10 | **Load_FactSales.dtsx** | Load | `staging.POSSales` (via SP) | `dw.FactSales` | Drop/Rebuild indexes, SP `dw.MergeFactSales`, UPDATE STATISTICS | Transform_SalesFacts | **Med** |
| 11 | **Audit_Framework.dtsx** | Utility | — | `audit.PackageExecution` | Parameterised SQL Tasks (Log Start/End), variable-driven row counts | — | **Low** |

### Packages Referenced in MasterOrchestrator but Not Present as .dtsx Files

These are called from the orchestrator but do not exist in the repo (stub/future packages):

| Package | Phase | Notes |
|---|---|---|
| `Transform_TimeDim.dtsx` | Transform | Date dimension population |
| `Transform_ProductDim.dtsx` | Transform | Product dimension SCD |
| `Transform_InventoryFacts.dtsx` | Transform | Inventory fact build |
| `Load_Dimensions.dtsx` | Load | Dimension table loader |
| `Load_FactInventory.dtsx` | Load | Inventory fact loader |
| `Load_Aggregations.dtsx` | Load | Pre-computed aggregation tables |

---

## 2. Dependency Diagram

```
                    ┌──────────────────────┐
                    │  MasterOrchestrator  │
                    └──────────┬───────────┘
                               │
            ┌──────────────────┼──────────────────┐
            ▼                  ▼                   ▼
    ┌───────────────┐  ┌───────────────┐  ┌───────────────┐
    │ EXTRACT PHASE │  │ (audit.Init)  │  │ (audit.Compl) │
    │  (parallel)   │  └───────────────┘  └───────────────┘
    └───────┬───────┘
            │
   ┌────────┼────────┬────────────┬───────────────┐
   ▼        ▼        ▼            ▼               ▼
┌───────┐┌───────┐┌──────────┐┌──────────────┐┌──────────────┐
│POS    ││Inven- ││CRM       ││Ecommerce     ││Supplier      │
│Sales  ││tory   ││Customers ││Orders        ││Files         │
│(OLEDB)││(Oracle)│(OLEDB)   ││(REST/Script) ││(CSV/ForEach) │
└───┬───┘└───┬───┘└────┬─────┘└──────┬───────┘└──────┬───────┘
    │        │         │             │               │
    └────────┴────┬────┴─────────────┴───────────────┘
                  ▼
       ┌─────────────────────┐
       │  TRANSFORM PHASE    │
       │  (sequential)       │
       └──────────┬──────────┘
                  │
         ┌────────┼──────────────────┐
         ▼        ▼                  ▼
   ┌──────────┐┌───────────────┐┌─────────────┐
   │DataQual- ││CustomerDim   ││SalesFacts   │
   │ity       ││(SCD Type 2)  ││(4× Lookup,  │
   │(Script+SP)│(SCD Wizard+SP)│ DerivedCol, │
   └─────┬────┘└───────┬──────┘│ CondSplit)  │
         │             │       └──────┬──────┘
         └─────────────┴──────────────┘
                       │
                       ▼
            ┌─────────────────────┐
            │    LOAD PHASE       │
            │   (sequential)      │
            └──────────┬──────────┘
                       │
              ┌────────┼────────┐
              ▼        ▼        ▼
        ┌──────────┐┌──────┐┌────────────┐
        │Dimensions││Fact  ││Aggregations│
        │          ││Sales ││            │
        └──────────┘└──────┘└────────────┘

  Legend:
    (OLEDB)   = OLE DB Connection
    (Oracle)  = ADO.NET Oracle Connection
    (REST)    = C# Script Task → HTTP
    (CSV)     = Flat File + ForEach Loop
    ────►     = Precedence Constraint (on-success)
```

### Execution Order (from precedence constraints)

```
1.  audit.InitializeBatch
2.  ├── Extract_POS_Sales
    ├── Extract_Inventory
    ├── Extract_CRM_Customers
    ├── Extract_Ecommerce_Orders
    └── Extract_Supplier_Files      ← all 5 in parallel
3.  Transform_DataQuality            ← must run first in transform phase
4.  Transform_TimeDim*
5.  Transform_CustomerDim
6.  Transform_ProductDim*
7.  Transform_SalesFacts
8.  Transform_InventoryFacts*
9.  Load_Dimensions*
10. Load_FactSales
11. Load_FactInventory*
12. Load_Aggregations*
13. audit.CompleteBatch

(* = referenced in orchestrator but .dtsx not present in repo)
```

---

## 3. Anti-Patterns → ADF Equivalents

| # | SSIS Anti-Pattern (found) | Where | Risk | ADF / Azure Equivalent |
|---|---|---|---|---|
| 1 | **C# Script Task for REST API calls** | `Extract_Ecommerce_Orders` | Untestable, opaque, no retry logic, credential leak risk | **ADF REST Linked Service + Copy Activity** with pagination & OAuth; or **Azure Function** activity for complex parsing |
| 2 | **C# Script Task for data quality rules** | `Transform_DataQuality` | Business logic buried in package XML, no unit testing | **ADF Mapping Data Flow** (expressions, derived columns, assert transforms) or **Azure SQL Stored Procedures** |
| 3 | **Hardcoded connection strings** | All packages, `Connections.dtsConfig` | Credential exposure, environment drift | **ADF Linked Services + Azure Key Vault** for secrets; parameterise with ADF Global Parameters |
| 4 | **Hardcoded file paths** (`\\FTP-SHARE`, `C:\ETLErrors`) | `Extract_Supplier_Files`, `Transform_SalesFacts`, Config | Environment-specific, no cloud portability | **Azure Blob Storage / ADLS Gen2** Linked Service; parameterise paths |
| 5 | **ForEach File Loop over UNC share** | `Extract_Supplier_Files` | Network dependency, no cloud equivalent | **ADF GetMetadata + ForEach Activity** over Blob/ADLS container |
| 6 | **SSIS Slowly Changing Dimension wizard** | `Transform_CustomerDim` | Generates row-by-row OLEDB Command (slow), hard to maintain | **ADF Mapping Data Flow** with Alter Row transform; or **MERGE** in Stored Procedure (already exists as `dw.ProcessCustomerDimensionSCD2`) |
| 7 | **OLE DB Command for row-by-row updates** | `Transform_CustomerDim` | N+1 query anti-pattern, very slow at scale | **Set-based MERGE** in stored procedure called via ADF Stored Procedure activity |
| 8 | **Error rows written to local flat file** | `Transform_SalesFacts` | Files lost on server crash, not queryable | **ADF Data Flow error rows → Azure SQL audit table** or **ADLS error partition** |
| 9 | **Drop/Rebuild indexes around load** | `Load_FactSales` | Blocking, manual maintenance | **ADF Pre/Post-copy scripts** on Copy Activity; or **Stored Procedure activities** (keep pattern but automate) |
| 10 | **Integrated Security (SSPI) everywhere** | All OLEDB connections | Won't work in cloud, tied to Windows AD | **Azure AD Managed Identity** or **SQL Authentication via Key Vault** |
| 11 | **SQLNCLI11.1 (deprecated provider)** | All OLEDB connections | End-of-support, no cloud support | **MSOLEDBSQL** for on-prem IR; or native ADF connectors |
| 12 | **Package-level variables for API keys** | `Extract_Ecommerce_Orders` | Secrets stored in package XML | **Azure Key Vault** secret references in ADF Linked Services |
| 13 | **Sequential orchestration only** | `MasterOrchestrator` | Extract→Transform→Load fully serial per phase | ADF supports **parallel + dependency-based execution** within and across phases |

---

## 4. Recommended Migration Order

Migration should follow a **bottom-up, dependency-aware** order — utilities first, then extract, transform, load, and finally the orchestrator.

| Wave | Package(s) | Rationale | ADF Artefacts |
|---|---|---|---|
| **Wave 0** | Audit_Framework | No dependencies; foundational for all other pipelines | Stored Procedure activities + ADF pipeline monitoring |
| **Wave 1** | Extract_CRM_Customers, Extract_POS_Sales | Simplest extracts (OLEDB→OLEDB, standard patterns) | Copy Activities with IR, Linked Services, Key Vault |
| **Wave 2** | Extract_Inventory | Cross-platform (Oracle→SQL), needs ADO.NET parity | ADF Oracle Connector + Self-hosted IR |
| **Wave 3** | Extract_Supplier_Files | File-based, needs storage migration (UNC→Blob) | GetMetadata + ForEach + Copy (Blob/ADLS) |
| **Wave 4** | Extract_Ecommerce_Orders | Highest risk — C# Script Task rewrite | ADF REST Linked Service or Azure Function |
| **Wave 5** | Transform_DataQuality | C# Script rewrite + SP calls | Mapping Data Flow or Stored Procedures |
| **Wave 6** | Transform_CustomerDim | SCD Type 2 — keep SP, retire SCD wizard | Stored Procedure activity (reuse `dw.ProcessCustomerDimensionSCD2`) |
| **Wave 7** | Transform_SalesFacts | Complex data flow with 4 lookups | Mapping Data Flow (Lookup + DerivedColumn + ConditionalSplit) |
| **Wave 8** | Load_FactSales | Index management + merge SP | Stored Procedure activities (sequential) |
| **Wave 9** | MasterOrchestrator | Top-level orchestration; depends on all others | ADF Master Pipeline with Execute Pipeline activities |

### Migration Complexity Summary

| Complexity | Count | Packages |
|---|---|---|
| **Low** | 2 | Audit_Framework, Extract_CRM_Customers |
| **Medium** | 3 | Extract_POS_Sales, Extract_Inventory, Extract_Supplier_Files, Load_FactSales |
| **High** | 5 | MasterOrchestrator, Extract_Ecommerce_Orders, Transform_DataQuality, Transform_CustomerDim, Transform_SalesFacts |

---

## 5. Connection Inventory

| Connection Name | Type | Server / Endpoint | Database / Path | Auth Method | ADF Migration |
|---|---|---|---|---|---|
| DW_SQL_Server / DW_Connection | OLEDB (SQLNCLI11.1) | DW-SQL-SERVER | BrightfieldDW | Windows SSPI | Azure SQL Linked Service + Managed Identity |
| POS_Source | OLEDB (SQLNCLI11.1) | POS-SERVER | POSTransactions | Windows SSPI | SQL Server Linked Service + Self-hosted IR |
| CRM_Source | OLEDB (SQLNCLI11.1) | CRM-SERVER | CustomerCRM | Windows SSPI | SQL Server Linked Service + Self-hosted IR |
| Oracle_Inventory | ADO.NET (Oracle) | INVDB | INVENTORY schema | SQL Auth (etl_reader) | Oracle Linked Service + Self-hosted IR + Key Vault |
| Supplier_CSV | Flat File | `\\FTP-SHARE\suppliers\` | `pricing_*.csv` | Network share | Azure Blob / ADLS Gen2 Linked Service |
| E-commerce API | HTTP (in Script) | `api.brightfield-ecommerce.com` | `/v1/orders` | API Key header | REST Linked Service + Key Vault |
| Error Log | Flat File | `C:\ETLErrors\` | `.txt` files | Local file system | ADLS Gen2 error container |

---

## 6. Environment Configuration

| Parameter | Dev | Prod | ADF Equivalent |
|---|---|---|---|
| DW_ServerName | DW-DEV-SQL | DW-PROD-SQL | ADF Global Parameter / Linked Service parameterisation |
| DW_DatabaseName | BrightfieldDW_Dev | BrightfieldDW | Linked Service parameter |
| POS_ServerName | POS-DEV-SQL | POS-PROD-SQL | Linked Service parameter |
| CRM_ServerName | CRM-DEV-SQL | CRM-PROD-SQL | Linked Service parameter |
| Oracle_ConnectionString | INVDB-DEV | INVDB-PROD | Linked Service + Key Vault per environment |
| FTP_SharePath | `\\DEV-FTP\suppliers\` | `\\PROD-FTP\suppliers\` | ADLS container path parameter |
| ApiEndpoint | api-dev.brightfield… | api.brightfield… | REST Linked Service Base URL parameter |
| ApiKey | DEV_KEY | (sensitive) | **Azure Key Vault** secret |
| ErrorLogPath | `C:\ETLErrors\` | `\\PROD-LOGS\ETLErrors\` | ADLS Gen2 path parameter |
