# 🌆 LEGACY ETL TO AZURE DATA FACTORY 🚀

```
███████╗████████╗██╗         ██████╗ ██╗██████╗ ███████╗██╗     ██╗███╗   ██╗███████╗
██╔════╝╚══██╔══╝██║         ██╔══██╗██║██╔══██╗██╔════╝██║     ██║████╗  ██║██╔════╝
█████╗     ██║   ██║         ██████╔╝██║██████╔╝█████╗  ██║     ██║██╔██╗ ██║█████╗  
██╔══╝     ██║   ██║         ██╔═══╝ ██║██╔═══╝ ██╔══╝  ██║     ██║██║╚██╗██║██╔══╝  
███████╗   ██║   ███████╗    ██║     ██║██║     ███████╗███████╗██║██║ ╚████║███████╗
╚══════╝   ╚═╝   ╚══════╝    ╚═╝     ╚═╝╚═╝     ╚══════╝╚══════╝╚═╝╚═╝  ╚═══╝╚══════╝
                        🌊 DATA FLOWING TO THE CLOUD 🌊
```

[![PIPELINE RUNNING 🔄](https://img.shields.io/badge/PIPELINE-RUNNING-ff69b4?style=for-the-badge&logo=azuredataexplorer)](https://azure.microsoft.com/en-us/products/data-factory/)
[![DATA FLOWING 🌊](https://img.shields.io/badge/DATA-FLOWING-00d9ff?style=for-the-badge&logo=databricks)](https://azure.microsoft.com/en-us/products/synapse-analytics/)
[![TRANSFORM COMPLETE ⚙️](https://img.shields.io/badge/TRANSFORM-COMPLETE-ffb86c?style=for-the-badge&logo=apache-spark)](https://docs.microsoft.com/en-us/azure/data-factory/concepts-data-flow-overview)

## 🎮 MISSION BRIEFING

Welcome to the **DATA PIPELINE ARCADE** 🕹️! Your mission: Transform a legacy SSIS-based ETL factory into a modern Azure Data Factory powerhouse! 💪✨

Transform legacy data pipelines like an arcade champion — extract from multiple sources, transform with visual flows, load into the cloud data warehouse, and orchestrate everything with precision timing! 🎯

### 🏢 Business Domain
**Brightfield Retail Analytics** — A national retail analytics data warehouse processing POS, inventory, CRM, e-commerce, and supplier data through 30+ SSIS packages nightly! 🛒📊

### 🎯 What You'll Master
- 🔍 **EXTRACT** data from legacy sources to Azure
- ⚙️ **TRANSFORM** complex SSIS flows to Mapping Data Flows
- 📥 **LOAD** into modern cloud data warehouses
- 🎭 **ORCHESTRATE** pipelines with parallel execution
- 📡 **MONITOR** with alerts and dashboards
- 🔄 **INCREMENTAL LOADS** for efficiency
- 🎨 **SCD TYPE 2** dimension tracking

---

## 🌈 TECH STACK TRANSFORMATION

### 🕹️ LEGACY ARCADE (Before)
```
🗄️  SQL Server Integration Services (SSIS) 2016
📦  30+ SSIS Packages in SSISDB
⏰  SQL Server Agent Scheduling
🔌  Multiple Source Connectors (SQL, Oracle, CSV, REST, Excel)
💾  SQL Server 2016 Data Warehouse
🖥️  SSIS Script Tasks (C#)
```

### ✨ CLOUD ARCADE (After)
```
☁️  Azure Data Factory (Pipelines + Mapping Data Flows)
🌊  Azure Data Lake Storage Gen2 (Staging)
🎯  Azure Synapse Analytics (Data Warehouse)
⚡  Azure Databricks (Complex Transformations)
📊  Azure Monitor + Alerts
🔄  Self-Hosted Integration Runtime (Hybrid Connectivity)
🚀  Parallel Execution & Dependency Management
```

---

## 🗺️ DATA FLOW ARCHITECTURE

```
┌─────────────────────────────────────────────────────────────────┐
│                    🎮 SOURCE SYSTEMS 🎮                         │
│  💳 POS  │  📦 Inventory  │  👥 CRM  │  🛍️ E-commerce │  📄 CSV │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│              🔄 AZURE DATA FACTORY PIPELINES 🔄                 │
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │ 📡 EXTRACT   │→ │ ⚙️ TRANSFORM │→ │ 📥 LOAD      │         │
│  │ Copy Activities│ │ Mapping Flows│ │ Bulk Insert  │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
│                                                                  │
│  🎭 Master Orchestrator Pipeline + Dependencies                │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│               🌊 STAGING LAYER (ADLS Gen2) 🌊                   │
│              📊 Parquet Files + Delta Tables 📊                 │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│           🎯 AZURE SYNAPSE ANALYTICS DW 🎯                      │
│                                                                  │
│  ⭐ FactSales      │  ⭐ FactInventory                          │
│  🎨 DimCustomer    │  🎨 DimProduct  │  🎨 DimStore            │
│                   (SCD Type 2)                                   │
└─────────────────────────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│              📡 MONITORING & ALERTS 📡                          │
│         Azure Monitor + ADF Monitor Dashboards                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📁 REPOSITORY STRUCTURE

```
appmodlab-legacy-etl-to-azure-data-factory/
├── 📋 README.md                          ← You are here! 🌟
├── 📖 APPMODLAB.md                       ← Full lab guide & steps
├── 🔧 BrightfieldETL/                    ← Legacy SSIS packages
│   ├── SSIS-Packages/
│   │   ├── Master/
│   │   │   └── MasterOrchestrator.dtsx   ← 🎭 Master package
│   │   ├── Extract/                      ← 📡 Source extraction
│   │   │   ├── Extract_POS_Sales.dtsx
│   │   │   ├── Extract_Inventory.dtsx
│   │   │   ├── Extract_CRM_Customers.dtsx
│   │   │   ├── Extract_Ecommerce_Orders.dtsx
│   │   │   └── Extract_Supplier_Files.dtsx
│   │   ├── Transform/                    ← ⚙️ Data transformations
│   │   │   ├── Transform_SalesFacts.dtsx
│   │   │   ├── Transform_InventoryFacts.dtsx
│   │   │   ├── Transform_CustomerDim.dtsx (SCD Type 2)
│   │   │   ├── Transform_ProductDim.dtsx
│   │   │   ├── Transform_TimeDim.dtsx
│   │   │   └── Transform_DataQuality.dtsx
│   │   ├── Load/                         ← 📥 DW loading
│   │   │   ├── Load_FactSales.dtsx
│   │   │   ├── Load_FactInventory.dtsx
│   │   │   ├── Load_Dimensions.dtsx
│   │   │   └── Load_Aggregations.dtsx
│   │   └── Utility/                      ← 🛠️ Audit & errors
│   │       ├── Audit_Framework.dtsx
│   │       └── ErrorHandling.dtsx
│   ├── SQL/                              ← 🗄️ Database scripts
│   │   ├── StagingTables/
│   │   ├── DataWarehouse/
│   │   ├── StoredProcedures/
│   │   └── IncrementalLoad/
│   ├── Config/                           ← ⚙️ SSIS configurations
│   └── Documentation/                    ← 📚 Legacy docs
├── 🚀 ADF-Pipelines/                     ← Azure Data Factory
│   ├── pipelines/                        ← 🔄 Pipeline definitions
│   ├── dataflows/                        ← ⚙️ Mapping Data Flows
│   ├── datasets/                         ← 📊 Dataset definitions
│   ├── linkedServices/                   ← 🔌 Connections
│   └── ARM-Templates/                    ← 📦 Deployment templates
├── 📓 Databricks-Notebooks/              ← Complex transformations
├── 🏗️ infrastructure/                    ← IaC (Bicep/ARM)
│   ├── adf.bicep
│   ├── adls.bicep
│   ├── synapse.bicep
│   └── main.bicep
└── 🔁 .github/workflows/                 ← CI/CD pipelines
    └── deploy-adf.yml
```

---

## 🎯 BRANCH STRATEGY

Each branch represents a level in your data pipeline arcade! 🕹️

| Branch | Description | Status |
|--------|-------------|--------|
| 🌿 `main` | Complete lab with full documentation | ✅ GAME COMPLETE |
| 🕹️ `legacy` | SSIS packages, SQL scripts, configs | 🔴 LEGACY MODE |
| ✨ `solution` | ADF pipelines, data flows, notebooks | 💎 SOLUTION |
| 🔍 `step-1-assessment` | SSIS inventory & migration analysis | 📋 LEVEL 1 |
| 🏗️ `step-2-infrastructure` | Azure resource provisioning | ☁️ LEVEL 2 |
| 📡 `step-3-extract-migration` | Copy Activities for extraction | 📥 LEVEL 3 |
| ⚙️ `step-4-transform-migration` | Mapping Data Flows | 🔄 LEVEL 4 |
| 🎭 `step-5-orchestration` | Master pipeline & monitoring | 🎯 LEVEL 5 |

---

## 🚀 QUICK START

### 🎮 LEVEL 1: Pre-Flight Check
```powershell
# 🔍 Check your power-ups (prerequisites)
az --version                    # Azure CLI installed? ✅
git --version                   # Git ready? ✅
code --version                  # VS Code locked and loaded? ✅
```

### 🎮 LEVEL 2: Clone the Arcade
```powershell
# 📦 Download the game
git clone https://github.com/EmeaAppGbb/appmodlab-legacy-etl-to-azure-data-factory.git
cd appmodlab-legacy-etl-to-azure-data-factory

# 🌿 Check out the legacy branch to see SSIS packages
git checkout legacy
```

### 🎮 LEVEL 3: Load the Mission
```powershell
# 📖 Open the full lab guide
code APPMODLAB.md

# 🎯 Or view in your browser
start APPMODLAB.md
```

### 🎮 LEVEL 4: Deploy Azure Resources
```powershell
# 🏗️ Provision your cloud infrastructure
cd infrastructure
az login
az deployment group create \
  --resource-group rg-brightfield-etl \
  --template-file main.bicep \
  --parameters environment=dev

# 🎉 INFRASTRUCTURE DEPLOYED! ✨
```

### 🎮 LEVEL 5: Start Pipeline Migration
Follow the detailed steps in **APPMODLAB.md** to:
1. 🔍 **Assess** SSIS packages
2. 📡 **Migrate** extraction logic
3. ⚙️ **Convert** transformations
4. 🎭 **Orchestrate** pipelines
5. 📊 **Monitor** and validate

---

## 🎓 LEARNING OBJECTIVES

By completing this arcade challenge, you'll unlock these achievements! 🏆

- 🔍 **SSIS Inspector** — Assess SSIS packages for ADF migration complexity
- 🔄 **Flow Converter** — Transform SSIS data flows to ADF Mapping Data Flows
- ⚡ **Incremental Master** — Implement watermark-based incremental loads
- 🎨 **SCD Wizard** — Handle Slowly Changing Dimensions (Type 2) in ADF
- 📡 **Pipeline Orchestrator** — Build dependency chains with parallel execution
- 🚨 **Monitor Guardian** — Set up alerts, logging, and dashboards
- ✅ **Data Validator** — Ensure output accuracy between SSIS and ADF

---

## 📋 PREREQUISITES

### 💻 Software & Tools
- 🌩️ Azure subscription with Contributor access
- 🔧 Azure CLI or Azure PowerShell
- 💼 Azure Data Studio or SQL Server Management Studio (SSMS)
- 🎨 Visual Studio Code (recommended)
- 📦 Git
- 🗄️ SQL Server with SSISDB (for legacy package testing)

### 🎯 Knowledge
- 🔹 **Required:**
  - SSIS development experience
  - Basic SQL and data warehousing concepts
  - Understanding of ETL patterns
- 🔸 **Nice to Have:**
  - Azure Data Factory familiarity
  - Star schema design
  - Azure Synapse Analytics knowledge

---

## ⏱️ ESTIMATED DURATION

**Total Time:** ⏰ 6–8 hours

| Phase | Duration | Description |
|-------|----------|-------------|
| 🔍 Assessment | 1 hour | Inventory SSIS packages & dependencies |
| 🏗️ Infrastructure | 1 hour | Provision Azure resources |
| 📡 Extract Migration | 2 hours | Convert to Copy Activities |
| ⚙️ Transform Migration | 2 hours | Build Mapping Data Flows |
| 🎭 Orchestration | 1.5 hours | Master pipeline & monitoring |
| ✅ Validation | 0.5 hour | Test & compare outputs |

---

## 🎨 KEY CONCEPTS

### 🔄 SSIS → ADF Migration Patterns

| SSIS Component | ADF Equivalent | Transformation |
|----------------|----------------|----------------|
| 📦 SSIS Package | 🔄 ADF Pipeline | Orchestration container |
| 🌊 Data Flow Task | ⚙️ Mapping Data Flow | Visual transformations |
| 📋 Execute SQL Task | 🗄️ Stored Procedure Activity | SQL execution |
| 📂 File System Task | 📁 File Activity | File operations |
| 🔧 Script Task (C#) | ⚡ Azure Function/Databricks | Custom logic |
| 🎭 Package Configurations | 🔑 Parameters & Variables | Dynamic values |
| ⏰ SQL Agent Job | ⏲️ ADF Trigger | Scheduling |

### ⚙️ Transformation Patterns

```
🔄 SLOWLY CHANGING DIMENSION (TYPE 2)
   SSIS: Custom SCD Component
   ADF:  Alter Row transformation + Conditional Split

🌊 INCREMENTAL LOAD
   SSIS: Execute SQL Task + Variables
   ADF:  Lookup Activity + Watermark pattern

📊 DATA QUALITY
   SSIS: Conditional Split + Error Output
   ADF:  Assert transformation + Failed rows handling

🎯 AGGREGATIONS
   SSIS: Aggregate transformation
   ADF:  Aggregate transformation in Mapping Data Flow
```

---

## 🌟 WHAT'S INCLUDED

### 📦 Legacy Assets (branch: `legacy`)
- ✅ 30+ working SSIS packages demonstrating all ETL patterns
- ✅ Complete SQL Server data warehouse schema (star schema)
- ✅ Sample source data across 5 source systems
- ✅ Docker-based SQL Server with SSISDB
- ✅ SSIS configurations and environment variables

### 🚀 Modernized Solution (branch: `solution`)
- ✅ Complete ADF pipeline definitions (ARM templates)
- ✅ Mapping Data Flows for all transformations
- ✅ Copy Activities with incremental load patterns
- ✅ SCD Type 2 implementation in ADF
- ✅ Databricks notebooks for complex transformations
- ✅ Monitoring dashboards and alert configurations

### 📖 Documentation
- ✅ **APPMODLAB.md** — Complete step-by-step lab guide
- ✅ **README.md** — This overview (you're reading it! 👀)
- ✅ SSIS-to-ADF component mapping reference
- ✅ Mapping Data Flow transformation guide
- ✅ Data validation procedures

### 🏗️ Infrastructure as Code
- ✅ Bicep templates for all Azure resources
- ✅ Azure Data Factory
- ✅ Azure Data Lake Storage Gen2
- ✅ Azure Synapse Analytics
- ✅ Azure Databricks workspace
- ✅ Self-Hosted Integration Runtime VM

### 🔁 CI/CD
- ✅ GitHub Actions workflow for ADF deployment
- ✅ Pipeline trigger management
- ✅ Environment-specific configurations

---

## 🎯 DATA WAREHOUSE SCHEMA

### ⭐ FACT TABLES

**FactSales** 💰
- 📅 DateKey, 🏪 StoreKey, 📦 ProductKey, 👤 CustomerKey
- 🔢 Quantity, Revenue, Discount, Cost, TransactionId

**FactInventory** 📦
- 📅 DateKey, 🏪 StoreKey, 📦 ProductKey
- 🔢 QuantityOnHand, QuantityOnOrder, DaysOfSupply

### 🎨 DIMENSION TABLES

**DimCustomer** 👥 (SCD Type 2)
- CustomerKey, CustomerId, Name, Email
- Segment, LoyaltyTier
- StartDate, EndDate, IsCurrent

**DimProduct** 📦 (SCD Type 2)
- ProductKey, ProductId, Name
- Category, SubCategory, Brand, UnitPrice
- StartDate, EndDate, IsCurrent

**DimStore** 🏪
- StoreKey, StoreId, Name, Address
- City, State, Region, Format

**DimDate** 📅
- DateKey, Date, DayOfWeek, Month, Quarter, Year
- IsHoliday, FiscalPeriod

---

## 🚨 LEGACY ANTI-PATTERNS TO FIX

Transform these arcade bugs into cloud features! 🐛 → ✨

| 🔴 Legacy Problem | ✅ ADF Solution |
|-------------------|-----------------|
| 🐌 Sequential execution | ⚡ Parallel pipeline execution |
| 🔧 C# Script Tasks (no testing) | 📓 Databricks notebooks (unit testable) |
| 🗂️ Hardcoded file paths | 🔑 Parameterized linked services |
| 🔐 Windows Authentication | 🎫 Azure AD + Managed Identity |
| 📄 XML configurations | 🔧 ADF parameters & variables |
| 🔄 Full table reloads | ⚡ Incremental watermark loads |
| 📁 Error files (no alerts) | 🚨 Azure Monitor alerts |
| 🎨 Custom SCD components | ⚙️ Built-in Alter Row transformation |
| 📊 Embedded data quality | 🎯 Reusable Assert transformations |

---

## 🎬 MONITORING & OBSERVABILITY

### 📊 PIPELINE RUNNING 🔄
Monitor your data flows in real-time! 

- 📡 **ADF Monitor Dashboard** — Real-time pipeline execution
- 📈 **Azure Monitor Metrics** — Performance tracking
- 🚨 **Alerts** — Failure notifications to Teams/Email
- 📝 **Diagnostic Logs** — Detailed execution logs to Log Analytics
- 🔍 **Data Flow Debug** — Interactive debugging mode

### 🌊 DATA FLOWING 🌊
Track data movement at every stage!

```
📡 Source → 🌊 Staging (ADLS) → ⚙️ Transform → 📥 Load → 🎯 DW
     ↓           ↓                  ↓            ↓         ↓
  Monitor     Monitor           Monitor      Monitor   Validate
```

---

## ✅ VALIDATION & SUCCESS CRITERIA

Your pipeline is complete when all lights are green! 🟢

- ✅ **EXTRACT** — All source systems connected, data flowing to staging
- ✅ **TRANSFORM** — Mapping Data Flows produce identical output to SSIS
- ✅ **LOAD** — Data warehouse tables populated correctly
- ✅ **SCD Type 2** — Historical tracking works (multiple versions per dimension)
- ✅ **Incremental Loads** — Only changed data processed (not full reloads)
- ✅ **Orchestration** — Master pipeline handles dependencies and parallelism
- ✅ **Monitoring** — Alerts fire on failures, dashboards show status
- ✅ **Performance** — ADF pipelines run faster than SSIS (parallel execution!)

---

## 🎓 ADDITIONAL RESOURCES

### 📚 Documentation
- 🔗 [Azure Data Factory Documentation](https://docs.microsoft.com/en-us/azure/data-factory/)
- 🔗 [Mapping Data Flows Guide](https://docs.microsoft.com/en-us/azure/data-factory/concepts-data-flow-overview)
- 🔗 [SSIS Migration Guide](https://docs.microsoft.com/en-us/azure/data-factory/ssis-migration-guide)
- 🔗 [Azure Synapse Analytics](https://docs.microsoft.com/en-us/azure/synapse-analytics/)
- 🔗 [Incremental Load Patterns](https://docs.microsoft.com/en-us/azure/data-factory/tutorial-incremental-copy-overview)

### 🎥 Learning Paths
- 🔗 [MS Learn: Azure Data Factory](https://docs.microsoft.com/en-us/learn/paths/data-integration-scale-azure-data-factory/)
- 🔗 [MS Learn: Data Engineering on Azure](https://docs.microsoft.com/en-us/learn/paths/azure-data-engineer/)

### 🛠️ Tools
- 🔗 [Azure Data Studio](https://docs.microsoft.com/en-us/sql/azure-data-studio/)
- 🔗 [Azure Storage Explorer](https://azure.microsoft.com/en-us/features/storage-explorer/)
- 🔗 [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/)

---

## 🤝 CONTRIBUTING

Want to level up this arcade? We welcome contributions! 🎮

1. 🍴 Fork the repository
2. 🌿 Create a feature branch (`git checkout -b feature/amazing-improvement`)
3. 💾 Commit your changes (`git commit -m 'Add amazing improvement'`)
4. 🚀 Push to the branch (`git push origin feature/amazing-improvement`)
5. 🎯 Open a Pull Request

---

## 📜 LICENSE

This lab is part of the **GBB App Modernization Labs** collection.

Copyright © Microsoft Corporation. All rights reserved.

---

## 🎉 ACHIEVEMENT UNLOCKED

```
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║          🏆 DATA PIPELINE MODERNIZATION MASTER 🏆           ║
║                                                              ║
║  You've discovered the legendary SSIS to ADF migration lab! ║
║                                                              ║
║           Transform legacy → Transform the future            ║
║                                                              ║
║              🌊 DATA FLOWS TO THE CLOUD 🌊                  ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
```

---

<div align="center">

### 🌟 READY TO TRANSFORM YOUR DATA PIPELINES? 🌟

**START THE LAB** → Open `APPMODLAB.md` and begin your journey! 🚀

---

**PIPELINE RUNNING** 🔄 | **DATA FLOWING** 🌊 | **TRANSFORM COMPLETE** ⚙️ | **LOAD SUCCESSFUL** 📥

Made with 💜 by the GBB App Modernization Team

🎮 **GAME ON!** 🎮

</div>
