---
title: "SSIS to Azure Data Factory"
description: "Modernize legacy SSIS ETL pipelines to Azure Data Factory with mapping data flows and orchestration"
authors: ["marconsilva"]
category: "Data Modernization"
industry: "Retail & Consumer Goods"
services: ["Azure Data Factory", "Azure Data Lake Storage Gen2", "Azure Synapse Analytics"]
languages: ["SQL", "Python", "C#"]
frameworks: [".NET"]
modernizationTools: ["Azure Data Factory"]
agenticTools: []
tags: ["ssis", "etl", "data-factory", "azure-pipelines", "data-integration"]
extensions: ["github.copilot"]
thumbnail: ""
video: ""
version: "1.0.0"
screenshots:
  - path: "assets/screenshots/01-project-structure.html"
    alt: "Brightfield ETL project tree — Config, SQL, SSIS-Packages, SampleData directories"
  - path: "assets/screenshots/02-master-orchestrator-ssis.html"
    alt: "MasterOrchestrator.dtsx — sequential ETL pipeline: Init → Extract → Transform → Load → Complete"
  - path: "assets/screenshots/03-extract-pos-sales-dataflow.html"
    alt: "Extract_POS_Sales.dtsx — incremental OLE DB source → Derived Column → staging destination"
  - path: "assets/screenshots/04-transform-sales-facts-pipeline.html"
    alt: "Transform_SalesFacts.dtsx — four dimension lookups, calculated metrics, data quality split"
  - path: "assets/screenshots/05-scd-type2-customer-dimension.html"
    alt: "SCD Type 2 stored procedure — expire, insert new version, insert new customer"
  - path: "assets/screenshots/06-data-warehouse-star-schema.html"
    alt: "Star schema — DimDate, DimStore, DimProduct, DimCustomer, FactSales, FactInventory"
  - path: "assets/screenshots/07-load-factsales-index-strategy.html"
    alt: "Load_FactSales.dtsx — drop indexes, merge with dimension lookups, rebuild indexes"
  - path: "assets/screenshots/08-incremental-load-watermark.html"
    alt: "Watermark pattern — audit.WatermarkTable with Get/Update stored procedures"
  - path: "assets/screenshots/09-config-connections.html"
    alt: "SSIS configuration — connection strings, Production/Development environment variables"
---

# Legacy SSIS ETL to Azure Data Factory

## Overview

This lab modernizes legacy SQL Server Integration Services (SSIS) ETL pipelines to Azure Data Factory with mapping data flows and orchestration. The Brightfield Retail Group's ETL processes extract POS sales data, transform it through dimension lookups, and load a star-schema data warehouse.

## Project Structure

The SSIS solution is organized into Config, SQL, SSIS-Packages, and SampleData directories following the traditional SSIS project deployment model.

[View: Brightfield ETL Project Structure](assets/screenshots/01-project-structure.html)

*Open the HTML file in a browser to view the syntax-highlighted rendering.*

## SSIS Packages

### Master Orchestrator

MasterOrchestrator.dtsx is the top-level package that runs the sequential ETL pipeline: Init → Extract → Transform → Load → Complete. This maps to an ADF pipeline with activity dependencies.

[View: Master Orchestrator Package](assets/screenshots/02-master-orchestrator-ssis.html)

*Open the HTML file in a browser to view the syntax-highlighted rendering.*

### Extract — POS Sales Data Flow

Extract_POS_Sales.dtsx performs incremental extraction from OLE DB source using a watermark pattern, applies Derived Column transformations, and loads to a staging destination.

[View: Extract POS Sales Data Flow](assets/screenshots/03-extract-pos-sales-dataflow.html)

*Open the HTML file in a browser to view the syntax-highlighted rendering.*

### Transform — Sales Facts Pipeline

Transform_SalesFacts.dtsx performs four dimension lookups, calculates metrics, and splits data based on quality rules. This maps to ADF mapping data flows with lookup transformations.

[View: Transform Sales Facts Pipeline](assets/screenshots/04-transform-sales-facts-pipeline.html)

*Open the HTML file in a browser to view the syntax-highlighted rendering.*

## Data Warehouse Schema

### Star Schema

The target data warehouse uses a classic star schema with DimDate, DimStore, DimProduct, DimCustomer, FactSales, and FactInventory tables.

[View: Data Warehouse Star Schema](assets/screenshots/06-data-warehouse-star-schema.html)

*Open the HTML file in a browser to view the syntax-highlighted rendering.*

### SCD Type 2 Customer Dimension

The SCD Type 2 stored procedure handles customer dimension changes: expire current records, insert new versions, and insert new customers. This logic moves to ADF data flows with Alter Row transformations.

[View: SCD Type 2 Customer Dimension](assets/screenshots/05-scd-type2-customer-dimension.html)

*Open the HTML file in a browser to view the syntax-highlighted rendering.*

## Load Strategy

### FactSales Load with Index Management

Load_FactSales.dtsx drops indexes before loading, merges data with dimension lookups, and rebuilds indexes afterward. ADF handles this with pre/post-copy scripts.

[View: FactSales Load and Index Strategy](assets/screenshots/07-load-factsales-index-strategy.html)

*Open the HTML file in a browser to view the syntax-highlighted rendering.*

### Incremental Load Watermark

The watermark pattern uses an audit.WatermarkTable with Get/Update stored procedures to track incremental load boundaries. ADF has native watermark support via Tumbling Window triggers.

[View: Incremental Load Watermark Pattern](assets/screenshots/08-incremental-load-watermark.html)

*Open the HTML file in a browser to view the syntax-highlighted rendering.*

## Configuration

### Connection Strings and Environment Variables

SSIS uses package configurations for connection strings and Production/Development environment variables. ADF replaces this with linked services, parameterized datasets, and Azure Key Vault integration.

[View: SSIS Configuration and Connections](assets/screenshots/09-config-connections.html)

*Open the HTML file in a browser to view the syntax-highlighted rendering.*

## Screenshots

Visual walkthroughs of the SSIS packages and data warehouse objects. Open the HTML files in a browser for syntax-highlighted views.

| # | Screenshot | Description |
|---|-----------|-------------|
| 1 | [Project Structure](assets/screenshots/01-project-structure.html) | Brightfield ETL project tree — Config, SQL, SSIS-Packages, SampleData directories |
| 2 | [Master Orchestrator](assets/screenshots/02-master-orchestrator-ssis.html) | MasterOrchestrator.dtsx — sequential ETL pipeline: Init → Extract → Transform → Load → Complete |
| 3 | [Extract POS Sales](assets/screenshots/03-extract-pos-sales-dataflow.html) | Incremental OLE DB source → Derived Column → staging destination |
| 4 | [Transform Sales Facts](assets/screenshots/04-transform-sales-facts-pipeline.html) | Four dimension lookups, calculated metrics, data quality split |
| 5 | [SCD Type 2 Customer](assets/screenshots/05-scd-type2-customer-dimension.html) | Expire, insert new version, insert new customer stored procedure |
| 6 | [Star Schema](assets/screenshots/06-data-warehouse-star-schema.html) | DimDate, DimStore, DimProduct, DimCustomer, FactSales, FactInventory |
| 7 | [FactSales Load](assets/screenshots/07-load-factsales-index-strategy.html) | Drop indexes, merge with dimension lookups, rebuild indexes |
| 8 | [Incremental Watermark](assets/screenshots/08-incremental-load-watermark.html) | Watermark pattern with audit.WatermarkTable and stored procedures |
| 9 | [Configuration](assets/screenshots/09-config-connections.html) | Connection strings, Production/Development environment variables |

## Solution Walkthrough

This solution was generated using GitHub Copilot CLI (`gh copilot`) in autonomous mode. Each step used a single CLI prompt to analyze legacy code and generate ADF migration artifacts.

### Branch & Tags

- **Branch:** `solution-final` — contains all 9 steps
- **Tags:** `step-01-assessment` through `step-09-validation`

### Steps & CLI Prompts

#### Step 1: Inventory SSIS Packages
```bash
gh copilot -- -p "Analyze BrightfieldETL directory. Create ADF-Migration/01-assessment/ with ssis-package-inventory.md (table of all 11 SSIS packages with complexity ratings, dependency diagram, anti-patterns vs ADF equivalents, migration order) and source-to-target-mapping.md (column-level mappings from staging to DW tables)." --allow-all-tools --yolo
```
**Output:** [assets/outputs/step-01-assessment.txt](assets/outputs/step-01-assessment.txt)
**Files:** `ADF-Migration/01-assessment/ssis-package-inventory.md`, `source-to-target-mapping.md`

#### Step 2: Provision Azure Resources
```bash
gh copilot -- -p "Create Bicep IaC templates in ADF-Migration/02-infrastructure/ with main.bicep orchestrator and modules for data-factory, storage-account (ADLS Gen2), synapse, key-vault, self-hosted-ir, plus deploy.ps1 script." --allow-all-tools --yolo
```
**Output:** [assets/outputs/step-02-infrastructure.txt](assets/outputs/step-02-infrastructure.txt)
**Files:** `ADF-Migration/02-infrastructure/main.bicep`, `modules/*.bicep`, `deploy.ps1`

#### Step 3: Migrate Extractions
```bash
gh copilot -- -p "Convert 5 SSIS extract packages to ADF pipeline JSON. Create ADF-Migration/03-extract-pipelines/ with pipelines/ (POS watermark incremental, Oracle inventory, CRM full copy, REST ecommerce, SFTP supplier ForEach), linked-services/, and datasets/." --allow-all-tools --yolo
```
**Output:** [assets/outputs/step-03-extract-migration.txt](assets/outputs/step-03-extract-migration.txt)
**Files:** 5 pipeline JSONs, 6 linked service JSONs, 11 dataset JSONs

#### Step 4: Migrate Transformations
```bash
gh copilot -- -p "Convert SSIS transform packages to ADF Mapping Data Flows. Create ADF-Migration/04-transform-dataflows/ with data flow JSONs for sales facts (4 lookups + derived columns + conditional split), customer dim SCD2, data quality, inventory facts, plus a Databricks notebook for complex transformations." --allow-all-tools --yolo
```
**Output:** [assets/outputs/step-04-transform-migration.txt](assets/outputs/step-04-transform-migration.txt)
**Files:** 4 data flow JSONs, `notebooks/complex-transformations.py`

#### Step 5: Implement SCD Type 2
```bash
gh copilot -- -p "Create ADF-Migration/05-scd-type2/ with detailed SCD2 data flow (Exists + ConditionalSplit + AlterRow for expire/insert), test data SQL, and validation queries for date range overlaps and IsCurrent consistency." --allow-all-tools --yolo
```
**Output:** [assets/outputs/step-05-scd-type2.txt](assets/outputs/step-05-scd-type2.txt)
**Files:** `dataflow-scd2-customer.json`, `scd2-test-data.sql`, `scd2-validation.sql`

#### Step 6: Build Orchestration
```bash
gh copilot -- -p "Create ADF-Migration/06-orchestration/ with master pipeline (parallel extracts → sequential transforms → loads with Execute Pipeline activities and dependency conditions), error handler pipeline, nightly schedule trigger (2AM UTC), and tumbling window trigger." --allow-all-tools --yolo
```
**Output:** [assets/outputs/step-06-orchestration.txt](assets/outputs/step-06-orchestration.txt)
**Files:** `pipeline-master-orchestrator.json`, `pipeline-error-handler.json`, 2 trigger JSONs

#### Step 7: Add Incremental Loads
```bash
gh copilot -- -p "Create ADF-Migration/07-incremental-loads/ with watermark-based incremental pipeline for POS sales (Lookup→Copy→StoredProcedure), daily snapshot for inventory, CDC pattern for CRM, watermark setup SQL, and incremental load patterns documentation." --allow-all-tools --yolo
```
**Output:** [assets/outputs/step-07-incremental-loads.txt](assets/outputs/step-07-incremental-loads.txt)
**Files:** 3 pipeline JSONs, `watermark-setup.sql`, `incremental-load-patterns.md`

#### Step 8: Set Up Monitoring
```bash
gh copilot -- -p "Create ADF-Migration/08-monitoring/ with alert-rules.bicep, diagnostic-settings.bicep, Azure dashboard JSON, KQL log analytics queries, and automation runbook for incident response." --allow-all-tools --yolo
```
**Output:** [assets/outputs/step-08-monitoring.txt](assets/outputs/step-08-monitoring.txt)
**Files:** 2 Bicep files, `dashboard.json`, `log-analytics-queries.md`, `runbook-alert-handler.ps1`

#### Step 9: Validate
```bash
gh copilot -- -p "Create ADF-Migration/09-validation/ with SQL validation queries (row counts, checksums, SCD2 integrity), ADF validation pipeline, test scenarios document (28 test cases across 5 categories), and GitHub Actions CI/CD pipeline (validate → deploy-dev → validate-dev → staging → prod)." --allow-all-tools --yolo
```
**Output:** [assets/outputs/step-09-validation.txt](assets/outputs/step-09-validation.txt)
**Files:** `validation-queries.sql`, `pipeline-validation.json`, `test-scenarios.md`, `ci-cd-pipeline.yml`

### Migration Artifact Summary

| Step | Directory | Artifacts | SSIS Component Replaced |
|------|-----------|-----------|------------------------|
| 1 | `01-assessment/` | 2 markdown docs | — (planning) |
| 2 | `02-infrastructure/` | 7 Bicep/PS1 files | On-prem SQL Server infra |
| 3 | `03-extract-pipelines/` | 22 JSON files | 5 Extract DTSX packages |
| 4 | `04-transform-dataflows/` | 5 files | 3 Transform DTSX + Script Tasks |
| 5 | `05-scd-type2/` | 3 files | SCD wizard + stored proc |
| 6 | `06-orchestration/` | 4 JSON files | MasterOrchestrator.dtsx + SQL Agent |
| 7 | `07-incremental-loads/` | 5 files | Watermark + CT queries |
| 8 | `08-monitoring/` | 5 files | Audit_Framework.dtsx + flat file logs |
| 9 | `09-validation/` | 4 files | — (new capability) |
