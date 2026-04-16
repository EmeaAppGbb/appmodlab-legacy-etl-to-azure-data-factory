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
