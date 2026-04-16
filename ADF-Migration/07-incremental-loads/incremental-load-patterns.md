# Incremental Load Patterns — ADF Migration

> Migrated from: `BrightfieldETL/SQL/IncrementalLoad/01_Incremental_Load_Patterns.sql`

## Overview

This directory contains Azure Data Factory (ADF) pipeline definitions that replace the legacy SSIS incremental load patterns from the Brightfield ETL system. Each pipeline uses a centralized **watermark table** (`adf.IncrementalWatermark`) to track the last-processed position for each source.

## Patterns

### 1. Watermark-Based Incremental (POS Sales)

**Pipeline:** `PL_Incremental_POS_Sales`
**File:** `pipeline-incremental-pos-sales.json`

Classic high-water-mark pattern using `TransactionDate`:

```
Lookup Old Watermark ──┐
                       ├──► Copy Delta ──► Update Watermark
Lookup New Watermark ──┘
```

| Step | ADF Activity | Purpose |
|------|-------------|---------|
| 1 | `LookupOldWatermark` | Read last-processed `TransactionDate` from watermark table |
| 2 | `LookupNewWatermark` | Read current MAX(`TransactionDate`) from source |
| 3 | `RecordRunStart` | Log pipeline start time |
| 4 | `CopyIncrementalPOSSales` | Copy rows where `TransactionDate` is between old and new watermark |
| 5 | `UpdateWatermark` | Advance watermark to new value; record rows processed |
| 6 | `FailureNotification` | Alert on copy failure via webhook |

**Legacy SSIS equivalent:** `Extract_POS_Sales` package used `MAX(TransactionDate)` from `staging.POSSales` as the watermark.

---

### 2. Daily Snapshot (Inventory)

**Pipeline:** `PL_Incremental_Inventory`
**File:** `pipeline-incremental-inventory.json`

Full snapshot of current inventory levels taken once per day:

```
Lookup Last Snapshot ──┐
                       ├──► If Not Loaded ──► Delete Old ──► Copy Snapshot ──► Update Watermark
Check Snapshot Exists ─┘
```

| Step | ADF Activity | Purpose |
|------|-------------|---------|
| 1 | `LookupLastSnapshot` | Read last snapshot date from watermark table |
| 2 | `SetSnapshotDate` | Determine today's snapshot date |
| 3 | `CheckSnapshotExists` | Idempotency check — skip if already loaded |
| 4 | `IfSnapshotNotLoaded` | Conditional branch |
| 4a | `DeleteExistingSnapshot` | Clean up partial loads for the date |
| 4b | `CopyDailyInventorySnapshot` | Full extract of current inventory state |
| 4c | `UpdateSnapshotWatermark` | Advance watermark to snapshot date |

**Key design choice:** The snapshot is idempotent — re-running for the same date replaces the existing data rather than duplicating it.

**Legacy SSIS equivalent:** Used `MAX(SnapshotDate)` from `staging.InventoryLevels`.

---

### 3. Change Data Capture / CDC (CRM Customers)

**Pipeline:** `PL_Incremental_CRM_CDC`
**File:** `pipeline-incremental-crm-cdc.json`

Timestamp-based CDC that classifies each changed row as Insert (I), Update (U), or Delete (D):

```
Lookup Last CDC Watermark ──┐
                            ├──► Check Changes ──► If Changes ──► Copy CDC ──► Apply CDC ──► Update Watermark
Lookup Current Max Modified ┘
```

| Step | ADF Activity | Purpose |
|------|-------------|---------|
| 1 | `LookupLastCDCWatermark` | Read last sync position |
| 2 | `LookupCurrentMaxModified` | Get current MAX(`LastModifiedDate`) from source |
| 3 | `CheckForChanges` | Count changed rows (skip processing if zero) |
| 4 | `IfChangesExist` | Conditional branch |
| 4a | `CopyCDCChanges` | Extract changed rows with `CDC_Operation` column |
| 4b | `ApplyCDCToTarget` | Stored procedure to merge/upsert/delete in target |
| 4c | `UpdateCDCWatermark` | Advance watermark |

**CDC_Operation logic:**
- `IsDeleted = 1` → `'D'` (soft delete)
- `CreatedDate = LastModifiedDate` → `'I'` (new insert)
- Otherwise → `'U'` (update)

**Legacy SSIS equivalent:** The original used SQL Server Change Tracking (`CHANGETABLE`) with a fallback to `LastModifiedDate > ?` parameter binding.

---

## Watermark Infrastructure

**File:** `watermark-setup.sql`

### Table: `adf.IncrementalWatermark`

| Column | Type | Purpose |
|--------|------|---------|
| `TableName` | VARCHAR(255) PK | Source table identifier |
| `WatermarkColumn` | VARCHAR(128) | Column used for tracking |
| `WatermarkValue` | DATETIME2(7) | Last-processed value |
| `WatermarkType` | VARCHAR(50) | Data type hint |
| `PipelineName` | VARCHAR(255) | Last pipeline that updated this row |
| `LastRunStatus` | VARCHAR(50) | Succeeded / Failed / InProgress |
| `LastRunStart` | DATETIME2(7) | Pipeline start timestamp |
| `LastRunEnd` | DATETIME2(7) | Pipeline end timestamp |
| `RowsProcessed` | BIGINT | Rows copied in last run |

### Stored Procedures

| Procedure | Called By | Purpose |
|-----------|----------|---------|
| `adf.usp_GetWatermark` | Lookup activity | Retrieve current watermark for a table |
| `adf.usp_UpdateWatermark` | StoredProcedure activity | Advance watermark after successful copy |
| `adf.usp_RecordRunStart` | StoredProcedure activity | Log pipeline start time |

### Monitoring View

`adf.vw_WatermarkStatus` — Query to see the current state of all incremental loads including run duration and row counts.

---

## Migration Notes

### Changes from Legacy SSIS

| Aspect | Legacy (SSIS) | ADF Migration |
|--------|--------------|---------------|
| Watermark storage | `audit.WatermarkTable` with `SQL_VARIANT` | `adf.IncrementalWatermark` with `DATETIME2(7)` |
| Watermark retrieval | SSIS variable + Execute SQL Task | ADF Lookup activity + stored procedure |
| Watermark update | Execute SQL Task | ADF StoredProcedure activity |
| Error handling | SSIS event handlers | ADF failure dependency + WebActivity alert |
| CDC approach | SQL Server Change Tracking | Timestamp-based with CDC_Operation classification |
| Idempotency | Not guaranteed | Built-in for snapshot; upsert for others |
| Observability | SSIS logging | Watermark table tracks status, duration, row counts |

### Required Linked Services

- `LS_AzureSQL_BrightfieldDW` — Destination Azure SQL Database
- Source linked services for each system (POS, Inventory, CRM)

### Required Datasets

- `DS_AzureSQL_Watermark` — Points to `adf.IncrementalWatermark`
- `DS_SourceSQL_POSSales` / `DS_SinkSQL_StagingPOSSales`
- `DS_SourceSQL_Inventory` / `DS_SinkSQL_StagingInventory`
- `DS_SourceSQL_CRMCustomers` / `DS_SinkSQL_StagingCRMCustomers`

### Setup Steps

1. Run `watermark-setup.sql` against the destination database to create the `adf` schema, watermark table, and stored procedures.
2. Import the three pipeline JSON files into your ADF instance.
3. Create the required linked services and datasets referenced by the pipelines.
4. Configure triggers (e.g., daily schedule for inventory, frequent schedule for POS/CRM).
