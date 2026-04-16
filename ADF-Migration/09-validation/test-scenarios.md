# Test Scenarios — Brightfield Retail SSIS-to-ADF Migration

> Comprehensive test plan covering full load, incremental load, SCD Type 2, error handling, and performance validation for the Brightfield Retail Group ETL migration from SSIS to Azure Data Factory.

---

## 1. Full Load Scenarios

### TC-FL-001: Full Load — Staging Tables (All Sources)

| Field | Detail |
|-------|--------|
| **Objective** | Verify all 5 extract pipelines populate staging tables with complete source data |
| **Precondition** | Staging tables are empty (truncated); source systems contain known test data |
| **Pipelines** | `pipeline_extract_pos_sales`, `pipeline_extract_inventory`, `pipeline_extract_crm_customers`, `pipeline_extract_ecommerce_orders`, `pipeline_extract_supplier_files` |
| **Steps** | 1. Trigger `pipeline_master_orchestrator` with full-load parameters<br>2. Wait for all extract activities to complete<br>3. Compare row counts in staging tables against source systems |
| **Expected Result** | Row counts match source: `staging.POSSales`, `staging.InventoryLevels`, `staging.CRMCustomers`, `staging.EcommerceOrders`, `staging.SupplierPricing` |
| **Validation Query** | Section 1.1 of `validation-queries.sql` |

### TC-FL-002: Full Load — Dimension Tables

| Field | Detail |
|-------|--------|
| **Objective** | Verify dimension tables (`DimDate`, `DimStore`, `DimProduct`, `DimCustomer`) are populated correctly from staging |
| **Precondition** | Staging tables contain validated data; dimension tables are empty |
| **Steps** | 1. Execute transform dataflows (`dataflow_transform_customer_dim`, etc.)<br>2. Verify row counts match expected dimension cardinality<br>3. Verify all business keys from staging exist in dimensions |
| **Expected Result** | Every unique `CustomerID`, `StoreID`, `ProductID` in staging has a corresponding dimension row with `IsCurrent = 1` |
| **Validation Query** | Section 1.2 of `validation-queries.sql` |

### TC-FL-003: Full Load — Fact Tables

| Field | Detail |
|-------|--------|
| **Objective** | Verify `FactSales` and `FactInventory` are fully populated with correct surrogate key lookups |
| **Precondition** | Dimensions loaded; staging data available |
| **Steps** | 1. Execute `dataflow_transform_sales_facts` and `dataflow_transform_inventory_facts`<br>2. Verify fact row counts match staging transaction counts<br>3. Verify all surrogate keys resolve to valid dimension records |
| **Expected Result** | Zero orphan keys (Section 4 of `validation-queries.sql`); row counts match staging |
| **Validation Query** | Sections 1.3, 4.1–4.5 of `validation-queries.sql` |

### TC-FL-004: Full Load — SSIS vs ADF Row Count Parity

| Field | Detail |
|-------|--------|
| **Objective** | Confirm ADF produces identical row counts to the legacy SSIS ETL for the same input data |
| **Precondition** | Both SSIS and ADF have processed the same date range against the same source snapshot |
| **Steps** | 1. Run SSIS packages on a snapshot database<br>2. Run ADF pipelines on the same snapshot<br>3. Compare row counts across all staging, dimension, and fact tables |
| **Expected Result** | Zero difference in row counts for all tables |
| **Validation Query** | Section 6.5 of `validation-queries.sql` |

### TC-FL-005: Full Load — Checksum Parity

| Field | Detail |
|-------|--------|
| **Objective** | Verify data values (not just row counts) match between SSIS and ADF using checksums |
| **Steps** | 1. Run checksum queries (Section 2) against both SSIS-loaded and ADF-loaded databases<br>2. Compare `RowChecksum` values per date |
| **Expected Result** | Checksums match for every date in `FactSales` and `FactInventory` |
| **Validation Query** | Sections 2.1–2.4 of `validation-queries.sql` |

---

## 2. Incremental Load Scenarios

### TC-IL-001: Watermark-Based Incremental — POS Sales

| Field | Detail |
|-------|--------|
| **Objective** | Verify only new transactions (after last watermark) are extracted |
| **Precondition** | Initial full load completed; `adf.IncrementalWatermark` contains valid watermark for `POSSales` |
| **Pipeline** | `pipeline_incremental_pos_sales` |
| **Steps** | 1. Insert 100 new rows in source `dbo.SalesTransactions` with `TransactionDate > watermark`<br>2. Trigger incremental pipeline<br>3. Verify exactly 100 new rows appear in `staging.POSSales`<br>4. Verify watermark value advanced to max `TransactionDate` of new rows |
| **Expected Result** | Delta = 100 rows; watermark updated; no duplicates |
| **Validation Query** | Sections 5.1–5.2 of `validation-queries.sql` |

### TC-IL-002: Daily Snapshot Incremental — Inventory

| Field | Detail |
|-------|--------|
| **Objective** | Verify daily snapshot replaces existing data for the same date (idempotent) |
| **Pipeline** | `pipeline_incremental_inventory` |
| **Steps** | 1. Run pipeline for today — note row count<br>2. Modify source inventory levels for 10 products<br>3. Re-run pipeline for same date<br>4. Verify updated values replaced old values; row count unchanged for those products |
| **Expected Result** | No duplicate snapshot records; values reflect latest source state |
| **Validation Query** | Section 5.3 of `validation-queries.sql` |

### TC-IL-003: CDC-Based Incremental — CRM Customers

| Field | Detail |
|-------|--------|
| **Objective** | Verify CDC pipeline detects inserts, updates, and soft deletes |
| **Pipeline** | `pipeline_incremental_crm_cdc` |
| **Steps** | 1. Insert 5 new customers in CRM source<br>2. Update address for 3 existing customers<br>3. Soft-delete 2 customers (set `IsDeleted = 1`)<br>4. Trigger CDC pipeline<br>5. Verify staging reflects all 10 changes with correct `CDC_Operation` |
| **Expected Result** | 5 inserts (`CDC_Operation = 'I'`), 3 updates (`'U'`), 2 deletes (`'D'`) applied |

### TC-IL-004: Incremental Load — Empty Delta

| Field | Detail |
|-------|--------|
| **Objective** | Verify pipeline handles gracefully when no new data exists since last watermark |
| **Steps** | 1. Run incremental pipeline with no new source data<br>2. Verify pipeline succeeds with 0 rows processed<br>3. Verify watermark value is unchanged |
| **Expected Result** | Pipeline status = `Succeeded`; `RowsProcessed = 0`; watermark unchanged |

### TC-IL-005: Incremental Load — Watermark Recovery After Failure

| Field | Detail |
|-------|--------|
| **Objective** | Verify that a failed incremental run does not advance the watermark, allowing safe re-run |
| **Steps** | 1. Simulate failure mid-copy (e.g., revoke target table permissions temporarily)<br>2. Verify watermark `LastRunStatus = 'Failed'` and `WatermarkValue` unchanged<br>3. Restore permissions and re-run<br>4. Verify all delta rows are processed on retry |
| **Expected Result** | Watermark only advances on success; re-run captures all missed rows |

### TC-IL-006: Incremental Load — Late-Arriving Data

| Field | Detail |
|-------|--------|
| **Objective** | Verify late-arriving transactions (backdated `TransactionDate`) are captured in next incremental run |
| **Steps** | 1. Insert source rows with `TransactionDate` = 2 days ago (but `InsertedAt` = now)<br>2. Run incremental pipeline (watermark based on `TransactionDate`)<br>3. Check whether late rows appear |
| **Expected Result** | Late-arriving rows with `TransactionDate < watermark` are NOT captured (by design). Document as known limitation or adjust watermark column to `InsertedAt` if required. |

---

## 3. SCD Type 2 Scenarios

### TC-SCD2-001: New Customer Insert

| Field | Detail |
|-------|--------|
| **Objective** | Verify new customers create a single row with `IsCurrent = 1`, `EndDate = NULL` |
| **Steps** | 1. Insert a new `CustomerID = 'CUST-NEW-001'` in `staging.CRMCustomers`<br>2. Execute `dataflow_transform_customer_dim`<br>3. Query `dw.DimCustomer` for this customer |
| **Expected Result** | Exactly 1 row: `IsCurrent = 1`, `StartDate = today`, `EndDate = NULL` |

### TC-SCD2-002: Customer Attribute Change (Address)

| Field | Detail |
|-------|--------|
| **Objective** | Verify address change creates a new version and expires the old one |
| **Steps** | 1. Load customer `CUST-100` with Address = '123 Main St'<br>2. Update staging to Address = '456 Oak Ave'<br>3. Execute SCD2 dataflow<br>4. Query all versions for `CUST-100` |
| **Expected Result** | 2 rows: Version 1 (`IsCurrent = 0`, `EndDate = today`), Version 2 (`IsCurrent = 1`, `EndDate = NULL`) |
| **Validation Query** | Section 3.1–3.2 of `validation-queries.sql` |

### TC-SCD2-003: Customer Attribute Change (Loyalty Tier)

| Field | Detail |
|-------|--------|
| **Objective** | Verify LoyaltyTier upgrade triggers SCD2 version change |
| **Steps** | 1. Customer `CUST-200` has `LoyaltyTier = 'Silver'`<br>2. Update to `LoyaltyTier = 'Gold'`<br>3. Execute SCD2 dataflow |
| **Expected Result** | Old version expired; new version with `LoyaltyTier = 'Gold'` and `IsCurrent = 1` |

### TC-SCD2-004: No Change — Idempotent Re-Run

| Field | Detail |
|-------|--------|
| **Objective** | Verify re-running SCD2 with unchanged data does not create spurious versions |
| **Steps** | 1. Load customer data (no changes from previous run)<br>2. Execute SCD2 dataflow twice<br>3. Count versions per customer |
| **Expected Result** | No new versions created; version count unchanged |

### TC-SCD2-005: Multiple Attribute Changes in Single Batch

| Field | Detail |
|-------|--------|
| **Objective** | Verify that changing multiple SCD2 tracked attributes (Address + Segment) in one batch creates exactly one new version |
| **Steps** | 1. Update both `Address` and `Segment` for `CUST-300`<br>2. Execute SCD2 dataflow |
| **Expected Result** | One new current version with both changes; one expired version |

### TC-SCD2-006: SCD2 Temporal Integrity

| Field | Detail |
|-------|--------|
| **Objective** | Validate no date range overlaps or gaps across all customers after bulk processing |
| **Steps** | 1. Process 10,000+ customers through SCD2 dataflow<br>2. Run overlap and continuity checks |
| **Expected Result** | Zero overlaps (Section 3.5); zero gaps (Section 3.6) |
| **Validation Query** | Sections 3.5–3.6 of `validation-queries.sql` |

### TC-SCD2-007: SCD2 with Fact Table Lookup Consistency

| Field | Detail |
|-------|--------|
| **Objective** | After SCD2 version change, verify `FactSales` references the correct `CustomerKey` for the time period |
| **Steps** | 1. Customer `CUST-400` changes address on March 15<br>2. Load sales from March 10 (should use old key) and March 20 (should use new key)<br>3. Verify `FactSales.CustomerKey` points to the correct version |
| **Expected Result** | March 10 sale → old `CustomerKey`; March 20 sale → new `CustomerKey` |

---

## 4. Error Handling Scenarios

### TC-ERR-001: Source System Unavailable

| Field | Detail |
|-------|--------|
| **Objective** | Verify pipeline handles source connection failure gracefully |
| **Steps** | 1. Disable network connectivity to POS SQL Server (or use invalid credentials)<br>2. Trigger `pipeline_extract_pos_sales`<br>3. Verify `pipeline_error_handler` is invoked |
| **Expected Result** | Pipeline status = `Failed`; error logged in `audit.PackageExecution`; alert sent; other independent pipelines unaffected |

### TC-ERR-002: Target Database Full / Write Failure

| Field | Detail |
|-------|--------|
| **Objective** | Verify pipeline handles target write failure and does not corrupt existing data |
| **Steps** | 1. Reduce target database size limit to trigger out-of-space error during copy<br>2. Trigger extract pipeline<br>3. Verify staging data is not partially written (atomicity) |
| **Expected Result** | Transaction rolled back; watermark NOT advanced; clean retry possible |

### TC-ERR-003: Data Type Mismatch / Schema Drift

| Field | Detail |
|-------|--------|
| **Objective** | Verify pipeline detects and reports schema changes in source data |
| **Steps** | 1. Add a new column to source `dbo.SalesTransactions`<br>2. Run extract pipeline<br>3. Verify pipeline handles the extra column (ignores or maps) |
| **Expected Result** | Pipeline succeeds (unmapped columns ignored); no data corruption |

### TC-ERR-004: REST API Rate Limiting (E-commerce)

| Field | Detail |
|-------|--------|
| **Objective** | Verify e-commerce extract handles HTTP 429 (rate limit) with retry |
| **Pipeline** | `pipeline_extract_ecommerce_orders` |
| **Steps** | 1. Configure API mock to return 429 on first 3 requests<br>2. Trigger pipeline<br>3. Verify retry policy activates and pipeline eventually succeeds |
| **Expected Result** | Pipeline retries per policy (exponential backoff); succeeds after rate limit clears |

### TC-ERR-005: Supplier File Missing / Corrupt

| Field | Detail |
|-------|--------|
| **Objective** | Verify ForEach loop handles missing or malformed CSV files |
| **Pipeline** | `pipeline_extract_supplier_files` |
| **Steps** | 1. Place one valid CSV and one corrupt CSV (bad encoding, missing headers) in supplier folder<br>2. Trigger pipeline<br>3. Verify valid file is processed; corrupt file is logged and skipped |
| **Expected Result** | Partial success: valid files loaded; corrupt files reported in error handler |

### TC-ERR-006: Master Orchestrator — Partial Extract Failure

| Field | Detail |
|-------|--------|
| **Objective** | Verify that failure in one extract pipeline does not block independent extracts |
| **Steps** | 1. Cause POS extract to fail (invalid credentials)<br>2. Trigger `pipeline_master_orchestrator`<br>3. Verify other 4 extract pipelines complete successfully<br>4. Verify transform phase does not proceed (depends on all extracts) |
| **Expected Result** | 4 extracts succeed, 1 fails; orchestrator reports partial failure; transforms skipped |

### TC-ERR-007: Self-Hosted Integration Runtime Offline

| Field | Detail |
|-------|--------|
| **Objective** | Verify pipelines using Self-hosted IR fail fast with clear error when IR is unavailable |
| **Steps** | 1. Stop the Self-hosted IR service on the VM<br>2. Trigger on-premises extract pipeline<br>3. Verify timeout and error message |
| **Expected Result** | Pipeline fails with IR connectivity error within configured timeout; alert raised |

### TC-ERR-008: Duplicate Key Handling in Fact Load

| Field | Detail |
|-------|--------|
| **Objective** | Verify fact table load handles duplicate staging records without failing |
| **Steps** | 1. Insert duplicate `TransactionID` rows in `staging.POSSales`<br>2. Execute `dataflow_transform_sales_facts`<br>3. Check for errors or duplicate fact rows |
| **Expected Result** | Duplicates detected by data quality dataflow; redirected to error output; no duplicate fact rows |

---

## 5. Performance Scenarios

### TC-PERF-001: Full Load SLA — Under 2 Hours

| Field | Detail |
|-------|--------|
| **Objective** | Verify complete ETL (extract → transform → load) finishes within 2-hour SLA |
| **Data Volume** | ~5M POS transactions, ~500K inventory snapshots, ~200K customers |
| **Steps** | 1. Trigger `pipeline_master_orchestrator` with production-scale data<br>2. Record start and end times for each phase |
| **Expected Result** | Total duration < 120 minutes; extract < 30 min; transform < 60 min; load < 30 min |
| **Monitoring** | Log Analytics: activity duration KQL queries from `08-monitoring/log-analytics-queries.md` |

### TC-PERF-002: Incremental Load SLA — Under 15 Minutes

| Field | Detail |
|-------|--------|
| **Objective** | Verify incremental load for typical daily delta completes within 15 minutes |
| **Data Volume** | ~50K new POS transactions, ~10K inventory updates, ~1K customer changes |
| **Steps** | 1. Trigger each incremental pipeline<br>2. Record duration |
| **Expected Result** | Each incremental pipeline completes in < 15 minutes |

### TC-PERF-003: SCD2 Processing at Scale

| Field | Detail |
|-------|--------|
| **Objective** | Verify SCD2 dataflow processes 200K customer records with 10% change rate within SLA |
| **Data Volume** | 200K total customers; 20K with attribute changes |
| **Steps** | 1. Prepare staging with 20K changed customers<br>2. Execute SCD2 dataflow<br>3. Verify 20K new versions created, 20K old versions expired |
| **Expected Result** | Processing completes within 30 minutes; all SCD2 integrity checks pass |

### TC-PERF-004: Parallel Extract Throughput

| Field | Detail |
|-------|--------|
| **Objective** | Verify 5 extract pipelines running in parallel do not cause resource contention |
| **Steps** | 1. Trigger all extract pipelines concurrently via master orchestrator<br>2. Monitor Integration Runtime CPU and memory<br>3. Monitor source system query performance |
| **Expected Result** | No extract pipeline timeout; IR resource utilization < 80%; source systems responsive |
| **Monitoring** | Dashboard metrics from `08-monitoring/dashboard.json` |

### TC-PERF-005: Data Flow Cluster Warm-Up

| Field | Detail |
|-------|--------|
| **Objective** | Measure and baseline Mapping Data Flow cluster spin-up time |
| **Steps** | 1. Trigger transform dataflow after cluster idle period<br>2. Record time-to-first-row from activity start<br>3. Repeat with TTL-enabled cluster |
| **Expected Result** | Cold start: < 5 minutes; warm start (TTL cluster): < 30 seconds |

### TC-PERF-006: Large File Extract — Supplier CSV Batch

| Field | Detail |
|-------|--------|
| **Objective** | Verify ForEach loop handles 50+ supplier CSV files totaling 2GB |
| **Pipeline** | `pipeline_extract_supplier_files` |
| **Steps** | 1. Place 50 CSV files (~40MB each) in supplier folder<br>2. Trigger pipeline with batch count = 10 (parallel)<br>3. Verify all files processed |
| **Expected Result** | All 50 files loaded within 20 minutes; no memory pressure on IR |

### TC-PERF-007: Regression — ADF vs SSIS Duration Comparison

| Field | Detail |
|-------|--------|
| **Objective** | Compare ADF pipeline durations against SSIS package durations for same data volume |
| **Steps** | 1. Run SSIS packages on production-scale data; record durations<br>2. Run ADF pipelines on identical data; record durations<br>3. Compare phase-by-phase |
| **Expected Result** | ADF total duration within 120% of SSIS duration (acceptable overhead for cloud benefits). Document any significant differences. |

---

## 6. Test Execution Matrix

| Scenario ID | Category | Priority | Automated | Pipeline Used |
|------------|----------|----------|-----------|---------------|
| TC-FL-001 | Full Load | P0 | Yes | `pipeline_validation` |
| TC-FL-002 | Full Load | P0 | Yes | `pipeline_validation` |
| TC-FL-003 | Full Load | P0 | Yes | `pipeline_validation` |
| TC-FL-004 | Full Load | P0 | Partial | Manual + `validation-queries.sql` |
| TC-FL-005 | Full Load | P1 | Partial | Manual + `validation-queries.sql` |
| TC-IL-001 | Incremental | P0 | Yes | `pipeline_validation` |
| TC-IL-002 | Incremental | P0 | Yes | `pipeline_validation` |
| TC-IL-003 | Incremental | P1 | Yes | `pipeline_validation` |
| TC-IL-004 | Incremental | P1 | Yes | `pipeline_validation` |
| TC-IL-005 | Incremental | P0 | Manual | Simulated failure |
| TC-IL-006 | Incremental | P2 | Manual | Design review |
| TC-SCD2-001 | SCD Type 2 | P0 | Yes | `pipeline_validation` |
| TC-SCD2-002 | SCD Type 2 | P0 | Yes | `pipeline_validation` |
| TC-SCD2-003 | SCD Type 2 | P1 | Yes | `pipeline_validation` |
| TC-SCD2-004 | SCD Type 2 | P1 | Yes | `pipeline_validation` |
| TC-SCD2-005 | SCD Type 2 | P1 | Yes | `pipeline_validation` |
| TC-SCD2-006 | SCD Type 2 | P0 | Yes | `pipeline_validation` |
| TC-SCD2-007 | SCD Type 2 | P0 | Manual | Point-in-time verification |
| TC-ERR-001 | Error | P0 | Manual | Simulated failure |
| TC-ERR-002 | Error | P0 | Manual | Simulated failure |
| TC-ERR-003 | Error | P1 | Manual | Schema drift test |
| TC-ERR-004 | Error | P1 | Manual | API mock |
| TC-ERR-005 | Error | P1 | Manual | Corrupt file test |
| TC-ERR-006 | Error | P0 | Manual | Simulated failure |
| TC-ERR-007 | Error | P1 | Manual | IR shutdown |
| TC-ERR-008 | Error | P1 | Yes | `pipeline_validation` |
| TC-PERF-001 | Performance | P0 | Yes | Timed run |
| TC-PERF-002 | Performance | P0 | Yes | Timed run |
| TC-PERF-003 | Performance | P1 | Yes | Timed run |
| TC-PERF-004 | Performance | P1 | Yes | Monitor dashboard |
| TC-PERF-005 | Performance | P2 | Manual | Baseline measurement |
| TC-PERF-006 | Performance | P1 | Yes | Timed run |
| TC-PERF-007 | Performance | P1 | Manual | Side-by-side comparison |

---

## 7. Environment Requirements

| Environment | Purpose | Data Scale |
|-------------|---------|------------|
| **Dev** | Unit testing individual pipelines | 1K rows per table |
| **Staging** | Integration testing, SCD2 validation | 100K rows per table |
| **Pre-prod** | Performance testing, SSIS parity checks | Production-scale (millions) |
| **Prod** | Smoke tests post-deployment | Live data (read-only validation) |

## 8. Exit Criteria

- [ ] All P0 test cases pass in staging environment
- [ ] All P0 and P1 test cases pass in pre-prod environment
- [ ] Row counts match SSIS output within 0% tolerance
- [ ] Checksums match SSIS output for all fact tables
- [ ] All SCD2 integrity checks return zero violations
- [ ] Full load completes within 2-hour SLA
- [ ] Incremental loads complete within 15-minute SLA
- [ ] Error handling scenarios confirmed with documented behavior
- [ ] `pipeline_validation` runs clean after full ETL cycle
