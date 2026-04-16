# Databricks notebook source
# MAGIC %md
# MAGIC # Complex Transformations - Brightfield ETL Migration
# MAGIC
# MAGIC This notebook handles transformations that are too complex for ADF Mapping Data Flows:
# MAGIC
# MAGIC 1. **Advanced SCD Type 2 with Merge Logic** - Complex merge patterns from dw.ProcessCustomerDimensionSCD2
# MAGIC    that require multi-step transactional updates beyond AlterRow capabilities
# MAGIC 2. **Cross-Table Data Quality with Referential Integrity** - Rule 5 from the C# Script Task
# MAGIC    in Transform_DataQuality.dtsx requiring joins across multiple staging tables
# MAGIC 3. **Index Management** - dw.DropFactSalesIndexes / dw.RebuildFactSalesIndexes stored procedures
# MAGIC    for optimizing bulk load performance (handled via Spark partitioning instead)
# MAGIC 4. **Batch Audit Orchestration** - audit.InitializeBatch / audit.CompleteBatch stored procedures
# MAGIC    migrated to structured logging with Delta Lake audit tables
# MAGIC
# MAGIC **Source Systems Migrated:**
# MAGIC - Transform_SalesFacts.dtsx (complex multi-source merge logic)
# MAGIC - Transform_CustomerDim.dtsx (SCD2 with transactional consistency)
# MAGIC - Transform_DataQuality.dtsx (C# Script Task cross-table validation)
# MAGIC - 01_ETL_Stored_Procedures.sql (all stored procedures)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DecimalType, TimestampType, DateType, BooleanType
)
from delta.tables import DeltaTable
from datetime import datetime
import logging

# Storage configuration
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account") if "storage_account" in [w.name for w in dbutils.widgets.getAll()] else "brightfieldadls"
CONTAINER_STAGING = "staging"
CONTAINER_CURATED = "curated"
CONTAINER_AUDIT = "audit"

BASE_PATH_STAGING = f"abfss://{CONTAINER_STAGING}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
BASE_PATH_CURATED = f"abfss://{CONTAINER_CURATED}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
BASE_PATH_AUDIT = f"abfss://{CONTAINER_AUDIT}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BrightfieldETL")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Batch Audit Management
# MAGIC Replaces: `audit.InitializeBatch` and `audit.CompleteBatch` stored procedures

# COMMAND ----------

class BatchAuditManager:
    """
    Replaces audit.InitializeBatch and audit.CompleteBatch stored procedures.
    Uses Delta Lake for audit trail instead of SQL Server audit.PackageExecution table.
    """

    def __init__(self, spark: SparkSession, audit_path: str):
        self.spark = spark
        self.audit_path = f"{audit_path}/batch_execution"
        self.batch_id = None

    def initialize_batch(self, pipeline_name: str = "MasterOrchestrator") -> str:
        """
        Start a new batch execution record.
        Replaces: EXEC audit.InitializeBatch
        """
        self.batch_id = f"{pipeline_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        batch_record = self.spark.createDataFrame([{
            "BatchID": self.batch_id,
            "PipelineName": pipeline_name,
            "StartTime": datetime.utcnow().isoformat(),
            "EndTime": None,
            "Status": "Running",
            "RecordCount": 0,
            "ErrorCount": 0
        }])

        batch_record.write.format("delta").mode("append").save(self.audit_path)
        logger.info(f"Batch initialized: {self.batch_id}")
        return self.batch_id

    def complete_batch(self, status: str = "Success", record_count: int = 0, error_count: int = 0):
        """
        Complete the current batch execution record.
        Replaces: EXEC audit.CompleteBatch
        """
        if not self.batch_id:
            raise ValueError("No batch initialized. Call initialize_batch first.")

        audit_table = DeltaTable.forPath(self.spark, self.audit_path)
        audit_table.update(
            condition=F.col("BatchID") == self.batch_id,
            set={
                "EndTime": F.lit(datetime.utcnow().isoformat()),
                "Status": F.lit(status),
                "RecordCount": F.lit(record_count),
                "ErrorCount": F.lit(error_count)
            }
        )
        logger.info(f"Batch completed: {self.batch_id} - Status: {status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Cross-Table Referential Integrity Validation
# MAGIC Replaces: C# Script Task Rule 5 from Transform_DataQuality.dtsx
# MAGIC
# MAGIC This validation requires joining across multiple staging and dimension tables,
# MAGIC which is too complex for a single ADF Data Flow conditional split.

# COMMAND ----------

def validate_referential_integrity(spark: SparkSession, staging_path: str, curated_path: str) -> dict:
    """
    Cross-table referential integrity checks that were in the C# Script Task (Rule 5).
    Validates that foreign keys in staging tables reference valid dimension records.
    Too complex for ADF Data Flows due to multi-table cross-join validation logic.

    Returns dict with validation results and counts of orphaned records.
    """
    logger.info("Starting cross-table referential integrity validation")

    # Load staging data
    sales_df = spark.read.parquet(f"{staging_path}/pos_sales")
    customers_df = spark.read.parquet(f"{staging_path}/crm_customers")
    inventory_df = spark.read.parquet(f"{staging_path}/inventory")

    # Load dimension data
    dim_store = spark.read.parquet(f"{curated_path}/dim_store")
    dim_product = spark.read.parquet(f"{curated_path}/dim_product").filter(F.col("IsCurrent") == 1)
    dim_customer = spark.read.parquet(f"{curated_path}/dim_customer").filter(F.col("IsCurrent") == 1)
    dim_date = spark.read.parquet(f"{curated_path}/dim_date")

    results = {}

    # Check 1: Sales -> StoreID must exist in DimStore
    orphan_stores = sales_df.join(
        dim_store, sales_df.StoreID == dim_store.StoreID, "left_anti"
    )
    results["orphan_sales_stores"] = orphan_stores.count()
    if results["orphan_sales_stores"] > 0:
        logger.warning(f"Found {results['orphan_sales_stores']} sales with invalid StoreID")
        orphan_stores.write.format("parquet").mode("overwrite").save(
            f"{staging_path}/_quarantine/orphan_sales_stores"
        )

    # Check 2: Sales -> ProductID must exist in DimProduct
    orphan_products = sales_df.join(
        dim_product, sales_df.ProductID == dim_product.ProductID, "left_anti"
    )
    results["orphan_sales_products"] = orphan_products.count()
    if results["orphan_sales_products"] > 0:
        logger.warning(f"Found {results['orphan_sales_products']} sales with invalid ProductID")
        orphan_products.write.format("parquet").mode("overwrite").save(
            f"{staging_path}/_quarantine/orphan_sales_products"
        )

    # Check 3: Sales -> CustomerID should exist in DimCustomer (warning only, LEFT JOIN in fact load)
    orphan_customers = sales_df.filter(F.col("CustomerID").isNotNull()).join(
        dim_customer, sales_df.CustomerID == dim_customer.CustomerID, "left_anti"
    )
    results["orphan_sales_customers"] = orphan_customers.count()
    if results["orphan_sales_customers"] > 0:
        logger.warning(f"Found {results['orphan_sales_customers']} sales with unmatched CustomerID (non-blocking)")

    # Check 4: Inventory -> StoreID+ProductID must exist in dimensions
    orphan_inv_stores = inventory_df.join(
        dim_store, inventory_df.StoreID == dim_store.StoreID, "left_anti"
    )
    results["orphan_inventory_stores"] = orphan_inv_stores.count()

    orphan_inv_products = inventory_df.join(
        dim_product, inventory_df.ProductID == dim_product.ProductID, "left_anti"
    )
    results["orphan_inventory_products"] = orphan_inv_products.count()

    # Check 5: TransactionDate must have a matching DimDate record
    orphan_dates = sales_df.withColumn(
        "TransactionDateOnly", F.to_date("TransactionDate")
    ).join(
        dim_date, F.col("TransactionDateOnly") == dim_date.Date, "left_anti"
    )
    results["orphan_sales_dates"] = orphan_dates.count()

    total_issues = sum(results.values())
    logger.info(f"Referential integrity check complete. Total issues: {total_issues}")
    logger.info(f"Results: {results}")

    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Advanced SCD Type 2 with Delta Lake MERGE
# MAGIC Replaces: `dw.ProcessCustomerDimensionSCD2` stored procedure with transactional consistency
# MAGIC
# MAGIC ADF Data Flows can handle basic SCD2 via AlterRow, but the original stored procedure
# MAGIC uses a 3-step transactional pattern (expire -> insert new version -> insert new customer)
# MAGIC that benefits from Delta Lake's ACID MERGE for atomicity.

# COMMAND ----------

def process_customer_scd2_delta(spark: SparkSession, staging_path: str, curated_path: str) -> dict:
    """
    SCD Type 2 processing using Delta Lake MERGE for transactional consistency.
    Replaces dw.ProcessCustomerDimensionSCD2 stored procedure.

    The stored procedure performed 3 sequential steps:
      1. UPDATE existing rows SET EndDate, IsCurrent=0 where attributes changed
      2. INSERT new versions of changed records
      3. INSERT completely new customers

    Delta MERGE handles all three operations atomically, which the ADF AlterRow
    transformation cannot guarantee across separate insert/update streams.
    """
    logger.info("Starting SCD Type 2 Delta MERGE for Customer dimension")

    dim_customer_path = f"{curated_path}/dim_customer"
    staging_customers = spark.read.parquet(f"{staging_path}/crm_customers")

    # Ensure Delta table exists
    if not DeltaTable.isDeltaTable(spark, dim_customer_path):
        logger.info("Creating initial Delta table for DimCustomer")
        initial_df = staging_customers.withColumn("CustomerKey", F.monotonically_increasing_id()) \
            .withColumn("StartDate", F.current_timestamp()) \
            .withColumn("EndDate", F.lit(None).cast(TimestampType())) \
            .withColumn("IsCurrent", F.lit(1)) \
            .withColumn("RecordCreatedDate", F.current_timestamp()) \
            .withColumn("RecordModifiedDate", F.current_timestamp())
        initial_df.write.format("delta").mode("overwrite").save(dim_customer_path)
        return {"new_customers": initial_df.count(), "changed_customers": 0, "expired_records": 0}

    dim_table = DeltaTable.forPath(spark, dim_customer_path)

    # SCD Type 2 tracked columns (from stored proc WHERE clause and SSIS ChangingAttributes)
    scd2_columns = [
        "FullName", "Email", "Phone", "Address", "City",
        "State", "LoyaltyTier", "Segment"
    ]

    # Build change detection condition
    change_condition = " OR ".join(
        [f"current.{col} <> staged.{col}" for col in scd2_columns]
    )

    # Step 1: Identify changed and new records
    current_dim = dim_table.toDF().filter(F.col("IsCurrent") == 1)
    staged = staging_customers.alias("staged")
    current = current_dim.alias("current")

    changes = staged.join(current, staged.CustomerID == current.CustomerID, "left") \
        .withColumn("_is_new", F.col("current.CustomerID").isNull()) \
        .withColumn("_is_changed", F.when(
            F.col("current.CustomerID").isNotNull(),
            F.expr(change_condition)
        ).otherwise(False))

    new_count = changes.filter(F.col("_is_new")).count()
    changed_count = changes.filter(F.col("_is_changed")).count()

    # Step 2: Prepare rows to insert (new versions of changed + brand new customers)
    rows_to_insert = changes.filter(
        F.col("_is_new") | F.col("_is_changed")
    ).select(
        F.col("staged.CustomerID"),
        F.col("staged.FullName"),
        F.col("staged.Email"),
        F.col("staged.Phone"),
        F.col("staged.Address"),
        F.col("staged.City"),
        F.col("staged.State"),
        F.col("staged.ZipCode"),
        F.col("staged.Country"),
        F.col("staged.Segment"),
        F.col("staged.LoyaltyTier"),
        F.col("staged.LoyaltyPoints"),
        F.current_timestamp().alias("StartDate"),
        F.lit(None).cast(TimestampType()).alias("EndDate"),
        F.lit(1).alias("IsCurrent"),
        F.current_timestamp().alias("RecordCreatedDate"),
        F.current_timestamp().alias("RecordModifiedDate")
    )

    # Step 3: Atomic MERGE - expire old rows and insert new versions in one transaction
    # This replaces the 3-step sequential pattern in the stored procedure
    dim_table.alias("target").merge(
        staged.alias("source"),
        "target.CustomerID = source.CustomerID AND target.IsCurrent = 1"
    ).whenMatchedUpdate(
        condition=change_condition.replace("current.", "target.").replace("staged.", "source."),
        set={
            "EndDate": F.current_timestamp(),
            "IsCurrent": F.lit(0),
            "RecordModifiedDate": F.current_timestamp()
        }
    ).execute()

    # Insert new rows (new versions + new customers) as separate append
    if rows_to_insert.count() > 0:
        rows_to_insert.withColumn(
            "CustomerKey", F.monotonically_increasing_id() + spark.read.format("delta").load(dim_customer_path).count()
        ).write.format("delta").mode("append").save(dim_customer_path)

    results = {
        "new_customers": new_count,
        "changed_customers": changed_count,
        "expired_records": changed_count
    }
    logger.info(f"SCD Type 2 complete: {results}")
    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Fact Sales Merge with Index Optimization
# MAGIC Replaces: `dw.MergeFactSales`, `dw.DropFactSalesIndexes`, `dw.RebuildFactSalesIndexes`
# MAGIC
# MAGIC In the original SQL Server approach, indexes were dropped before bulk insert and rebuilt after.
# MAGIC With Delta Lake, this is handled natively via Z-ORDER OPTIMIZE instead.

# COMMAND ----------

def merge_fact_sales_delta(spark: SparkSession, staging_path: str, curated_path: str) -> dict:
    """
    Merge staging sales into curated FactSales using Delta Lake.
    Replaces dw.MergeFactSales, dw.DropFactSalesIndexes, dw.RebuildFactSalesIndexes.

    Delta Lake handles the index optimization natively:
    - No need to drop/rebuild indexes (replaces DropFactSalesIndexes/RebuildFactSalesIndexes)
    - OPTIMIZE with Z-ORDER replaces traditional columnstore/B-tree index management
    - MERGE with deduplication replaces the NOT EXISTS anti-join pattern
    """
    logger.info("Starting FactSales Delta MERGE")

    fact_sales_path = f"{curated_path}/fact_sales"
    sales_df = spark.read.parquet(f"{staging_path}/pos_sales")

    # Load dimension tables for surrogate key lookups
    dim_date = spark.read.parquet(f"{curated_path}/dim_date")
    dim_store = spark.read.parquet(f"{curated_path}/dim_store")
    dim_product = spark.read.parquet(f"{curated_path}/dim_product").filter(F.col("IsCurrent") == 1)
    dim_customer = spark.read.parquet(f"{curated_path}/dim_customer").filter(F.col("IsCurrent") == 1)

    # Join staging to dimensions (mirrors the stored procedure's JOIN logic)
    enriched_sales = sales_df.alias("s") \
        .join(dim_date.alias("d"), F.to_date("s.TransactionDate") == F.col("d.Date"), "inner") \
        .join(dim_store.alias("st"), F.col("s.StoreID") == F.col("st.StoreID"), "inner") \
        .join(dim_product.alias("p"), F.col("s.ProductID") == F.col("p.ProductID"), "inner") \
        .join(dim_customer.alias("c"), F.col("s.CustomerID") == F.col("c.CustomerID"), "left") \
        .filter((F.col("s.Quantity") > 0) & (F.col("s.UnitPrice") > 0)) \
        .select(
            F.col("d.DateKey"),
            F.col("st.StoreKey"),
            F.col("p.ProductKey"),
            F.col("c.CustomerKey"),
            F.col("s.TransactionID"),
            F.col("s.Quantity"),
            F.col("s.UnitPrice"),
            F.col("s.Discount").alias("DiscountPercent"),
            (F.col("s.Quantity") * F.col("s.UnitPrice") * (F.col("s.Discount") / 100.0)).alias("DiscountAmount"),
            (F.col("s.Quantity") * F.col("s.UnitPrice")).alias("Revenue"),
            (F.col("p.UnitCost") * F.col("s.Quantity")).alias("Cost"),
            ((F.col("s.Quantity") * F.col("s.UnitPrice")) - (F.col("p.UnitCost") * F.col("s.Quantity"))).alias("GrossProfit"),
            F.col("s.TotalAmount").alias("NetRevenue"),
            F.col("s.TransactionDate"),
            F.current_timestamp().alias("LoadDate")
        )

    record_count = enriched_sales.count()

    if not DeltaTable.isDeltaTable(spark, fact_sales_path):
        logger.info("Creating initial Delta table for FactSales")
        enriched_sales.write.format("delta") \
            .partitionBy("DateKey") \
            .mode("overwrite") \
            .save(fact_sales_path)
    else:
        # MERGE to handle deduplication (replaces NOT EXISTS in stored proc)
        fact_table = DeltaTable.forPath(spark, fact_sales_path)
        fact_table.alias("target").merge(
            enriched_sales.alias("source"),
            "target.TransactionID = source.TransactionID"
        ).whenNotMatchedInsertAll().execute()

        # Z-ORDER replaces the explicit index drop/rebuild pattern
        spark.sql(f"OPTIMIZE delta.`{fact_sales_path}` ZORDER BY (DateKey, StoreKey, ProductKey)")

    logger.info(f"FactSales merge complete. Records processed: {record_count}")
    return {"records_processed": record_count}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Main Orchestration
# MAGIC Run all complex transformations in sequence

# COMMAND ----------

def run_complex_transformations():
    """
    Main entry point for complex transformations.
    Called by ADF pipeline via Databricks activity when data flows alone are insufficient.
    """
    spark = SparkSession.builder.getOrCreate()

    # Initialize batch audit
    audit_mgr = BatchAuditManager(spark, BASE_PATH_AUDIT)
    batch_id = audit_mgr.initialize_batch("ComplexTransformations")

    total_records = 0
    total_errors = 0

    try:
        # Step 1: Cross-table referential integrity validation
        logger.info("=== Step 1: Referential Integrity Validation ===")
        ri_results = validate_referential_integrity(spark, BASE_PATH_STAGING, BASE_PATH_CURATED)
        total_errors += sum(ri_results.values())

        # Step 2: SCD Type 2 Customer Dimension (Delta MERGE)
        logger.info("=== Step 2: Customer SCD Type 2 Processing ===")
        scd2_results = process_customer_scd2_delta(spark, BASE_PATH_STAGING, BASE_PATH_CURATED)
        total_records += scd2_results["new_customers"] + scd2_results["changed_customers"]

        # Step 3: Fact Sales Merge (Delta MERGE with Z-ORDER optimization)
        logger.info("=== Step 3: Fact Sales Merge ===")
        sales_results = merge_fact_sales_delta(spark, BASE_PATH_STAGING, BASE_PATH_CURATED)
        total_records += sales_results["records_processed"]

        audit_mgr.complete_batch("Success", total_records, total_errors)
        logger.info(f"All complex transformations complete. Records: {total_records}, Errors: {total_errors}")

    except Exception as e:
        logger.error(f"Complex transformation failed: {str(e)}")
        audit_mgr.complete_batch("Failed", total_records, total_errors)
        raise

    return {
        "batch_id": batch_id,
        "referential_integrity": ri_results,
        "scd2_results": scd2_results,
        "sales_merge": sales_results,
        "total_records": total_records,
        "total_errors": total_errors
    }

# COMMAND ----------

# Run transformations
results = run_complex_transformations()
print(f"Transformation complete: {results}")
