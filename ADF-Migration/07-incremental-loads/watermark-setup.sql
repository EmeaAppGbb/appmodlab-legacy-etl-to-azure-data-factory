-- =============================================
-- Watermark Table Setup for ADF Incremental Loads
-- Migrated from: BrightfieldETL/SQL/IncrementalLoad/01_Incremental_Load_Patterns.sql
-- =============================================
-- This script creates the watermark infrastructure used by ADF pipelines
-- to track incremental load positions for each source table.
-- =============================================

USE BrightfieldDW;
GO

-- =============================================
-- Schema Setup
-- =============================================
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'adf')
    EXEC('CREATE SCHEMA adf');
GO

-- =============================================
-- Watermark Tracking Table
-- =============================================
-- Replaces the legacy audit.WatermarkTable with a version
-- optimized for ADF Lookup activity consumption.

IF OBJECT_ID('adf.IncrementalWatermark', 'U') IS NOT NULL
    DROP TABLE adf.IncrementalWatermark;
GO

CREATE TABLE adf.IncrementalWatermark
(
    TableName           VARCHAR(255)    NOT NULL,
    WatermarkColumn     VARCHAR(128)    NOT NULL,
    WatermarkValue      DATETIME2(7)    NOT NULL,
    WatermarkType       VARCHAR(50)     NOT NULL DEFAULT 'DateTime',
    PipelineName        VARCHAR(255)    NULL,
    LastRunStatus       VARCHAR(50)     NULL,
    LastRunStart        DATETIME2(7)    NULL,
    LastRunEnd          DATETIME2(7)    NULL,
    RowsProcessed       BIGINT          NULL DEFAULT 0,
    CreatedDate         DATETIME2(7)    NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedDate        DATETIME2(7)    NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_IncrementalWatermark PRIMARY KEY (TableName)
);
GO

-- =============================================
-- Initialize Watermarks (matching legacy sources)
-- =============================================
INSERT INTO adf.IncrementalWatermark (TableName, WatermarkColumn, WatermarkValue, WatermarkType, PipelineName)
VALUES
    ('POSSales',        'TransactionDate',  '2024-01-01', 'DateTime', 'PL_Incremental_POS_Sales'),
    ('InventoryLevels', 'SnapshotDate',     '2024-01-01', 'DateTime', 'PL_Incremental_Inventory'),
    ('CRMCustomers',    'LastModifiedDate',  '2024-01-01', 'DateTime', 'PL_Incremental_CRM_CDC');
GO

-- =============================================
-- Stored Procedure: Get Watermark (for ADF Lookup)
-- =============================================
-- Called by ADF Lookup activity at pipeline start to retrieve
-- the last-processed watermark value for the given table.

IF OBJECT_ID('adf.usp_GetWatermark', 'P') IS NOT NULL
    DROP PROCEDURE adf.usp_GetWatermark;
GO

CREATE PROCEDURE adf.usp_GetWatermark
    @TableName VARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        TableName,
        WatermarkColumn,
        WatermarkValue
    FROM adf.IncrementalWatermark
    WHERE TableName = @TableName;
END
GO

-- =============================================
-- Stored Procedure: Update Watermark (for ADF post-copy)
-- =============================================
-- Called by ADF Stored Procedure activity after a successful
-- Copy activity to advance the watermark.

IF OBJECT_ID('adf.usp_UpdateWatermark', 'P') IS NOT NULL
    DROP PROCEDURE adf.usp_UpdateWatermark;
GO

CREATE PROCEDURE adf.usp_UpdateWatermark
    @TableName      VARCHAR(255),
    @NewWatermark   DATETIME2(7),
    @PipelineName   VARCHAR(255) = NULL,
    @RunStatus      VARCHAR(50)  = 'Succeeded',
    @RowsProcessed  BIGINT       = 0
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE adf.IncrementalWatermark
    SET
        WatermarkValue  = @NewWatermark,
        LastRunStatus   = @RunStatus,
        LastRunEnd      = SYSUTCDATETIME(),
        RowsProcessed   = @RowsProcessed,
        PipelineName    = ISNULL(@PipelineName, PipelineName),
        ModifiedDate    = SYSUTCDATETIME()
    WHERE TableName = @TableName;

    IF @@ROWCOUNT = 0
        RAISERROR('Watermark row not found for table: %s', 16, 1, @TableName);
END
GO

-- =============================================
-- Stored Procedure: Record Run Start
-- =============================================
-- Called at pipeline start to record the run begin time.

IF OBJECT_ID('adf.usp_RecordRunStart', 'P') IS NOT NULL
    DROP PROCEDURE adf.usp_RecordRunStart;
GO

CREATE PROCEDURE adf.usp_RecordRunStart
    @TableName      VARCHAR(255),
    @PipelineName   VARCHAR(255) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE adf.IncrementalWatermark
    SET
        LastRunStart    = SYSUTCDATETIME(),
        LastRunStatus   = 'InProgress',
        PipelineName    = ISNULL(@PipelineName, PipelineName),
        ModifiedDate    = SYSUTCDATETIME()
    WHERE TableName = @TableName;
END
GO

-- =============================================
-- View: Current Watermark Status
-- =============================================
IF OBJECT_ID('adf.vw_WatermarkStatus', 'V') IS NOT NULL
    DROP VIEW adf.vw_WatermarkStatus;
GO

CREATE VIEW adf.vw_WatermarkStatus
AS
SELECT
    TableName,
    WatermarkColumn,
    WatermarkValue,
    PipelineName,
    LastRunStatus,
    LastRunStart,
    LastRunEnd,
    DATEDIFF(SECOND, LastRunStart, LastRunEnd) AS RunDurationSeconds,
    RowsProcessed,
    ModifiedDate
FROM adf.IncrementalWatermark;
GO
