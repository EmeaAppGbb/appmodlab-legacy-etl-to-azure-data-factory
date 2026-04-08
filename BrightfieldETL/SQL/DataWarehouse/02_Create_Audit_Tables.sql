-- =============================================
-- Audit Schema and Tables
-- Tracks ETL execution, row counts, errors
-- =============================================

USE BrightfieldDW;
GO

-- Create audit schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'audit')
BEGIN
    EXEC('CREATE SCHEMA audit');
END
GO

-- =============================================
-- Audit Table: Package Execution Log
-- =============================================
IF OBJECT_ID('audit.PackageExecution', 'U') IS NOT NULL
    DROP TABLE audit.PackageExecution;
GO

CREATE TABLE audit.PackageExecution
(
    ExecutionID BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    PackageName VARCHAR(255) NOT NULL,
    StartTime DATETIME NOT NULL,
    EndTime DATETIME NULL,
    Status VARCHAR(50) NOT NULL, -- Running, Success, Failed
    RowsProcessed INT NULL,
    ErrorMessage NVARCHAR(MAX) NULL,
    ServerName VARCHAR(100) NULL,
    UserName VARCHAR(100) NULL
);
GO

CREATE INDEX IX_PackageExecution_PackageName ON audit.PackageExecution(PackageName);
CREATE INDEX IX_PackageExecution_StartTime ON audit.PackageExecution(StartTime);
GO

-- =============================================
-- Audit Table: Extract Log
-- =============================================
IF OBJECT_ID('audit.ExtractLog', 'U') IS NOT NULL
    DROP TABLE audit.ExtractLog;
GO

CREATE TABLE audit.ExtractLog
(
    ExtractID BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    SourceSystem VARCHAR(100) NOT NULL,
    ExtractDate DATETIME NOT NULL,
    RowCount INT NULL,
    ExtractStatus VARCHAR(50) NOT NULL
);
GO

CREATE INDEX IX_ExtractLog_SourceSystem ON audit.ExtractLog(SourceSystem);
GO

-- =============================================
-- Audit Table: Data Quality Issues
-- =============================================
IF OBJECT_ID('audit.DataQualityIssues', 'U') IS NOT NULL
    DROP TABLE audit.DataQualityIssues;
GO

CREATE TABLE audit.DataQualityIssues
(
    IssueID BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    SourceTable VARCHAR(255) NOT NULL,
    IssueType VARCHAR(100) NOT NULL,
    IssueDescription NVARCHAR(MAX) NULL,
    AffectedRows INT NULL,
    IdentifiedDate DATETIME NOT NULL DEFAULT GETDATE()
);
GO
