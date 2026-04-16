# Log Analytics KQL Queries for ADF Monitoring

> These queries target the **ADFPipelineRun** and **ADFActivityRun** tables populated by
> ADF diagnostic settings. Run them in the **Log Analytics workspace** linked to your
> Data Factory.

---

## 1 — Failed Pipelines

### 1.1 All Failed Pipeline Runs (Last 24 h)

```kql
ADFPipelineRun
| where Status == "Failed"
| where TimeGenerated > ago(24h)
| project TimeGenerated, PipelineName, RunId, FailureType, ErrorMessage = tostring(Error.message)
| order by TimeGenerated desc
```

### 1.2 Failure Count by Pipeline (Last 7 Days)

```kql
ADFPipelineRun
| where Status == "Failed"
| where TimeGenerated > ago(7d)
| summarize FailureCount = count() by PipelineName
| order by FailureCount desc
```

### 1.3 Pipelines With Consecutive Failures

```kql
ADFPipelineRun
| where TimeGenerated > ago(24h)
| order by PipelineName asc, TimeGenerated desc
| serialize
| extend PrevStatus = prev(Status), PrevPipeline = prev(PipelineName)
| where PipelineName == PrevPipeline and Status == "Failed" and PrevStatus == "Failed"
| distinct PipelineName
```

### 1.4 Failure Rate by Pipeline (Last 7 Days)

```kql
ADFPipelineRun
| where TimeGenerated > ago(7d)
| summarize
    Total = count(),
    Failed = countif(Status == "Failed")
    by PipelineName
| extend FailureRate = round(todouble(Failed) / todouble(Total) * 100, 2)
| order by FailureRate desc
```

---

## 2 — Slow Activities

### 2.1 Top 20 Slowest Activities (Last 24 h)

```kql
ADFActivityRun
| where Status == "Succeeded"
| where TimeGenerated > ago(24h)
| extend DurationSeconds = datetime_diff("second", End, Start)
| top 20 by DurationSeconds desc
| project PipelineName, ActivityName, ActivityType, DurationSeconds, Start, End
```

### 2.2 Average & P95 Duration by Activity (Last 7 Days)

```kql
ADFActivityRun
| where Status == "Succeeded"
| where TimeGenerated > ago(7d)
| extend DurationSeconds = datetime_diff("second", End, Start)
| summarize
    AvgDuration = avg(DurationSeconds),
    P50Duration = percentile(DurationSeconds, 50),
    P95Duration = percentile(DurationSeconds, 95),
    MaxDuration = max(DurationSeconds),
    RunCount    = count()
    by ActivityName, ActivityType
| order by P95Duration desc
```

### 2.3 Duration Trend for a Specific Activity

```kql
let targetActivity = "CopyCustomersToStaging";
ADFActivityRun
| where ActivityName == targetActivity and Status == "Succeeded"
| where TimeGenerated > ago(30d)
| extend DurationSeconds = datetime_diff("second", End, Start)
| summarize AvgDuration = avg(DurationSeconds) by bin(TimeGenerated, 1d)
| order by TimeGenerated asc
| render timechart
```

### 2.4 Activities Exceeding SLA Threshold

```kql
let slaThresholdMinutes = 60;
ADFActivityRun
| where Status == "Succeeded"
| where TimeGenerated > ago(7d)
| extend DurationMinutes = datetime_diff("minute", End, Start)
| where DurationMinutes > slaThresholdMinutes
| project PipelineName, ActivityName, ActivityType, DurationMinutes, Start, End
| order by DurationMinutes desc
```

---

## 3 — Data Volume Trends

### 3.1 Daily Row Counts by Pipeline

```kql
ADFActivityRun
| where ActivityType == "Copy" and Status == "Succeeded"
| where TimeGenerated > ago(30d)
| extend RowsRead    = toint(parse_json(Output).rowsRead)
| extend RowsCopied  = toint(parse_json(Output).rowsCopied)
| summarize
    TotalRowsRead    = sum(RowsRead),
    TotalRowsCopied  = sum(RowsCopied)
    by PipelineName, bin(TimeGenerated, 1d)
| order by TimeGenerated desc
```

### 3.2 Data Volume (MB) Over Time

```kql
ADFActivityRun
| where ActivityType == "Copy" and Status == "Succeeded"
| where TimeGenerated > ago(30d)
| extend DataReadMB    = round(tolong(parse_json(Output).dataRead)    / 1048576.0, 2)
| extend DataWrittenMB = round(tolong(parse_json(Output).dataWritten) / 1048576.0, 2)
| summarize
    TotalReadMB    = sum(DataReadMB),
    TotalWrittenMB = sum(DataWrittenMB)
    by bin(TimeGenerated, 1d)
| order by TimeGenerated asc
| render timechart
```

### 3.3 Row Count Anomaly Detection (Significant Drop)

```kql
let lookback = 14d;
let threshold = 0.5; // flag if today < 50% of 14-day average
ADFActivityRun
| where ActivityType == "Copy" and Status == "Succeeded"
| where TimeGenerated > ago(lookback)
| extend RowsCopied = toint(parse_json(Output).rowsCopied)
| summarize DailyRows = sum(RowsCopied) by PipelineName, bin(TimeGenerated, 1d)
| partition by PipelineName (
    order by TimeGenerated asc
    | extend AvgRows = avg_if(DailyRows, TimeGenerated < ago(1d))
    | where TimeGenerated >= ago(1d)
    | where DailyRows < AvgRows * threshold
)
| project PipelineName, TimeGenerated, DailyRows, AvgRows
```

---

## 4 — Error Patterns

### 4.1 Top Error Messages (Last 7 Days)

```kql
ADFActivityRun
| where Status == "Failed"
| where TimeGenerated > ago(7d)
| extend ErrorMessage = tostring(Error.message)
| summarize Count = count() by ErrorMessage
| order by Count desc
| take 20
```

### 4.2 Errors Grouped by Failure Type

```kql
ADFActivityRun
| where Status == "Failed"
| where TimeGenerated > ago(7d)
| extend FailureType = tostring(Error.failureType)
| summarize Count = count() by FailureType, ActivityType
| order by Count desc
```

### 4.3 Error Timeline (Hourly Heatmap)

```kql
ADFActivityRun
| where Status == "Failed"
| where TimeGenerated > ago(7d)
| summarize ErrorCount = count() by bin(TimeGenerated, 1h)
| order by TimeGenerated asc
| render timechart
```

### 4.4 Recurring Errors by Pipeline and Activity

```kql
ADFActivityRun
| where Status == "Failed"
| where TimeGenerated > ago(7d)
| extend ErrorCode = tostring(Error.errorCode)
| summarize
    Count       = count(),
    FirstSeen   = min(TimeGenerated),
    LastSeen    = max(TimeGenerated)
    by PipelineName, ActivityName, ErrorCode
| where Count > 1
| order by Count desc
```

---

## 5 — Operational Overview

### 5.1 Daily Pipeline Success Rate

```kql
ADFPipelineRun
| where TimeGenerated > ago(30d)
| summarize
    Succeeded = countif(Status == "Succeeded"),
    Failed    = countif(Status == "Failed"),
    Cancelled = countif(Status == "Cancelled")
    by bin(TimeGenerated, 1d)
| extend SuccessRate = round(todouble(Succeeded) / todouble(Succeeded + Failed + Cancelled) * 100, 2)
| order by TimeGenerated asc
| render timechart
```

### 5.2 Integration Runtime CPU & Memory

```kql
ADFIntegrationRuntimeMetrics
| where TimeGenerated > ago(24h)
| summarize
    AvgCPU    = avg(CpuPercentage),
    MaxCPU    = max(CpuPercentage),
    AvgMemory = avg(AvailableMemory)
    by IntegrationRuntimeName, bin(TimeGenerated, 15m)
| order by TimeGenerated desc
```

### 5.3 Trigger Execution Summary

```kql
ADFTriggerRun
| where TimeGenerated > ago(7d)
| summarize
    Succeeded = countif(Status == "Succeeded"),
    Failed    = countif(Status == "Failed")
    by TriggerName
| extend SuccessRate = round(todouble(Succeeded) / todouble(Succeeded + Failed) * 100, 2)
| order by Failed desc
```
