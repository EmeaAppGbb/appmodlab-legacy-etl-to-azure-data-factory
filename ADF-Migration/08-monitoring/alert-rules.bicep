// ---------------------------------------------------------------------------
// Azure Monitor Alert Rules for ADF Pipeline Monitoring
// Covers: pipeline failures, long-running pipelines, data quality breaches
// ---------------------------------------------------------------------------

@description('Name of the Azure Data Factory instance to monitor.')
param dataFactoryName string

@description('Resource ID of the Azure Data Factory.')
param dataFactoryResourceId string

@description('Resource ID of the Action Group for alert notifications.')
param actionGroupResourceId string

@description('Resource ID of the Log Analytics workspace.')
param logAnalyticsWorkspaceId string

@description('Long-running pipeline threshold in minutes.')
param longRunningThresholdMinutes int = 120

@description('Data quality error rate threshold (percentage).')
param dataQualityErrorThreshold int = 5

@description('Alert severity: 0=Critical, 1=Error, 2=Warning, 3=Informational.')
param alertSeverity int = 1

@description('Azure region for the alert rules.')
param location string = resourceGroup().location

@description('Tags to apply to all resources.')
param tags object = {}

// ---- Pipeline Failure Alert (Metric-based) --------------------------------
resource pipelineFailureAlert 'Microsoft.Insights/metricAlerts@2018-03-01' = {
  name: 'alert-${dataFactoryName}-pipeline-failures'
  location: 'global'
  tags: tags
  properties: {
    description: 'Fires when any ADF pipeline run fails.'
    severity: alertSeverity
    enabled: true
    scopes: [
      dataFactoryResourceId
    ]
    evaluationFrequency: 'PT5M'
    windowSize: 'PT15M'
    criteria: {
      'odata.type': 'Microsoft.Azure.Monitor.MultipleResourceMultipleMetricCriteria'
      allOf: [
        {
          name: 'PipelineFailedRuns'
          metricName: 'PipelineFailedRuns'
          metricNamespace: 'Microsoft.DataFactory/factories'
          operator: 'GreaterThan'
          threshold: 0
          timeAggregation: 'Total'
          criterionType: 'StaticThresholdCriterion'
        }
      ]
    }
    actions: [
      {
        actionGroupId: actionGroupResourceId
      }
    ]
  }
}

// ---- Activity Failure Alert (Metric-based) --------------------------------
resource activityFailureAlert 'Microsoft.Insights/metricAlerts@2018-03-01' = {
  name: 'alert-${dataFactoryName}-activity-failures'
  location: 'global'
  tags: tags
  properties: {
    description: 'Fires when any ADF activity run fails.'
    severity: alertSeverity
    enabled: true
    scopes: [
      dataFactoryResourceId
    ]
    evaluationFrequency: 'PT5M'
    windowSize: 'PT15M'
    criteria: {
      'odata.type': 'Microsoft.Azure.Monitor.MultipleResourceMultipleMetricCriteria'
      allOf: [
        {
          name: 'ActivityFailedRuns'
          metricName: 'ActivityFailedRuns'
          metricNamespace: 'Microsoft.DataFactory/factories'
          operator: 'GreaterThan'
          threshold: 0
          timeAggregation: 'Total'
          criterionType: 'StaticThresholdCriterion'
        }
      ]
    }
    actions: [
      {
        actionGroupId: actionGroupResourceId
      }
    ]
  }
}

// ---- Long-Running Pipeline Alert (Log-based / Scheduled Query) ------------
resource longRunningPipelineAlert 'Microsoft.Insights/scheduledQueryRules@2022-06-15' = {
  name: 'alert-${dataFactoryName}-long-running-pipelines'
  location: location
  tags: tags
  properties: {
    description: 'Fires when a pipeline run exceeds ${longRunningThresholdMinutes} minutes.'
    severity: alertSeverity
    enabled: true
    scopes: [
      logAnalyticsWorkspaceId
    ]
    evaluationFrequency: 'PT10M'
    windowSize: 'PT30M'
    criteria: {
      allOf: [
        {
          query: '''
            ADFPipelineRun
            | where Status == "InProgress"
            | extend DurationMinutes = datetime_diff("minute", now(), Start)
            | where DurationMinutes > ${longRunningThresholdMinutes}
            | project PipelineName, RunId, Start, DurationMinutes
          '''
          timeAggregation: 'Count'
          operator: 'GreaterThan'
          threshold: 0
          failingPeriods: {
            numberOfEvaluationPeriods: 1
            minFailingPeriodsToAlert: 1
          }
        }
      ]
    }
    actions: {
      actionGroups: [
        actionGroupResourceId
      ]
    }
  }
}

// ---- Data Quality Threshold Breach Alert (Log-based / Scheduled Query) ----
resource dataQualityAlert 'Microsoft.Insights/scheduledQueryRules@2022-06-15' = {
  name: 'alert-${dataFactoryName}-data-quality-breach'
  location: location
  tags: tags
  properties: {
    description: 'Fires when data quality error rate exceeds ${dataQualityErrorThreshold}%.'
    severity: alertSeverity
    enabled: true
    scopes: [
      logAnalyticsWorkspaceId
    ]
    evaluationFrequency: 'PT15M'
    windowSize: 'PT1H'
    criteria: {
      allOf: [
        {
          query: '''
            ADFActivityRun
            | where ActivityType == "Copy" and Status == "Succeeded"
            | extend RowsRead = toint(parse_json(Output).rowsRead)
            | extend RowsCopied = toint(parse_json(Output).rowsCopied)
            | extend ErrorRate = round(todouble(RowsRead - RowsCopied) / todouble(RowsRead) * 100, 2)
            | where ErrorRate > ${dataQualityErrorThreshold}
            | project PipelineName, ActivityName, RowsRead, RowsCopied, ErrorRate, TimeGenerated
          '''
          timeAggregation: 'Count'
          operator: 'GreaterThan'
          threshold: 0
          failingPeriods: {
            numberOfEvaluationPeriods: 1
            minFailingPeriodsToAlert: 1
          }
        }
      ]
    }
    actions: {
      actionGroups: [
        actionGroupResourceId
      ]
    }
  }
}

// ---- Consecutive Failure Alert (Scheduled Query) --------------------------
resource consecutiveFailureAlert 'Microsoft.Insights/scheduledQueryRules@2022-06-15' = {
  name: 'alert-${dataFactoryName}-consecutive-failures'
  location: location
  tags: tags
  properties: {
    description: 'Fires when the same pipeline fails 3 or more times consecutively.'
    severity: 0 // Critical
    enabled: true
    scopes: [
      logAnalyticsWorkspaceId
    ]
    evaluationFrequency: 'PT15M'
    windowSize: 'PT1H'
    criteria: {
      allOf: [
        {
          query: '''
            ADFPipelineRun
            | where Status == "Failed"
            | summarize FailureCount = count() by PipelineName, bin(TimeGenerated, 1h)
            | where FailureCount >= 3
            | project PipelineName, FailureCount, TimeGenerated
          '''
          timeAggregation: 'Count'
          operator: 'GreaterThan'
          threshold: 0
          failingPeriods: {
            numberOfEvaluationPeriods: 1
            minFailingPeriodsToAlert: 1
          }
        }
      ]
    }
    actions: {
      actionGroups: [
        actionGroupResourceId
      ]
    }
  }
}

// ---- Integration Runtime Node Unavailable (Metric-based) ------------------
resource irNodeAlert 'Microsoft.Insights/metricAlerts@2018-03-01' = {
  name: 'alert-${dataFactoryName}-ir-node-unavailable'
  location: 'global'
  tags: tags
  properties: {
    description: 'Fires when a Self-Hosted Integration Runtime node becomes unavailable.'
    severity: 1
    enabled: true
    scopes: [
      dataFactoryResourceId
    ]
    evaluationFrequency: 'PT5M'
    windowSize: 'PT5M'
    criteria: {
      'odata.type': 'Microsoft.Azure.Monitor.MultipleResourceMultipleMetricCriteria'
      allOf: [
        {
          name: 'IntegrationRuntimeAvailableNodeNumber'
          metricName: 'IntegrationRuntimeAvailableNodeNumber'
          metricNamespace: 'Microsoft.DataFactory/factories'
          operator: 'LessThan'
          threshold: 1
          timeAggregation: 'Minimum'
          criterionType: 'StaticThresholdCriterion'
        }
      ]
    }
    actions: [
      {
        actionGroupId: actionGroupResourceId
      }
    ]
  }
}

// ---- Outputs --------------------------------------------------------------
output pipelineFailureAlertId string = pipelineFailureAlert.id
output activityFailureAlertId string = activityFailureAlert.id
output longRunningAlertId string = longRunningPipelineAlert.id
output dataQualityAlertId string = dataQualityAlert.id
output consecutiveFailureAlertId string = consecutiveFailureAlert.id
output irNodeAlertId string = irNodeAlert.id
