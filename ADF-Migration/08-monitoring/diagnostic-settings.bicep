// ---------------------------------------------------------------------------
// ADF Diagnostic Settings – send logs & metrics to Log Analytics workspace
// ---------------------------------------------------------------------------

@description('Name of the existing Azure Data Factory.')
param dataFactoryName string

@description('Resource ID of the Log Analytics workspace.')
param logAnalyticsWorkspaceId string

@description('Optional: Resource ID of a Storage Account for long-term log archival.')
param storageAccountId string = ''

@description('Optional: Resource ID of an Event Hub authorization rule for streaming.')
param eventHubAuthorizationRuleId string = ''

@description('Optional: Event Hub name for streaming diagnostics.')
param eventHubName string = ''

@description('Number of days to retain logs (0 = unlimited when using Log Analytics).')
param retentionDays int = 90

// Reference the existing ADF resource
resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' existing = {
  name: dataFactoryName
}

// ---- Primary Diagnostic Setting – Log Analytics ---------------------------
resource diagnosticSettingLogAnalytics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: 'diag-${dataFactoryName}-loganalytics'
  scope: dataFactory
  properties: {
    workspaceId: logAnalyticsWorkspaceId
    logs: [
      {
        categoryGroup: 'allLogs'
        enabled: true
        retentionPolicy: {
          enabled: retentionDays > 0
          days: retentionDays
        }
      }
    ]
    metrics: [
      {
        category: 'AllMetrics'
        enabled: true
        retentionPolicy: {
          enabled: retentionDays > 0
          days: retentionDays
        }
      }
    ]
  }
}

// ---- Optional: Archive to Storage Account ---------------------------------
resource diagnosticSettingStorage 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = if (!empty(storageAccountId)) {
  name: 'diag-${dataFactoryName}-storage'
  scope: dataFactory
  properties: {
    storageAccountId: storageAccountId
    logs: [
      {
        categoryGroup: 'allLogs'
        enabled: true
        retentionPolicy: {
          enabled: true
          days: 365
        }
      }
    ]
    metrics: [
      {
        category: 'AllMetrics'
        enabled: true
        retentionPolicy: {
          enabled: true
          days: 365
        }
      }
    ]
  }
}

// ---- Optional: Stream to Event Hub ----------------------------------------
resource diagnosticSettingEventHub 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = if (!empty(eventHubAuthorizationRuleId)) {
  name: 'diag-${dataFactoryName}-eventhub'
  scope: dataFactory
  properties: {
    eventHubAuthorizationRuleId: eventHubAuthorizationRuleId
    eventHubName: eventHubName
    logs: [
      {
        categoryGroup: 'allLogs'
        enabled: true
        retentionPolicy: {
          enabled: false
          days: 0
        }
      }
    ]
    metrics: [
      {
        category: 'AllMetrics'
        enabled: true
        retentionPolicy: {
          enabled: false
          days: 0
        }
      }
    ]
  }
}

// ---- Outputs --------------------------------------------------------------
output diagnosticSettingName string = diagnosticSettingLogAnalytics.name
output storageSettingName string = !empty(storageAccountId) ? diagnosticSettingStorage.name : ''
output eventHubSettingName string = !empty(eventHubAuthorizationRuleId) ? diagnosticSettingEventHub.name : ''
