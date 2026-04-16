// ──────────────────────────────────────────────────────────────
// Azure Data Factory with system-assigned managed identity
// and diagnostic settings for monitoring.
// ──────────────────────────────────────────────────────────────

@description('Base name used for resource naming.')
param baseName string

@description('Azure region.')
param location string

@description('Resource tags.')
param tags object

@description('Name of the Key Vault for linked-service credentials.')
param keyVaultName string

// ─── Variables ────────────────────────────────────────────────
var dataFactoryName = 'adf-${baseName}'

// ─── Log Analytics (for diagnostics) ─────────────────────────
resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: 'log-${baseName}'
  location: location
  tags: tags
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: 30
  }
}

// ─── Data Factory ─────────────────────────────────────────────
resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: dataFactoryName
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    publicNetworkAccess: 'Enabled'
  }
}

// ─── Diagnostic Settings ──────────────────────────────────────
resource diagnostics 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: '${dataFactoryName}-diag'
  scope: dataFactory
  properties: {
    workspaceId: logAnalytics.id
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

// ─── Key Vault Access Policy for ADF Managed Identity ─────────
resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' existing = {
  name: keyVaultName
}

resource kvAccessPolicy 'Microsoft.KeyVault/vaults/accessPolicies@2023-07-01' = {
  name: 'add'
  parent: keyVault
  properties: {
    accessPolicies: [
      {
        tenantId: subscription().tenantId
        objectId: dataFactory.identity.principalId
        permissions: {
          secrets: ['get', 'list']
        }
      }
    ]
  }
}

// ─── Outputs ──────────────────────────────────────────────────
output dataFactoryId string = dataFactory.id
output dataFactoryName string = dataFactory.name
output dataFactoryPrincipalId string = dataFactory.identity.principalId
