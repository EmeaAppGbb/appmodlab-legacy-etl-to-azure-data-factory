// ──────────────────────────────────────────────────────────────
// Azure Data Lake Storage Gen2 (Storage Account with HNS)
// Creates raw, staging, and curated containers for the
// Brightfield Retail ETL data-lake layers.
// ──────────────────────────────────────────────────────────────

@description('Base name used for resource naming.')
param baseName string

@description('Azure region.')
param location string

@description('Resource tags.')
param tags object

// ─── Variables ────────────────────────────────────────────────
// Storage account names: lowercase alphanumeric, max 24 chars
var storageAccountName = take(replace('st${baseName}', '-', ''), 24)

var containerNames = [
  'raw'
  'staging'
  'curated'
]

// ─── Storage Account (ADLS Gen2) ──────────────────────────────
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: location
  tags: tags
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
    isHnsEnabled: true
    accessTier: 'Hot'
    supportsHttpsTrafficOnly: true
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    networkAcls: {
      defaultAction: 'Allow'
      bypass: 'AzureServices'
    }
  }
}

// ─── Blob Service ─────────────────────────────────────────────
resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  name: 'default'
  parent: storageAccount
}

// ─── Containers (raw / staging / curated) ─────────────────────
resource containers 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = [
  for name in containerNames: {
    name: name
    parent: blobService
    properties: {
      publicAccess: 'None'
    }
  }
]

// ─── Outputs ──────────────────────────────────────────────────
output storageAccountId string = storageAccount.id
output storageAccountName string = storageAccount.name
output dfsEndpoint string = storageAccount.properties.primaryEndpoints.dfs
