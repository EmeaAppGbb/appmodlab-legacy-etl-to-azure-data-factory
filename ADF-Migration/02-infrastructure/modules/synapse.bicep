// ──────────────────────────────────────────────────────────────
// Azure Synapse Analytics workspace with a dedicated SQL pool
// for the Brightfield Retail data warehouse.
// ──────────────────────────────────────────────────────────────

@description('Base name used for resource naming.')
param baseName string

@description('Azure region.')
param location string

@description('Resource tags.')
param tags object

@description('SQL administrator login name.')
param sqlAdminLogin string

@secure()
@description('SQL administrator password.')
param sqlAdminPassword string

@description('Resource ID of the ADLS Gen2 storage account.')
param storageAccountId string

@description('DFS endpoint URL of the ADLS Gen2 storage account.')
param storageAccountUrl string

@description('SKU for the dedicated SQL pool (DW performance level).')
@allowed(['DW100c', 'DW200c', 'DW300c', 'DW400c', 'DW500c'])
param sqlPoolSku string = 'DW100c'

// ─── Variables ────────────────────────────────────────────────
var workspaceName = 'syn-${baseName}'
var sqlPoolName = 'dwbrightfield'
var filesystemName = 'synapseroot'

// ─── Managed filesystem for Synapse on the linked ADLS Gen2 ──
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' existing = {
  name: last(split(storageAccountId, '/'))
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' existing = {
  name: 'default'
  parent: storageAccount
}

resource synapseFileSystem 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  name: filesystemName
  parent: blobService
  properties: {
    publicAccess: 'None'
  }
}

// ─── Synapse Workspace ────────────────────────────────────────
resource synapseWorkspace 'Microsoft.Synapse/workspaces@2021-06-01' = {
  name: workspaceName
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    defaultDataLakeStorage: {
      accountUrl: storageAccountUrl
      filesystem: filesystemName
    }
    sqlAdministratorLogin: sqlAdminLogin
    sqlAdministratorLoginPassword: sqlAdminPassword
    publicNetworkAccess: 'Enabled'
  }
  dependsOn: [
    synapseFileSystem
  ]
}

// ─── Firewall: allow Azure services ──────────────────────────
resource firewallAllowAzure 'Microsoft.Synapse/workspaces/firewallRules@2021-06-01' = {
  name: 'AllowAllWindowsAzureIps'
  parent: synapseWorkspace
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

// ─── Dedicated SQL Pool ───────────────────────────────────────
resource sqlPool 'Microsoft.Synapse/workspaces/sqlPools@2021-06-01' = {
  name: sqlPoolName
  parent: synapseWorkspace
  location: location
  tags: tags
  sku: {
    name: sqlPoolSku
  }
  properties: {
    collation: 'SQL_Latin1_General_CP1_CI_AS'
    createMode: 'Default'
  }
}

// ─── Outputs ──────────────────────────────────────────────────
output workspaceName string = synapseWorkspace.name
output workspaceId string = synapseWorkspace.id
output sqlPoolName string = sqlPool.name
output synapsePrincipalId string = synapseWorkspace.identity.principalId
