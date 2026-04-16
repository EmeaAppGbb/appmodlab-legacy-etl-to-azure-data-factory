// ──────────────────────────────────────────────────────────────
// Brightfield Retail – SSIS-to-ADF Migration  |  Main Orchestrator
// Deploys all infrastructure for the migrated ETL platform.
// ──────────────────────────────────────────────────────────────

targetScope = 'resourceGroup'

// ─── Parameters ───────────────────────────────────────────────
@description('Environment name used for naming and tagging (e.g. dev, staging, prod).')
@allowed(['dev', 'staging', 'prod'])
param environment string = 'dev'

@description('Azure region for all resources.')
param location string = resourceGroup().location

@description('Naming prefix applied to every resource (e.g. bfretail).')
@minLength(3)
@maxLength(12)
param prefix string = 'bfretail'

@description('Administrator login for the Synapse SQL pool.')
param sqlAdminLogin string = 'sqladmin'

@secure()
@description('Administrator password for the Synapse SQL pool.')
param sqlAdminPassword string

@description('VM admin username for the self-hosted Integration Runtime host.')
param irVmAdminUsername string = 'iradmin'

@secure()
@description('VM admin password for the self-hosted Integration Runtime host.')
param irVmAdminPassword string

@description('Tags applied to every resource.')
param tags object = {
  project: 'brightfield-etl-migration'
  environment: environment
}

// ─── Variables ────────────────────────────────────────────────
var baseName = '${prefix}-${environment}'

// ─── Modules ──────────────────────────────────────────────────

module keyVault 'modules/key-vault.bicep' = {
  name: 'deploy-key-vault'
  params: {
    baseName: baseName
    location: location
    tags: tags
  }
}

module storageAccount 'modules/storage-account.bicep' = {
  name: 'deploy-storage-account'
  params: {
    baseName: baseName
    location: location
    tags: tags
  }
}

module dataFactory 'modules/data-factory.bicep' = {
  name: 'deploy-data-factory'
  params: {
    baseName: baseName
    location: location
    tags: tags
    keyVaultName: keyVault.outputs.keyVaultName
  }
}

module synapse 'modules/synapse.bicep' = {
  name: 'deploy-synapse'
  params: {
    baseName: baseName
    location: location
    tags: tags
    sqlAdminLogin: sqlAdminLogin
    sqlAdminPassword: sqlAdminPassword
    storageAccountId: storageAccount.outputs.storageAccountId
    storageAccountUrl: storageAccount.outputs.dfsEndpoint
  }
}

module selfHostedIr 'modules/self-hosted-ir.bicep' = {
  name: 'deploy-self-hosted-ir'
  params: {
    baseName: baseName
    location: location
    tags: tags
    vmAdminUsername: irVmAdminUsername
    vmAdminPassword: irVmAdminPassword
    dataFactoryId: dataFactory.outputs.dataFactoryId
  }
}

// ─── Outputs ──────────────────────────────────────────────────
output dataFactoryName string = dataFactory.outputs.dataFactoryName
output storageAccountName string = storageAccount.outputs.storageAccountName
output synapseWorkspaceName string = synapse.outputs.workspaceName
output keyVaultName string = keyVault.outputs.keyVaultName
output irVmName string = selfHostedIr.outputs.vmName
