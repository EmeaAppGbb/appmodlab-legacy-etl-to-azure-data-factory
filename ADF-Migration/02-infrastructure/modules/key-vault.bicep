// ──────────────────────────────────────────────────────────────
// Azure Key Vault for connection strings and secrets
// used by ADF linked services and Synapse connections.
// ──────────────────────────────────────────────────────────────

@description('Base name used for resource naming.')
param baseName string

@description('Azure region.')
param location string

@description('Resource tags.')
param tags object

@description('Enable soft-delete protection.')
param enablePurgeProtection bool = true

// ─── Variables ────────────────────────────────────────────────
var keyVaultName = 'kv-${baseName}'

// ─── Key Vault ────────────────────────────────────────────────
resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  tags: tags
  properties: {
    sku: {
      family: 'A'
      name: 'standard'
    }
    tenantId: subscription().tenantId
    enabledForDeployment: false
    enabledForDiskEncryption: false
    enabledForTemplateDeployment: true
    enableSoftDelete: true
    softDeleteRetentionInDays: 90
    enablePurgeProtection: enablePurgeProtection
    enableRbacAuthorization: false
    accessPolicies: []
    networkAcls: {
      defaultAction: 'Allow'
      bypass: 'AzureServices'
    }
  }
}

// ─── Outputs ──────────────────────────────────────────────────
output keyVaultId string = keyVault.id
output keyVaultName string = keyVault.name
output keyVaultUri string = keyVault.properties.vaultUri
