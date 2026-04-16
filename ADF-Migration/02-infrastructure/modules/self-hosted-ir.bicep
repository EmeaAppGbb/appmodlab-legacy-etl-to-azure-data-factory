// ──────────────────────────────────────────────────────────────
// Self-Hosted Integration Runtime VM
// Windows Server VM that will host the ADF self-hosted IR
// to connect to on-premises SQL Server sources.
// ──────────────────────────────────────────────────────────────

@description('Base name used for resource naming.')
param baseName string

@description('Azure region.')
param location string

@description('Resource tags.')
param tags object

@description('VM administrator username.')
param vmAdminUsername string

@secure()
@description('VM administrator password.')
param vmAdminPassword string

@description('Resource ID of the parent Data Factory.')
param dataFactoryId string

@description('VM size for the IR host.')
param vmSize string = 'Standard_D2s_v3'

// ─── Variables ────────────────────────────────────────────────
var vmName = 'vm-ir-${baseName}'
var nicName = 'nic-ir-${baseName}'
var vnetName = 'vnet-${baseName}'
var subnetName = 'snet-ir'
var nsgName = 'nsg-ir-${baseName}'
var pipName = 'pip-ir-${baseName}'
var dataFactoryName = last(split(dataFactoryId, '/'))
var irName = 'ir-self-hosted'

// ─── Integration Runtime definition in ADF ────────────────────
resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' existing = {
  name: dataFactoryName
}

resource integrationRuntime 'Microsoft.DataFactory/factories/integrationRuntimes@2018-06-01' = {
  name: irName
  parent: dataFactory
  properties: {
    type: 'SelfHosted'
    description: 'Self-hosted IR for on-premises SQL Server connectivity'
  }
}

// ─── Network Security Group ──────────────────────────────────
resource nsg 'Microsoft.Network/networkSecurityGroups@2023-11-01' = {
  name: nsgName
  location: location
  tags: tags
  properties: {
    securityRules: [
      {
        name: 'AllowRDP'
        properties: {
          priority: 1000
          direction: 'Inbound'
          access: 'Allow'
          protocol: 'Tcp'
          sourcePortRange: '*'
          destinationPortRange: '3389'
          sourceAddressPrefix: '*'
          destinationAddressPrefix: '*'
        }
      }
    ]
  }
}

// ─── Virtual Network ──────────────────────────────────────────
resource vnet 'Microsoft.Network/virtualNetworks@2023-11-01' = {
  name: vnetName
  location: location
  tags: tags
  properties: {
    addressSpace: {
      addressPrefixes: ['10.0.0.0/16']
    }
    subnets: [
      {
        name: subnetName
        properties: {
          addressPrefix: '10.0.1.0/24'
          networkSecurityGroup: {
            id: nsg.id
          }
        }
      }
    ]
  }
}

// ─── Public IP ────────────────────────────────────────────────
resource publicIp 'Microsoft.Network/publicIPAddresses@2023-11-01' = {
  name: pipName
  location: location
  tags: tags
  sku: {
    name: 'Standard'
  }
  properties: {
    publicIPAllocationMethod: 'Static'
  }
}

// ─── Network Interface ───────────────────────────────────────
resource nic 'Microsoft.Network/networkInterfaces@2023-11-01' = {
  name: nicName
  location: location
  tags: tags
  properties: {
    ipConfigurations: [
      {
        name: 'ipconfig1'
        properties: {
          subnet: {
            id: vnet.properties.subnets[0].id
          }
          publicIPAddress: {
            id: publicIp.id
          }
          privateIPAllocationMethod: 'Dynamic'
        }
      }
    ]
  }
}

// ─── Virtual Machine (Windows Server 2022) ────────────────────
resource vm 'Microsoft.Compute/virtualMachines@2024-03-01' = {
  name: vmName
  location: location
  tags: tags
  properties: {
    hardwareProfile: {
      vmSize: vmSize
    }
    osProfile: {
      computerName: take(replace(vmName, '-', ''), 15)
      adminUsername: vmAdminUsername
      adminPassword: vmAdminPassword
    }
    storageProfile: {
      imageReference: {
        publisher: 'MicrosoftWindowsServer'
        offer: 'WindowsServer'
        sku: '2022-datacenter-azure-edition'
        version: 'latest'
      }
      osDisk: {
        createOption: 'FromImage'
        managedDisk: {
          storageAccountType: 'Premium_LRS'
        }
      }
    }
    networkProfile: {
      networkInterfaces: [
        {
          id: nic.id
        }
      ]
    }
  }
}

// ─── Outputs ──────────────────────────────────────────────────
output vmName string = vm.name
output vmId string = vm.id
output integrationRuntimeName string = integrationRuntime.name
