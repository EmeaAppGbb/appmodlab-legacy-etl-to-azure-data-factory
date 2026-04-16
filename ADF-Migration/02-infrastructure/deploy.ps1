# ──────────────────────────────────────────────────────────────
# Brightfield Retail – SSIS-to-ADF Migration
# Infrastructure Deployment Script
# ──────────────────────────────────────────────────────────────
# Usage:
#   .\deploy.ps1 -Environment dev -Location eastus -Prefix bfretail
# ──────────────────────────────────────────────────────────────

[CmdletBinding()]
param(
    [Parameter()]
    [ValidateSet('dev', 'staging', 'prod')]
    [string]$Environment = 'dev',

    [Parameter()]
    [string]$Location = 'eastus',

    [Parameter()]
    [string]$Prefix = 'bfretail',

    [Parameter()]
    [string]$ResourceGroupName = "rg-${Prefix}-${Environment}",

    [Parameter()]
    [string]$SqlAdminLogin = 'sqladmin',

    [Parameter()]
    [string]$IrVmAdminUsername = 'iradmin'
)

$ErrorActionPreference = 'Stop'

Write-Host "=============================================" -ForegroundColor Cyan
Write-Host " Brightfield ETL Migration – IaC Deployment" -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Environment     : $Environment"
Write-Host "Location        : $Location"
Write-Host "Prefix          : $Prefix"
Write-Host "Resource Group  : $ResourceGroupName"
Write-Host ""

# ─── Prompt for secrets ──────────────────────────────────────
$sqlAdminPassword = Read-Host -Prompt "Enter SQL Admin password" -AsSecureString
$irVmAdminPassword = Read-Host -Prompt "Enter IR VM Admin password" -AsSecureString

# ─── Ensure resource group exists ─────────────────────────────
Write-Host "`nEnsuring resource group '$ResourceGroupName' exists..." -ForegroundColor Yellow
az group create `
    --name $ResourceGroupName `
    --location $Location `
    --tags "project=brightfield-etl-migration" "environment=$Environment" `
    --output none

if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to create/verify resource group."
    exit 1
}
Write-Host "Resource group ready." -ForegroundColor Green

# ─── Convert SecureString to plain text for az deployment ─────
$sqlPwd = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto(
    [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($sqlAdminPassword)
)
$irPwd = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto(
    [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($irVmAdminPassword)
)

# ─── Deploy Bicep template ────────────────────────────────────
Write-Host "`nStarting Bicep deployment..." -ForegroundColor Yellow
$scriptDir = $PSScriptRoot
$templateFile = Join-Path $scriptDir "main.bicep"

az deployment group create `
    --resource-group $ResourceGroupName `
    --template-file $templateFile `
    --parameters `
        environment=$Environment `
        location=$Location `
        prefix=$Prefix `
        sqlAdminLogin=$SqlAdminLogin `
        sqlAdminPassword=$sqlPwd `
        irVmAdminUsername=$IrVmAdminUsername `
        irVmAdminPassword=$irPwd `
    --name "deploy-brightfield-$Environment-$(Get-Date -Format 'yyyyMMdd-HHmmss')" `
    --verbose

if ($LASTEXITCODE -ne 0) {
    Write-Error "Bicep deployment failed."
    exit 1
}

Write-Host "`n=============================================" -ForegroundColor Green
Write-Host " Deployment completed successfully!" -ForegroundColor Green
Write-Host "=============================================" -ForegroundColor Green

# ─── Show outputs ─────────────────────────────────────────────
Write-Host "`nDeployment outputs:" -ForegroundColor Cyan
az deployment group show `
    --resource-group $ResourceGroupName `
    --name (az deployment group list --resource-group $ResourceGroupName --query "[0].name" -o tsv) `
    --query "properties.outputs" `
    --output table
