<#
.SYNOPSIS
    Azure Automation runbook for automated incident response on ADF pipeline failures.

.DESCRIPTION
    This runbook is triggered by Azure Monitor alert webhooks. It performs:
      1. Parses the incoming alert payload to identify the failed pipeline/activity.
      2. Retrieves failure details from the ADF REST API.
      3. Attempts automatic remediation (re-run) for transient failures.
      4. Sends a structured notification to a Teams channel or email.
      5. Creates an incident record for tracking.

.NOTES
    Prerequisites:
      - Azure Automation account with a System-Assigned Managed Identity.
      - Managed Identity granted "Data Factory Contributor" on the ADF resource.
      - Az.DataFactory and Az.Monitor PowerShell modules imported into Automation.
      - Webhook configured in the Automation account and linked to an Action Group.

    Environment variables / Automation variables expected:
      - ADF_SUBSCRIPTION_ID
      - ADF_RESOURCE_GROUP
      - ADF_FACTORY_NAME
      - TEAMS_WEBHOOK_URL   (optional – for Teams notifications)
      - MAX_AUTO_RETRIES     (optional – default 1)
#>

param (
    [Parameter(Mandatory = $false)]
    [object]$WebhookData
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
$ErrorActionPreference = 'Stop'

$SubscriptionId   = Get-AutomationVariable -Name 'ADF_SUBSCRIPTION_ID'
$ResourceGroup    = Get-AutomationVariable -Name 'ADF_RESOURCE_GROUP'
$FactoryName      = Get-AutomationVariable -Name 'ADF_FACTORY_NAME'
$TeamsWebhookUrl  = Get-AutomationVariable -Name 'TEAMS_WEBHOOK_URL' -ErrorAction SilentlyContinue
$MaxAutoRetries   = try { [int](Get-AutomationVariable -Name 'MAX_AUTO_RETRIES') } catch { 1 }

# Transient error codes eligible for automatic retry
$TransientErrorCodes = @(
    'PipelineRunTimeout'
    'IntegrationRuntimeNotAvailable'
    'MappingDataflowTimeout'
    'AzureSqlDatabaseTransientError'
    'CosmosDbThrottling'
    'StorageAccountThrottling'
)

# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------
function Connect-AzureWithIdentity {
    <#
    .SYNOPSIS Authenticates using the Automation Managed Identity.
    #>
    Write-Output '>>> Authenticating with Managed Identity ...'
    Connect-AzAccount -Identity | Out-Null
    Set-AzContext -SubscriptionId $SubscriptionId | Out-Null
    Write-Output ">>> Context set to subscription $SubscriptionId."
}

function Get-AlertContext {
    <#
    .SYNOPSIS Parses the incoming webhook data and returns the alert context.
    #>
    param ([object]$WebhookData)

    if (-not $WebhookData) {
        throw 'No WebhookData received. Ensure this runbook is triggered via an alert webhook.'
    }

    $payload = if ($WebhookData.RequestBody) {
        $WebhookData.RequestBody | ConvertFrom-Json
    } else {
        $WebhookData | ConvertFrom-Json
    }

    $schemaId = $payload.schemaId
    Write-Output ">>> Alert schema: $schemaId"

    if ($schemaId -eq 'azureMonitorCommonAlertSchema') {
        return $payload.data
    }

    # Fallback for legacy schema
    return $payload.data
}

function Get-FailedPipelineRuns {
    <#
    .SYNOPSIS Queries ADF for recent failed pipeline runs.
    #>
    param (
        [string]$PipelineName,
        [int]$LookbackMinutes = 30
    )

    $startTime = (Get-Date).AddMinutes(-$LookbackMinutes).ToUniversalTime()
    $endTime   = (Get-Date).ToUniversalTime()

    $runs = Get-AzDataFactoryV2PipelineRun `
        -ResourceGroupName $ResourceGroup `
        -DataFactoryName   $FactoryName `
        -LastUpdatedAfter  $startTime `
        -LastUpdatedBefore $endTime `
        -PipelineName      $PipelineName `
        -ErrorAction       SilentlyContinue

    return $runs | Where-Object { $_.Status -eq 'Failed' }
}

function Get-FailureDetails {
    <#
    .SYNOPSIS Retrieves activity-level failure details for a pipeline run.
    #>
    param ([string]$RunId)

    $activities = Get-AzDataFactoryV2ActivityRun `
        -ResourceGroupName $ResourceGroup `
        -DataFactoryName   $FactoryName `
        -PipelineRunId     $RunId `
        -RunStartedAfter   (Get-Date).AddDays(-1).ToUniversalTime() `
        -RunStartedBefore  (Get-Date).ToUniversalTime()

    $failed = $activities | Where-Object { $_.Status -eq 'Failed' }

    $details = foreach ($act in $failed) {
        [PSCustomObject]@{
            ActivityName = $act.ActivityName
            ActivityType = $act.ActivityType
            ErrorCode    = $act.Error.errorCode
            ErrorMessage = $act.Error.message
            FailureType  = $act.Error.failureType
            Start        = $act.ActivityRunStart
            End          = $act.ActivityRunEnd
        }
    }
    return $details
}

function Test-IsTransientError {
    <#
    .SYNOPSIS Returns $true if the failure is eligible for automatic retry.
    #>
    param ([object[]]$FailureDetails)

    foreach ($detail in $FailureDetails) {
        if ($detail.ErrorCode -in $TransientErrorCodes) {
            return $true
        }
        # Heuristic: connection/timeout patterns
        if ($detail.ErrorMessage -match '(?i)(timeout|transient|throttl|connection reset|503|429)') {
            return $true
        }
    }
    return $false
}

function Invoke-PipelineRetry {
    <#
    .SYNOPSIS Re-triggers the failed pipeline.
    #>
    param (
        [string]$PipelineName,
        [hashtable]$Parameters = @{}
    )

    Write-Output ">>> Retrying pipeline '$PipelineName' ..."
    $run = Invoke-AzDataFactoryV2Pipeline `
        -ResourceGroupName $ResourceGroup `
        -DataFactoryName   $FactoryName `
        -PipelineName      $PipelineName `
        -Parameter         $Parameters

    Write-Output ">>> Retry initiated. New RunId: $run"
    return $run
}

function Send-TeamsNotification {
    <#
    .SYNOPSIS Posts a notification card to a Microsoft Teams channel.
    #>
    param (
        [string]$PipelineName,
        [string]$RunId,
        [string]$Status,
        [string]$ErrorSummary,
        [bool]$RetryAttempted,
        [string]$RetryRunId
    )

    if (-not $TeamsWebhookUrl) {
        Write-Output '>>> Teams webhook URL not configured – skipping notification.'
        return
    }

    $color = switch ($Status) {
        'Failed'  { 'FF0000' }
        'Retried' { 'FFA500' }
        default   { '0078D4' }
    }

    $adfPortalUrl = "https://adf.azure.com/en/monitoring/pipelineruns/$($RunId)?factory=/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroup/providers/Microsoft.DataFactory/factories/$FactoryName"

    $card = @{
        '@type'      = 'MessageCard'
        '@context'   = 'http://schema.org/extensions'
        themeColor   = $color
        summary      = "ADF Alert: $PipelineName - $Status"
        sections     = @(
            @{
                activityTitle    = "ADF Pipeline Alert: $PipelineName"
                activitySubtitle = "Factory: $FactoryName | Status: $Status"
                facts            = @(
                    @{ name = 'Pipeline';        value = $PipelineName }
                    @{ name = 'Run ID';          value = $RunId }
                    @{ name = 'Status';          value = $Status }
                    @{ name = 'Error';           value = $ErrorSummary }
                    @{ name = 'Auto-Retry';      value = if ($RetryAttempted) { "Yes (RunId: $RetryRunId)" } else { 'No' } }
                    @{ name = 'Timestamp (UTC)'; value = (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') }
                )
                markdown = $true
            }
        )
        potentialAction = @(
            @{
                '@type'  = 'OpenUri'
                name     = 'View in ADF Portal'
                targets  = @( @{ os = 'default'; uri = $adfPortalUrl } )
            }
        )
    }

    $json = $card | ConvertTo-Json -Depth 10
    Invoke-RestMethod -Uri $TeamsWebhookUrl -Method Post -Body $json -ContentType 'application/json'
    Write-Output '>>> Teams notification sent.'
}

function Write-IncidentRecord {
    <#
    .SYNOPSIS Logs a structured incident record to the Automation output stream.
    #>
    param (
        [string]$PipelineName,
        [string]$RunId,
        [object[]]$FailureDetails,
        [bool]$RetryAttempted,
        [string]$RetryRunId
    )

    $record = [PSCustomObject]@{
        Timestamp       = (Get-Date -Format 'o')
        Factory         = $FactoryName
        ResourceGroup   = $ResourceGroup
        SubscriptionId  = $SubscriptionId
        PipelineName    = $PipelineName
        RunId           = $RunId
        FailureCount    = $FailureDetails.Count
        TopError        = ($FailureDetails | Select-Object -First 1).ErrorMessage
        RetryAttempted  = $RetryAttempted
        RetryRunId      = $RetryRunId
    }

    Write-Output '>>> Incident Record:'
    Write-Output ($record | ConvertTo-Json -Depth 5)
}

# ---------------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------------
try {
    Write-Output '============================================================'
    Write-Output "ADF Alert Handler Runbook – $(Get-Date -Format 'o')"
    Write-Output '============================================================'

    # 1. Authenticate
    Connect-AzureWithIdentity

    # 2. Parse alert payload
    $alertContext = Get-AlertContext -WebhookData $WebhookData

    # Extract pipeline name from alert context
    $pipelineName = $null
    if ($alertContext.alertContext.condition.allOf) {
        $dims = $alertContext.alertContext.condition.allOf[0].dimensions
        $pipelineName = ($dims | Where-Object { $_.name -eq 'PipelineName' }).value
    }

    if (-not $pipelineName) {
        # Attempt extraction from custom log-based alert
        $searchResults = $alertContext.alertContext.SearchResults.tables.rows
        if ($searchResults) {
            $pipelineName = $searchResults[0][0]  # First column assumed PipelineName
        }
    }

    if (-not $pipelineName) {
        Write-Output '>>> Could not determine pipeline name from alert. Querying all recent failures ...'
        $pipelineName = '*'
    }

    Write-Output ">>> Affected pipeline: $pipelineName"

    # 3. Get failed runs
    $failedRuns = if ($pipelineName -eq '*') {
        # Query without pipeline filter
        $startTime = (Get-Date).AddMinutes(-30).ToUniversalTime()
        $endTime   = (Get-Date).ToUniversalTime()
        Get-AzDataFactoryV2PipelineRun `
            -ResourceGroupName $ResourceGroup `
            -DataFactoryName   $FactoryName `
            -LastUpdatedAfter  $startTime `
            -LastUpdatedBefore $endTime |
            Where-Object { $_.Status -eq 'Failed' }
    } else {
        Get-FailedPipelineRuns -PipelineName $pipelineName
    }

    if (-not $failedRuns -or $failedRuns.Count -eq 0) {
        Write-Output '>>> No recent failed pipeline runs found. Alert may be stale.'
        return
    }

    Write-Output ">>> Found $($failedRuns.Count) failed run(s)."

    # 4. Process each failed run
    foreach ($run in $failedRuns) {
        $runId        = $run.RunId
        $runPipeline  = $run.PipelineName
        Write-Output "--- Processing RunId: $runId (Pipeline: $runPipeline) ---"

        # 4a. Get failure details
        $failureDetails = Get-FailureDetails -RunId $runId
        $errorSummary   = ($failureDetails | Select-Object -First 1).ErrorMessage
        Write-Output ">>> Error: $errorSummary"

        # 4b. Decide on auto-retry
        $retryAttempted = $false
        $retryRunId     = $null

        if ((Test-IsTransientError -FailureDetails $failureDetails) -and $MaxAutoRetries -gt 0) {
            Write-Output '>>> Transient error detected – attempting automatic retry.'
            try {
                $retryRunId     = Invoke-PipelineRetry -PipelineName $runPipeline
                $retryAttempted = $true
            } catch {
                Write-Output ">>> Auto-retry failed: $_"
            }
        } else {
            Write-Output '>>> Non-transient error or retries disabled – skipping auto-retry.'
        }

        # 4c. Send notification
        $notifyStatus = if ($retryAttempted) { 'Retried' } else { 'Failed' }
        Send-TeamsNotification `
            -PipelineName   $runPipeline `
            -RunId          $runId `
            -Status         $notifyStatus `
            -ErrorSummary   ($errorSummary ?? 'Unknown error') `
            -RetryAttempted $retryAttempted `
            -RetryRunId     ($retryRunId ?? 'N/A')

        # 4d. Log incident record
        Write-IncidentRecord `
            -PipelineName   $runPipeline `
            -RunId          $runId `
            -FailureDetails $failureDetails `
            -RetryAttempted $retryAttempted `
            -RetryRunId     ($retryRunId ?? 'N/A')
    }

    Write-Output '============================================================'
    Write-Output 'Alert handler completed successfully.'
    Write-Output '============================================================'

} catch {
    Write-Error "Runbook failed: $_"
    Write-Error $_.ScriptStackTrace
    throw
}
