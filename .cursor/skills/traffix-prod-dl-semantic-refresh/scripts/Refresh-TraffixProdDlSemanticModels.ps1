<#
.SYNOPSIS
    Force refresh all dl_* semantic models in Traffix-Medallion-Production.

.DESCRIPTION
    Uses Azure CLI user token and Power BI Enhanced Refresh API.
    Skips lakehouse datasets (LH_*). Optional status polling.
#>
[CmdletBinding()]
param(
    [string]$WorkspaceId = '2c3ac24f-d00f-474f-971a-63714de64268',
    [string]$NamePrefix = 'dl_',
    [switch]$WaitForCompletion,
    [int]$PollSeconds = 30
)

function Get-PbiHeaders {
    $token = az account get-access-token --resource "https://analysis.windows.net/powerbi/api" --query accessToken -o tsv
    if ($LASTEXITCODE -ne 0 -or -not $token) {
        throw 'Azure CLI is not logged in. Run: az login'
    }
    return @{
        Authorization  = "Bearer $token"
        'Content-Type' = 'application/json'
    }
}

function Get-RefreshableSemanticModels {
    param(
        [hashtable]$Headers,
        [string]$GroupId,
        [string]$Prefix
    )

    $uri = "https://api.powerbi.com/v1.0/myorg/groups/$GroupId/datasets"
    $datasets = (Invoke-RestMethod -Headers $Headers -Uri $uri -Method Get).value
    return @($datasets | Where-Object { $_.isRefreshable -and $_.name -like "$Prefix*" })
}

function Start-ForceDatasetRefresh {
    param(
        [hashtable]$Headers,
        [string]$GroupId,
        [string]$DatasetId
    )

    $body = @{
        type               = 'full'
        applyRefreshPolicy = $false
        notifyOption       = 'NoNotification'
    } | ConvertTo-Json

    Invoke-RestMethod -Headers $Headers -Uri "https://api.powerbi.com/v1.0/myorg/groups/$GroupId/datasets/$DatasetId/refreshes" -Method Post -Body $body | Out-Null
}

function Wait-DatasetRefresh {
    param(
        [hashtable]$Headers,
        [string]$GroupId,
        [string]$DatasetId,
        [int]$SleepSeconds
    )

    do {
        Start-Sleep -Seconds $SleepSeconds
        $history = Invoke-RestMethod -Headers $Headers -Uri "https://api.powerbi.com/v1.0/myorg/groups/$GroupId/datasets/$DatasetId/refreshes?`$top=1" -Method Get
        $latest = $history.value | Select-Object -First 1
    } while ($latest.status -eq 'Unknown')

    return $latest
}

$headers = Get-PbiHeaders
$targets = Get-RefreshableSemanticModels -Headers $headers -GroupId $WorkspaceId -Prefix $NamePrefix

if (-not $targets -or $targets.Count -eq 0) {
    Write-Host "No refreshable semantic models found with prefix '$NamePrefix'."
    exit 0
}

Write-Host "Found $($targets.Count) semantic model(s) to refresh."
$results = foreach ($ds in $targets) {
    $row = [ordered]@{
        name   = $ds.name
        id     = $ds.id
        status = 'Unknown'
        detail = ''
    }

    try {
        Start-ForceDatasetRefresh -Headers $headers -GroupId $WorkspaceId -DatasetId $ds.id
        $row.status = 'Started'
        if ($WaitForCompletion) {
            $latest = Wait-DatasetRefresh -Headers $headers -GroupId $WorkspaceId -DatasetId $ds.id -SleepSeconds $PollSeconds
            $row.status = $latest.status
            if ($latest.status -ne 'Completed') {
                $row.detail = $latest.serviceExceptionJson
            }
        }
    }
    catch {
        $row.status = 'Failed'
        $row.detail = $_.ErrorDetails.Message
        if (-not $row.detail) { $row.detail = $_.Exception.Message }
    }

    [pscustomobject]$row
}

$results | Format-Table -AutoSize
$started = @($results | Where-Object { $_.status -in @('Started', 'Completed', 'Unknown') }).Count
$failed = @($results | Where-Object { $_.status -eq 'Failed' }).Count
Write-Host "Started/completed: $started / $($results.Count)"
Write-Host "Failed: $failed"

if ($failed -gt 0) { exit 1 }
exit 0
