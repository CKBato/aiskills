<#
.SYNOPSIS
    Run ado-universal-pr-review batch on all Active Traffix Medallion PRs locally.

.DESCRIPTION
    Invokes the Cursor agent CLI (agent -p) with the skill daily batch prompt.
    Schedule with Windows Task Scheduler for weekday mornings, e.g. 8:00 AM.

.EXAMPLE
    & "$env:USERPROFILE\.cursor\skills\ado-universal-pr-review\scripts\Run-DailyAdoPrReviewBatch.ps1"
#>
[CmdletBinding()]
param()

$SkillRoot = Split-Path -Parent $PSScriptRoot
$PromptFile = Join-Path $SkillRoot 'prompts\daily-batch.prompt.txt'
$LogDir = if ($env:ADO_PR_REVIEW_LOG_DIR) { $env:ADO_PR_REVIEW_LOG_DIR } else { Join-Path $env:USERPROFILE '.cursor\logs\ado-pr-review-batch' }
$Timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$LogFile = Join-Path $LogDir "run-$Timestamp.log"

if (-not (Test-Path $PromptFile)) {
    throw "Missing prompt file: $PromptFile"
}

$AgentBin = $null
if (Get-Command agent -ErrorAction SilentlyContinue) {
    $AgentBin = (Get-Command agent).Source
}
elseif (Test-Path (Join-Path $env:USERPROFILE '.cursor\bin\agent.exe')) {
    $AgentBin = Join-Path $env:USERPROFILE '.cursor\bin\agent.exe'
}
elseif (Test-Path (Join-Path $env:USERPROFILE '.cursor\bin\agent')) {
    $AgentBin = Join-Path $env:USERPROFILE '.cursor\bin\agent'
}
else {
    throw 'Cursor agent CLI not found. Install: curl https://cursor.com/install -fsS | bash'
}

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$Prompt = (Get-Content -Raw -Path $PromptFile) -replace "`r", ''

$Header = @(
    '=== ADO daily PR review batch ==='
    "Started: $(Get-Date -Format o)"
    "Agent: $AgentBin"
    "Prompt: $PromptFile"
    ''
)

$Header | Tee-Object -FilePath $LogFile
& $AgentBin -p $Prompt 2>&1 | Tee-Object -FilePath $LogFile -Append
"Finished: $(Get-Date -Format o)" | Tee-Object -FilePath $LogFile -Append
"Log: $LogFile"
