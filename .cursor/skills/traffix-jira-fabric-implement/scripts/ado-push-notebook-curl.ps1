# Push a single notebook-content.py to an ADO feature branch (use curl — avoids PS TLS issues).
# Usage:
#   .\ado-push-notebook-curl.ps1 -LocalFile "path\notebook-content.py" -Branch "feature/TIX-29903-fix-home-currency-mrt-load" -OldObjectId "<parent commit>" -RepoPath "/silver_3gtms_hub_datamart_batch_01.Notebook/notebook-content.py" -Message "TIX-29903: summary"
param(
    [Parameter(Mandatory=$true)][string]$LocalFile,
    [Parameter(Mandatory=$true)][string]$Branch,
    [Parameter(Mandatory=$true)][string]$OldObjectId,
    [Parameter(Mandatory=$true)][string]$RepoPath,
    [Parameter(Mandatory=$true)][string]$Message
)
$ErrorActionPreference = 'Stop'
$org = 'Traffix-Data-Infrastructure'
$project = 'd47186d3-ba21-45f6-8c83-14e351ea90e8'
$repoId = '5c949b12-f510-4039-b9b7-59d6d5326875'
$b64 = [Convert]::ToBase64String([IO.File]::ReadAllBytes($LocalFile))
$token = az account get-access-token --resource https://app.vssps.visualstudio.com --query accessToken -o tsv
$body = @{
    refUpdates = @(@{ name = "refs/heads/$Branch"; oldObjectId = $OldObjectId })
    commits    = @(@{
            comment = $Message
            changes = @(@{
                    changeType = 'edit'
                    item       = @{ path = $RepoPath }
                    newContent = @{ content = $b64; contentType = 'base64encoded' }
                })
        })
} | ConvertTo-Json -Depth 10 -Compress
$tmp = [IO.Path]::GetTempFileName()
$body | Out-File -Encoding utf8 $tmp
curl.exe -sS -X POST "https://dev.azure.com/$org/$project/_apis/git/repositories/$repoId/pushes?api-version=7.1" `
    -H "Authorization: Bearer $token" -H "Content-Type: application/json" --data-binary "@$tmp"
Remove-Item $tmp -Force
