$ErrorActionPreference = 'Stop'
$org = 'Traffix-Data-Infrastructure'
$project = 'Traffix Medallion'
$repoId = '5c949b12-f510-4039-b9b7-59d6d5326875'
$branchName = 'feature/TIX-30027-tombstone-jdbc-batch'
$filePath = '/bronze_netsuite_incremental_load_batch_02.Notebook/notebook-content.py'
$contentPath = "$env:USERPROFILE\.cursor\NFF\Inputs\TIX-16063\notebooks\bronze_netsuite_incremental_load_batch_02.Notebook\notebook-content.py"

$token = (az account get-access-token -o json | ConvertFrom-Json).accessToken
$headers = @{ Authorization = "Bearer $token"; 'Content-Type' = 'application/json' }
$base = "https://dev.azure.com/$org/$([uri]::EscapeDataString($project))/_apis/git/repositories/$repoId"

$refs = Invoke-RestMethod -Uri "$base/refs?filter=heads/$branchName&api-version=7.1" -Headers $headers
$branchOid = $refs.value[0].objectId
Write-Host "branchOid $branchOid"

$contentBytes = [System.IO.File]::ReadAllBytes($contentPath)
$contentB64 = [Convert]::ToBase64String($contentBytes)
$pushBody = @{
  refUpdates = @(@{ name = "refs/heads/$branchName"; oldObjectId = $branchOid })
  commits = @(@{
    comment = 'TIX-30027: batch tombstone_bronze_line_orphans JDBC queries under NetSuite QueryMaxSize'
    changes = @(@{
      changeType = 'edit'
      item = @{ path = $filePath }
      newContent = @{ content = $contentB64; contentType = 'base64encoded' }
    })
  })
}
$pushJson = $pushBody | ConvertTo-Json -Depth 10 -Compress
$push = Invoke-RestMethod -Method Post -Uri "$base/pushes?api-version=7.1" -Headers $headers -Body $pushJson
Write-Host "PUSH_OK commit $($push.commits[0].commitId)"

$prBody = @{
  sourceRefName = "refs/heads/$branchName"
  targetRefName = 'refs/heads/main'
  title = 'TIX-30027: Batch tombstone JDBC queries for NetSuite QueryMaxSize limit'
  description = @'
## Summary

Fixes prod bronze batch 02 failure when tombstone_bronze_line_orphans builds a single NetSuite JDBC IN clause exceeding SuiteAnalytics QueryMaxSize (32768).

## Problem

Incremental merge for transactionaccountingline succeeded, then tombstone failed when checking ~15K touched transactions (query length ~139K).

## Fix

Batch JDBC reads dynamically (max ~30K chars per query), union NS keys, then run orphan tombstone merge as before.

## Jira

- [TIX-30027](https://traffixsupport.atlassian.net/browse/TIX-30027)

## Test plan

- [ ] Merge PR
- [ ] Git sync / deploy bronze_netsuite_batch_02
- [ ] Re-run bronze batch 02; confirm multiple JDBC batch log lines and notebook completes
'@
} | ConvertTo-Json -Depth 5

$pr = Invoke-RestMethod -Method Post -Uri "$base/pullrequests?api-version=7.1" -Headers $headers -Body $prBody
$url = "https://dev.azure.com/$org/Traffix%20Medallion/_git/Traffix%20Medallion%20Production/pullrequest/$($pr.pullRequestId)"
Write-Host "PR_URL $url"
