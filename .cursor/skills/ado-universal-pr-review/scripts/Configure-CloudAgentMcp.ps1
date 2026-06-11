<#
.SYNOPSIS
    Print Cloud Agents MCP dashboard JSON for user-ado + user-atlassian.

.EXAMPLE
    $env:ADO_RAW_PAT = 'your-pat'; & .\Configure-CloudAgentMcp.ps1
#>
[CmdletBinding()]
param(
    [string]$AdoRawPat = $env:ADO_RAW_PAT
)

$Org = 'Traffix-Data-Infrastructure'
$Project = 'Traffix Medallion'
$AtlassianUrl = 'https://mcp.atlassian.com/v1/mcp'

if (-not $AdoRawPat) {
    $secure = Read-Host 'Azure DevOps PAT (Code Read & write)' -AsSecureString
    $AdoRawPat = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
        [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
    )
}

if (-not $AdoRawPat) {
    throw 'ADO PAT is required.'
}

$PatB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("cursor-bot:$AdoRawPat"))

@"

=== Step 1: Cursor Dashboard → Cloud Agents → Secrets ===
Add secret (exact name):

  PERSONAL_ACCESS_TOKEN=$PatB64

=== Step 2: cursor.com/agents → MCP → user-ado ===
Paste:

{
  "command": "npx",
  "args": [
    "-y",
    "@azure-devops/mcp",
    "$Org",
    "--authentication",
    "pat",
    "-d",
    "core",
    "repositories"
  ],
  "env": {
    "ado_mcp_project": "$Project",
    "PERSONAL_ACCESS_TOKEN": "$PatB64"
  }
}

=== Step 3: cursor.com/agents → MCP → user-atlassian ===
Paste:

{
  "url": "$AtlassianUrl"
}

Then complete OAuth in the dashboard (traffixsupport.atlassian.net account).

=== Step 4: Verify (Cloud Agent test prompt) ===
List Active pull requests in Azure DevOps project "$Project" using repo_list_pull_requests_by_repo_or_project (project only, top 5).

=== Step 5: Enable on automation ===
cursor.com/automations → Tools/MCP → enable user-ado + user-atlassian → Run now

"@
