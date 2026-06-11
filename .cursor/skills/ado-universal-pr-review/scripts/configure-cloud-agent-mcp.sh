#!/usr/bin/env bash
# Print Cloud Agents MCP dashboard JSON for user-ado + user-atlassian.
# Usage:
#   ADO_RAW_PAT='your-pat' bash configure-cloud-agent-mcp.sh
#   bash configure-cloud-agent-mcp.sh   # prompts for PAT

set -euo pipefail

ORG="Traffix-Data-Infrastructure"
PROJECT="Traffix Medallion"
ATLASSIAN_URL="https://mcp.atlassian.com/v1/mcp"

if [[ -z "${ADO_RAW_PAT:-}" ]]; then
  read -rsp "Azure DevOps PAT (Code Read & write): " ADO_RAW_PAT
  echo
fi

if [[ -z "$ADO_RAW_PAT" ]]; then
  echo "Error: ADO PAT is required." >&2
  exit 1
fi

PAT_B64="$(printf '%s' "cursor-bot:${ADO_RAW_PAT}" | base64 | tr -d '\n')"

cat <<EOF
=== Step 1: Cursor Dashboard → Cloud Agents → Secrets ===
Add secret (exact name):

  PERSONAL_ACCESS_TOKEN=${PAT_B64}

=== Step 2: cursor.com/agents → MCP → user-ado ===
Paste (PAT comes from Secrets tab — do NOT commit this file):

{
  "command": "npx",
  "args": [
    "-y",
    "@azure-devops/mcp",
    "${ORG}",
    "--authentication",
    "pat",
    "-d",
    "core",
    "repositories"
  ],
  "env": {
    "ado_mcp_project": "${PROJECT}",
    "PERSONAL_ACCESS_TOKEN": "${PAT_B64}"
  }
}

=== Step 3: cursor.com/agents → MCP → user-atlassian ===
Paste:

{
  "url": "${ATLASSIAN_URL}"
}

Then complete OAuth in the dashboard (traffixsupport.atlassian.net account).

=== Step 4: Verify (Cloud Agent test prompt) ===
List Active pull requests in Azure DevOps project "${PROJECT}" using repo_list_pull_requests_by_repo_or_project (project only, top 5).

=== Step 5: Enable on automation ===
cursor.com/automations → Tools/MCP → enable user-ado + user-atlassian → Run now

EOF
