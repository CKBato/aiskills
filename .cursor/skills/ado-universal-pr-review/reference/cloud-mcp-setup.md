# Cloud Agent MCP setup — ado-universal-pr-review

Configure **user-ado** and **user-atlassian** in the **Cloud Agents** dashboard so scheduled automations can batch-review Traffix Medallion PRs.

Traffix defaults are in [config.json](../config.json). Example JSON to paste is in [mcp.cloud-agents.example.json](../mcp.cloud-agents.example.json).

## Where to configure

Cloud Agents do **not** reliably inherit your desktop `~/.cursor/mcp.json`. Add MCP servers in the dashboard:

| Plan | URL |
|------|-----|
| Individual | [cursor.com/agents](https://cursor.com/agents) → MCP |
| Team | [cursor.com/dashboard/integrations](https://cursor.com/dashboard/integrations) |

After saving, open your ADO batch automation → **Tools / MCP** → enable **user-ado** and **user-atlassian** → **Run now** to verify.

## 1. user-ado (Azure DevOps)

Skills expect tools such as `repo_list_pull_requests_by_repo_or_project`, `repo_get_pull_request_by_id`, `repo_get_pull_request_changes`, and `repo_create_pull_request_thread`. Use Microsoft's **`@azure-devops/mcp`** package (stdio) with **PAT auth** for Cloud Agents — the hosted remote ADO MCP (`https://mcp.dev.azure.com/...`) uses different tool names and Entra OAuth that may not work in Cursor yet.

### Create an ADO PAT

1. Azure DevOps → **User settings → Personal access tokens**.
2. **New token** with scopes at minimum:
   - **Code** — Read & write (list PRs, read diffs, post review threads)
   - **Work Items** — Read (optional; linked work items on PRs)
3. Copy the token immediately.

### Encode the PAT for `@azure-devops/mcp`

The server expects `PERSONAL_ACCESS_TOKEN` as **base64** of `email:pat` (email can be any non-empty string):

**macOS / Linux:**

```bash
echo -n "cursor-bot:YOUR_RAW_PAT" | base64
```

**PowerShell:**

```powershell
[Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("cursor-bot:YOUR_RAW_PAT"))
```

### Paste into Cloud Agents MCP dashboard

Use server name **`user-ado`** (matches skill docs). Example:

```json
{
  "command": "npx",
  "args": [
    "-y",
    "@azure-devops/mcp",
    "Traffix-Data-Infrastructure",
    "--authentication",
    "pat",
    "-d",
    "core",
    "repositories"
  ],
  "env": {
    "ado_mcp_project": "Traffix Medallion",
    "PERSONAL_ACCESS_TOKEN": "PASTE_BASE64_VALUE_HERE"
  }
}
```

**Cloud Agent secret note:** paste the **actual** base64 token in the dashboard. `${env:...}` interpolation is often **not** applied for Cloud Agent MCP configs — use literal values or Cursor **Cloud Agent secrets** if your plan supports them, then paste the resolved value into the MCP env block.

### Verify user-ado

In a Cloud Agent test run, ask:

```text
List Active pull requests in Azure DevOps project "Traffix Medallion" using repo_list_pull_requests_by_repo_or_project (project only, top 5).
```

You should get PR metadata back without auth errors.

## 2. user-atlassian (Jira)

Skills expect `getAccessibleAtlassianResources`, `getJiraIssue`, and `addCommentToJiraIssue` against **traffixsupport.atlassian.net** (`cloudId`: `2d0b86b9-1fbf-4e1e-a322-8645bf37910f`).

### Paste into Cloud Agents MCP dashboard

Server name **`user-atlassian`**:

```json
{
  "url": "https://mcp.atlassian.com/v1/mcp"
}
```

### Authenticate

Complete the **OAuth** flow when the dashboard prompts you (or on first tool use). Sign in with an account that can:

- Read **TIX** issues
- Add comments on issues (for Approved / Approved with follow-ups reviews)

Browser OAuth in the Cloud Agents UI is the supported path — do not rely on API tokens for the official Atlassian MCP.

### Verify user-atlassian

```text
getJiraIssue for TIX-1 (or any known ticket) with responseContentFormat markdown. Report summary only.
```

## 3. Wire MCP into the daily automation

1. [cursor.com/automations](https://cursor.com/automations) → your ADO batch automation (or create one).
2. **Repository:** No repository.
3. **Tools / MCP:** enable **user-ado** and **user-atlassian**.
4. **Prompt:** use [prompts/daily-batch.prompt.txt](../prompts/daily-batch.prompt.txt).
5. **Run now** → confirm ADO threads and Jira reads work → enable schedule (`0 8 * * 1-5` + timezone).

## 4. Local desktop MCP (optional mirror)

For the same tool names in interactive Cursor chats, add equivalent entries to `~/.cursor/mcp.json` or project `.cursor/mcp.json`. Desktop can use **interactive** ADO auth (browser login) instead of PAT:

```json
{
  "mcpServers": {
    "user-ado": {
      "command": "npx",
      "args": [
        "-y",
        "@azure-devops/mcp",
        "Traffix-Data-Infrastructure",
        "-d",
        "core",
        "repositories"
      ],
      "env": {
        "ado_mcp_project": "Traffix Medallion"
      }
    },
    "user-atlassian": {
      "url": "https://mcp.atlassian.com/v1/mcp"
    }
  }
}
```

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| MCP tools missing in automation | Enable both servers under automation **Tools / MCP**; re-save automation. |
| ADO 401 / auth failed | Regenerate PAT; confirm base64 is `email:pat`; scopes include Code Read & write. |
| Jira Needs validation / auth failed | Re-run OAuth for **user-atlassian** in dashboard; confirm access to traffixsupport.atlassian.net. |
| Wrong tool names (`repo_pull_request` vs `repo_get_pull_request_by_id`) | You connected remote ADO MCP or wrong package — use `@azure-devops/mcp` stdio config above. |
| Secrets not interpolated | Paste literal PAT base64 in MCP env; avoid `${env:TOKEN}` in Cloud Agent MCP JSON. |
| Reviews run but no Jira comments | Expected for Not Approved; comments only for Approved / Approved with follow-ups per skill. |

## PAT rotation

1. Create new ADO PAT with same scopes.
2. Re-encode base64 → update **user-ado** env in Cloud Agents MCP dashboard.
3. Revoke old PAT.
