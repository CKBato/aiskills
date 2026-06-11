---
name: traffix-prod-dl-semantic-refresh
description: >-
  Force refresh all dl_* semantic models in Traffix-Medallion-Production
  (Traffix production Fabric workspace). Uses Azure CLI + Power BI Enhanced
  Refresh API. Use when the user says refresh traffix prod semantic models,
  refresh dl_* models, force refresh production semantic models, or invokes
  the Traffix Prod dl_* Semantic Refresh automation.
disable-model-invocation: false
---

# Traffix prod dl_* semantic model refresh

## Target

| Field | Value |
|---|---|
| Workspace | `Traffix-Medallion-Production` |
| Workspace ID | `2c3ac24f-d00f-474f-971a-63714de64268` |
| Model filter | `dl_*` only |
| Skip | `LH_*` lakehouse datasets |

## Role

Sub-agent for **force refreshing all production medallion semantic models** (`dl_*`) after deploy, sync, or on demand.

## Preconditions

- User is logged in via **`az login`** as a user with refresh rights on the workspace.
- Optional: **user-datafactory-mcp** authenticated (not required if shell + Azure CLI works).

## Preferred execution

Run the bundled script:

```powershell
& "$env:USERPROFILE\.cursor\skills\traffix-prod-dl-semantic-refresh\scripts\Refresh-TraffixProdDlSemanticModels.ps1"
```

Wait for completion:

```powershell
& "$env:USERPROFILE\.cursor\skills\traffix-prod-dl-semantic-refresh\scripts\Refresh-TraffixProdDlSemanticModels.ps1" -WaitForCompletion
```

## Manual workflow (if script unavailable)

1. Confirm `az account show` succeeds.
2. `GET https://api.powerbi.com/v1.0/myorg/groups/2c3ac24f-d00f-474f-971a-63714de64268/datasets`
3. Filter `isRefreshable = true` and `name` starts with `dl_`
4. For each dataset, `POST .../datasets/{id}/refreshes` with body:

```json
{
  "type": "full",
  "applyRefreshPolicy": false,
  "notifyOption": "NoNotification"
}
```

5. Report started count, failures, and model names.
6. If user asks to wait, poll `GET .../refreshes?$top=1` until status is not `Unknown`.

## Do not

- Refresh `LH_*` unless the user explicitly asks for lakehouse datasets too.
- Sync from Git in this skill unless the user also asks for sync.
- Use service principal unless the user explicitly configured SPN auth.

## Chat summary format

After running, reply with:

- Workspace name
- Number of models refreshed
- List of model names started
- Any failures with error detail
- Note that refreshes are async unless `-WaitForCompletion` was used

## Chat triggers (copy any of these)

```text
refresh traffix prod semantic models
```

```text
Follow skill traffix-prod-dl-semantic-refresh and force refresh all dl_* semantic models in Traffix-Medallion-Production.
```

```text
Force refresh all dl_* models in Traffix production workspace and report failures.
```

## User Rule snippet (optional)

Add under **Cursor → Settings → Rules → User Rules**:

```text
When I ask to refresh Traffix production semantic models, refresh dl_* models, or use the phrase "refresh traffix prod semantic models", follow skill traffix-prod-dl-semantic-refresh for workspace Traffix-Medallion-Production.
```
