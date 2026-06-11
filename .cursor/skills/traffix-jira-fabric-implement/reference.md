# Traffix Jira → Fabric implement — reference

## Backup

Prior skill snapshot: [backup/2026-06-05-pre-playbooks-g2-modes/](backup/2026-06-05-pre-playbooks-g2-modes/BACKUP_README.md)

## Playbooks

See [playbooks.md](playbooks.md) for ticket-type workflows (dimension vs silver datamart).

## Invocation modes

| Mode | Trigger phrases | Behavior |
|------|-----------------|----------|
| Standard | default, "step by step" | Stop at G1–G8 |
| Fast | "GO", "go ahead", "run it all" | G1 report + **G2 must pass** → through G8 |
| Opt-out | "skip PR review" | G6 → G7 only (no G8) |

## G2 sync gate (required before notebook runs)

### Step 1 — Branch name

```bash
node scripts/verify-workspace-git.js "TIX-28531-Dimension-Undefined" "feature/TIX-29903-fix-home-currency-mrt-load"
```

Exit **4** = branch mismatch.

### Step 2 — Head vs ADO

```bash
node scripts/compare-workspace-head-to-ado.js "TIX-28531-Dimension-Undefined" "feature/TIX-29903-fix-home-currency-mrt-load"
```

| Exit | Meaning | Action |
|------|---------|--------|
| 0 | `workspaceHead === adoHead` | Proceed |
| 4 | Wrong branch | Switch branch or accept reuse + document |
| 5 | Head mismatch | Fabric → **Update from Git**; re-run script |

### Fabric API (manual check)

```http
GET https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/git/connection
```

Compare `gitSyncDetails.head` to ADO ref `objectId` for `heads/{branch}`.

### When workspace branch ≠ PR branch

1. Implement in workspace via `updateDefinition` on git-connected notebook
2. Push identical `notebook-content.py` to **`feature/TIX-{key}-...` from `main`** for PR
3. Document both branches in PR + Jira

## Workspace naming

| Input | Pattern | Example |
|-------|---------|---------|
| Jira key + short description | `TIX-{key}-{shortDescription}` | `TIX-29894-dim-user-employee-type` |
| ADO branch | `feature/TIX-{key}-{shortDescription}` | `feature/TIX-29903-fix-home-currency-mrt-load` |

## Notebook operations

| Action | API |
|--------|-----|
| Update notebook | `POST /workspaces/{wsId}/notebooks/{nbId}/updateDefinition` |
| Run notebook | `POST /workspaces/{wsId}/notebooks/{nbId}/jobs/RunNotebook/instances` |

Poll until `status` = `Completed` or `Failed`.

Auth: `az account get-access-token --resource https://api.fabric.microsoft.com`

Validation workspaces read **production** lakehouses via `production_lakehouse_*_abfss` and write to **current** workspace lakehouses.

## Validate notebook (preferred over standalone diagnostic)

Single file `TIX-{key}_{topic}_validate`:

1. **Before** — prod baseline cohort counts (read-only)
2. **After** — assertions, Jira sample row, distribution table

Clone lakehouse/environment deps from an existing workspace notebook if creating via API.

## G8 — Chained ADO PR review (after G6)

**Default:** immediately after `repo_create_pull_request`, run **ado-universal-pr-review** single-PR workflow on that PR.

```
G6 PR created → G8 ado-universal-pr-review → G7 Jira implementation comment
```

### Pass-through inputs

| Input | Source |
|-------|--------|
| `pullRequestId` | G6 `repo_create_pull_request` response |
| `repositoryId` | `Traffix Medallion Production` |
| `project` | `Traffix Medallion` |
| Jira key | Intake `TIX-{key}` — verify matches PR title |

### Skill routing

| Diff signal | Additional skill |
|-------------|------------------|
| `dl_*`, `.tmdl`, `.bim`, `SemanticModel/**` | **ado-semantic-model-pr-review** |
| `gold_*` notebooks | Gold FK `-1` check (in universal skill) |
| Silver/datamart only | Notebook + Jira fit + Fabric validation cross-check |

### Implement-skill checks (on top of universal review)

- PR validation tables match G5 workspace results
- Playbook B: test plan includes **post-merge full reload**
- Known limitations from validation documented in PR
- Jira requirements fit uses G1 acceptance criteria

### Mode behavior

| Mode | Post ADO overview thread? |
|------|---------------------------|
| Standard | Draft recommendation first; post only after user approves |
| Fast | Post per universal skill (duplicate detection applies) |

### G7 coordination

- G8 may post a **PR review** Jira comment when Approved / Approved with follow-ups
- G7 posts the **implementation** comment (validation tables, post-merge steps) — reference G8 recommendation in one line; do not duplicate review template

## PR title template (required)

```
TIX-{key}: {Jira ticket summary — concise}
```

## PR body template (required)

```markdown
## Summary
- {bullet changes}

## Context
**[TIX-{key}]({jiraUrl})** — {ticket summary}
{root cause, affected table/notebook}
{playbook: A dimension / B silver datamart}

## Changes
| File | Change |
|------|--------|
| {notebook path} | {what changed} |

## Fabric validation (workspace `{workspaceName}`)
| Step | Notebook | Result |
|------|----------|--------|
| G2 sync | head = ADO tip | {SYNCED / note} |
| {reload/fix run} | `{notebook}` | {Completed / PASS} |
| Post-fix validation | `TIX-{key}_{topic}_validate` | {PASS/FAIL} |

**Prod baseline (bad cohort):** {count}
**Post-fix assertion:** {e.g. zero rows with sales_rep + empty currency}

**Sample (post-fix):** {from Jira SQL}

**Distribution:**
| {column} | count |
|----------|-------|
| {value} | {n} |

## Test plan
- [x] G2 workspace sync verified
- [x] Validated in `{workspaceName}`
- [ ] G8 ADO PR review (chained after PR open)
- [ ] PR approval (Traffix PR Approvers)
- [ ] Post-merge: {orchestration activity + full reload if playbook B}
- [ ] Analyst sign-off

## Out of scope
- {items}
```

## Jira comment template (G7 — implementation; after G8)

Mirror PR validation section. Include PR link, branch, post-merge reload steps, known limitations.

**PR review (G8):** {Approved | Approved with follow-ups | Not Approved | …} — see ADO overview on PR #{id}.

If G8 already posted the universal-skill review comment, keep G7 to implementation + validation only.

## ADO push

```powershell
.\scripts\ado-push-notebook-curl.ps1 `
  -LocalFile "path\notebook-content.py" `
  -Branch "feature/TIX-29903-fix-home-currency-mrt-load" `
  -OldObjectId "<parent commit>" `
  -RepoPath "/silver_3gtms_hub_datamart_batch_01.Notebook/notebook-content.py" `
  -Message "TIX-29903: summary"
```

Prefer **curl.exe** — PowerShell `Invoke-RestMethod` may fail TLS to dev.azure.com.
