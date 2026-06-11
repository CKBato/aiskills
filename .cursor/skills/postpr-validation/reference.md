# PostPR Validation — reference

## Playbook detection (from Jira)

| Signal | Playbook | Primary prod table(s) |
|--------|----------|------------------------|
| `dim_*`, seed `-1`, `undefined` attribute | A — Dimension | `LH_Gold.*.dim_*` |
| `mrt_*`, silver datamart, null/empty column | B — Silver | `LH_Silver.*.mrt_*` |
| `fact_*`, gold measure/column, `*_home` | C — Gold fact | `LH_Gold.*.fact_*` |
| `dl_*`, semantic model, TMDL | D — Semantic | Model + gold source columns |

If unclear, parse **Implementation** Jira comment from `traffix-jira-fabric-implement` for table/notebook names.

## Extracting PR context from Jira

Search ticket comments (newest first) for:

| Field | Patterns |
|-------|----------|
| PR URL | `dev.azure.com` + `pullrequest/` |
| PR number | `PR #1379`, `ADO PR 1379` |
| Workspace | `TIX-28531-...`, validation workspace name |
| Validate notebook | `TIX-{key}_*_validate` |
| Target table | `LH_Silver.`, `LH_Gold.`, `mrt_`, `fact_`, `dim_` |
| Sample | load number, SQL in description |
| Known limitations | bullet in Implementation comment |

`getJiraIssue` with `expand` comments if needed; or `listWorkItemComments` via search in issue body.

## PR merge check (read-only)

`repo_get_pull_request_by_id` — `status` **Completed** (3) = merged. If **Active** (1), stop with **BLOCKED — PR not merged** Jira comment (draft for user approval unless they said post anyway).

Do **not** merge PRs from this skill.

## Prod reload confirmation

Before prod assertions, confirm reload happened. Sources (read-only):

1. User message (“reload done”, “orchestration completed”)
2. Jira comment from ops/author noting prod reload
3. ADO PR description test-plan checkbox (informational only)

If reload status unknown, run prod read-only checks anyway but mark Jira result **PASS with caveat — prod reload not confirmed** or **BLOCKED** per user preference. Default: **BLOCKED** until user confirms reload.

## Validation workspace rules

| Allowed | Forbidden |
|---------|-----------|
| Run `TIX-{key}_*_validate` **prod section** in validation workspace | Any notebook run in **Traffix-Medallion-Production** |
| `spark.read` on `production_lakehouse_*_abfss` | `.write`, `updateDefinition`, git sync in prod WS |
| Create/update validate notebook in **validation** workspace only | Trigger orchestration / semantic refresh in prod |
| `az login` tokens for Fabric read + notebook run in validation WS | `repo_vote_pull_request`, code push |

Use `%run spark_configuration` + `silver_configuration` or `gold_configuration` as in pre-PR validate notebooks.

## Standard prod assertion pack (all playbooks)

Run after ticket-specific checks:

| # | Check | Pass criteria |
|---|-------|---------------|
| R1 | Table readable | `spark.read` prod path succeeds |
| R2 | Row count | Within ±{documented}% of pre-merge baseline from Jira/PR, or user-provided baseline |
| R3 | Grain | `COUNT(*) = COUNT(DISTINCT {grain_key})` when ticket defines grain |
| R4 | Untouched columns | Null-rate on 3–5 non-changed columns stable vs PR validation snapshot (±2pp) |
| R5 | Downstream join | At least one gold/report join path still returns rows for Jira sample |

## Playbook B — Silver (TIX-29903 pattern)

```python
prod_mrt = spark.read.format("delta").load(production_lakehouse_silver_abfss + "/Tables/tgtms_hub/mrt_load")
empty_expr = F.col("home_currency").isNull() | (F.trim(F.col("home_currency")) == "")
has_rep = F.col("sales_rep_id").isNotNull() & (F.trim(F.col("sales_rep_id")) != "")
assert prod_mrt.where(empty_expr & has_rep).count() == 0
```

Distribution + Jira sample load number.

## Playbook C — Gold (TIX-29863 pattern)

```python
prod_fact = spark.read.format("delta").load(production_lakehouse_gold_abfss + "/Tables/customer_sales/fact_customer_sales_v2")
for c in ["revenue_home", "gross_profit_home", "cost_home"]:
    assert c in prod_fact.columns
# CASE mismatches per currency → 0
```

## Playbook A — Dimension

- Bad cohort count → 0 on prod `dim_*`
- `dim_key = -1` seed exists when ticket required it
- Sample user/load from Jira resolves

## Playbook D — Semantic (read-only)

- `repo_get_file_content` on merged `main` for changed TMDL — columns exist in prod gold (spark schema check)
- Optional: analyst confirms report visual (user-driven); skill documents **Needs analyst sign-off**
- Do **not** publish or refresh semantic model from this skill

## Jira final-check comment template

```markdown
**Post-prod validation — final check (read-only)** — {PASS | FAIL | BLOCKED}

**Ticket:** {TIX-####}
**PR:** [{title}]({ado_pr_url}) — merged {date or unknown}
**Prod reload confirmed:** {Yes — source | No — blocked}
**Validation workspace:** `{name}` (prod data via `production_lakehouse_*` only; no prod workspace writes)

## Ticket-specific results
| Check | Result | Detail |
|-------|--------|--------|
| Primary assertion | Pass/Fail | {e.g. bad cohort = 0} |
| Jira sample | Pass/Fail | {load/user row} |
| Distribution | Pass/Fail | {top values} |

## Regression (read-only)
| Check | Result |
|-------|--------|
| Row count / grain | Pass/Fail — {counts} |
| Untouched columns | Pass/Fail / Skipped |
| Downstream join | Pass/Fail / N/A |

## Known limitations (unchanged)
- {from Implementation comment}

## Recommendation
- **Ready to close ticket:** {Yes | No — {reason}}
- **Follow-ups:** {TIX-####, analyst sign-off, etc.}

_Validation run: {timestamp}. Traffix-Medallion-Production workspace not modified._
```

## Duplicate detection

Skip posting if latest comment contains **`Post-prod validation — final check`** with same **PASS/FAIL** and same PR merge state. Post anew if result changes or new prod reload occurred.

## Jira close and worklog (on PASS + user confirms **yes**)

**Gate:** Step 5 **PASS** → Step 6 final-check comment posted → Step 7 user answers **yes** to close prompt. Never close on FAIL, BLOCKED, or without explicit **yes**. Prior “close when PASS” intent does **not** bypass the prompt.

### User approval prompt (Step 7)

After PASS, ask before any `transitionJiraIssue`:

```text
PostPR validation PASS for {TIX-####}. Move this ticket to Done and log work hours?

Yes / No
```

- **Yes** → run Step 8 close + worklog workflow below.
- **No** → leave ticket open; PASS comment already posted.
- **No response** → end turn; do not transition.

### MCP tools

| Action | Tool |
|--------|------|
| Current status + available transitions | `getTransitionsForJiraIssue` — `expand`: `transitions.fields` |
| Move status | `transitionJiraIssue` — `transition.id`, `fields`, `update` |
| Extra time after Done | `addWorklogToJiraIssue` |

`cloudId`: `2d0b86b9-1fbf-4e1e-a322-8645bf37910f` (from [config.json](config.json)).

### Workflow (Step 8 — after user **yes**)

1. Post **Step 6** final-check comment (PASS) and obtain **Step 7** user **yes**.
2. `getTransitionsForJiraIssue` — map chain to **Done** (`statusCategory.key` = `done` or name **Done**).
3. For each hop until Done:
   - Read `transitions[].fields` for **required** screen fields.
   - Call `transitionJiraIssue` with comment + any required fields.
   - Refresh transitions from new status.
4. On **`Complete`** (or final Done transition): include worklog in `update` if screen requires time spent.
5. Optionally `addWorklogToJiraIssue` for postPR-only minutes not included in Complete.

### Observed Traffix transition chains (examples)

Transition **ids are status-specific** — always re-fetch after each hop.

**TIX-29903 (Problem, started Backlog)**

| Step | Transition | id | Required on screen |
|------|------------|-----|-------------------|
| 1 | Commit | 141 | assignee, stakeholder groups, timetracking estimate, comment |
| 2 | Start Work | 81 | comment |
| 3 | Complete | 101 | worklog `timeSpent` (e.g. `5h`) |

**TIX-29894 (Sub-task, started Untriaged)**

| Step | Transition | id | Required on screen |
|------|------------|-----|-------------------|
| 1 | Crash In | 171 | comment, duedate, assignee, stakeholder groups |
| 2 | Complete | 101 | worklog `timeSpent` in `update.worklog` (e.g. `3h 30m`) |

### `transitionJiraIssue` payload patterns

**Commit / Triage / Crash In** (example):

```json
{
  "transition": { "id": "141" },
  "fields": {
    "assignee": { "accountId": "{assignee_account_id}" },
    "customfield_10218": [{ "id": "10280" }],
    "timetracking": { "originalEstimate": "5h" }
  },
  "update": {
    "comment": [{
      "add": {
        "body": {
          "type": "doc",
          "version": 1,
          "content": [{
            "type": "paragraph",
            "content": [{ "type": "text", "text": "PostPR PASS — closing ticket." }]
          }]
        }
      }
    }]
  }
}
```

**Complete** (worklog required):

```json
{
  "transition": { "id": "101" },
  "update": {
    "worklog": [{
      "add": {
        "timeSpent": "5h",
        "started": "2026-06-05T12:00:00.000+0000",
        "comment": {
          "type": "doc",
          "version": 1,
          "content": [{
            "type": "paragraph",
            "content": [{ "type": "text", "text": "Implement + Fabric validation + PR + post-prod read-only validation." }]
          }]
        }
      }
    }]
  }
}
```

**Supplemental postPR worklog** (after Done):

```json
{
  "issueIdOrKey": "TIX-29894",
  "timeSpent": "30m",
  "started": "2026-06-05T18:00:00.000+0000",
  "commentBody": "Post-prod read-only validation and Jira close-out."
}
```

Use `addWorklogToJiraIssue` with ADF `commentBody` per MCP schema.

### Stakeholder Groups (`customfield_10218`)

| Group | Option id |
|-------|-----------|
| Customer Sales | 10280 |
| HR & Payroll | 10283 |
| Finance | 10282 |
| Technology | 10298 |

Infer from ticket table/domain (e.g. `mrt_load` / sales rep → Customer Sales; `dim_user` / HR → HR & Payroll).

### Worklog guidance

- **Complete** transition: log total implement effort (analysis, Fabric WS, PR cycle, postPR).
- **addWorklogToJiraIssue**: use when postPR time is tracked separately (e.g. 30m after 3h 30m on Complete).
- Use ISO `started` timestamps; `timeSpent` in Jira format (`5h`, `3h 30m`, `30m`).

### Failure handling

| Error | Action |
|-------|--------|
| “Field X is required” on transition | Re-run `getTransitionsForJiraIssue` with `expand=transitions.fields`; add missing `fields` or `update` |
| No path to Done from current status | Report in chat; leave ticket open with PASS comment |
| Transition succeeds but status ≠ Done | Continue chain from new status |

## Related skills

- **traffix-jira-fabric-implement** — pre-PR validation (G5); produces Implementation Jira comment this skill consumes
- **ado-universal-pr-review** — pre-merge only; not re-run here unless user asks
