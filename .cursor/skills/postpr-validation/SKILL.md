---
name: postpr-validation
description: >-
  Post-merge read-only production validation for Traffix Medallion Jira TIX
  tickets. Runs hours or days after PR merge and prod reload; reads production
  lakehouses only from a validation workspace, verifies fix and regression,
  posts a final-check Jira comment, then prompts the user yes/no before closing
  the Jira ticket to Done and logging work hours. Input is the Jira ticket (PR
  link and validation context from Implementation comments). Use when the user says
  PostPR validation, post-prod validation, prod final check, validate production
  after merge, or gives a TIX URL after deploy.
disable-model-invocation: false
---

# PostPR Validation (read-only prod final check)

Final gate **after** PR merge + production reload. Confirms the fix on **real prod data** with **no writes** to **Traffix-Medallion-Production** workspace.

Typical timing: **few hours to next day** after `traffix-jira-fabric-implement` (G5/G6/G7/G8).

Read [config.json](config.json). Checklists and SQL: [reference.md](reference.md). Playbook context: [../traffix-jira-fabric-implement/playbooks.md](../traffix-jira-fabric-implement/playbooks.md).

## Input

**Required:** Jira ticket — URL or `TIX-####`

**Not required:** PR URL (resolve from Jira Implementation / PR review comments)

**Optional:** “reload done”, validation workspace name (default `TIX-28531-Dimension-Undefined` from config)

## Preconditions

- **user-atlassian** MCP — read Jira, post final-check comment
- **user-ado** MCP — read PR merge status (read-only)
- **Azure CLI** — `az login` for Fabric notebook run in **validation** workspace only
- Prod full reload **completed** (user confirms or documented in Jira)

## Hard guardrails

| Never | Always |
|-------|--------|
| Write/run/deploy in **Traffix-Medallion-Production** workspace | `spark.read` prod via `production_lakehouse_*_abfss` |
| Trigger orchestration, git sync, `updateDefinition` in prod | Run validate logic in **validation** workspace |
| Force semantic model refresh | Post **read-only** final-check Jira comment |
| Transition Jira to Done without explicit user **yes** after PASS | On **PASS**, prompt user **yes/no** before Done + worklog (see Step 7) |
| Merge PRs or push code | Transition + log hours **only** after user answers **yes** (Step 8) |

## Invocation examples

- `PostPR validation TIX-29903`
- `https://traffixsupport.atlassian.net/browse/TIX-29863 — prod reload done, final check`
- `Post-prod validation for TIX-29894`

## Workflow

### Step 1 — Intake

1. Parse `TIX-####` from URL or key.
2. Load ticket: **user-atlassian** `getJiraIssue` (`cloudId` from [config.json](config.json), `responseContentFormat`: `markdown`).
3. Read description + comments for: PR link, merged PR id, target table/notebook, validate notebook name, workspace used in pre-PR validation, Jira sample, known limitations.

### Step 2 — PR & reload gates

1. **PR merged?** `repo_get_pull_request_by_id` on PR from Jira. If **Active** → outcome **BLOCKED** (see Step 6); do not claim prod fix is live.
2. **Prod reload confirmed?** From user message or Jira. If **no** → **BLOCKED** or run read-only checks with explicit caveat (default **BLOCKED**).

### Step 3 — Classify playbook

Use [reference.md](reference.md) detection table (A dimension / B silver / C gold / D semantic). Prefer table names from Implementation comment.

### Step 4 — Run read-only prod validation

**Location:** validation workspace only (never prod workspace).

**Preferred:** Reuse existing `TIX-{key}_*_validate` notebook — add or run **PROD POST-MERGE** section that only reads `production_lakehouse_*` and asserts (no writes to prod paths).

If notebook missing, run inline Spark via deployed one-off validate notebook in validation WS (same read-only rules).

**Ticket-specific checks** — see [reference.md](reference.md) per playbook (primary assertion, Jira sample, distribution).

**Regression pack (all tickets):**

- R1 table readable
- R2 row count vs baseline from PR/Jira
- R3 grain integrity
- R4 untouched column null-rates (if baseline available)
- R5 downstream join spot-check

Record counts and Pass/Fail per row.

### Step 5 — Result

| Outcome | When |
|---------|------|
| **PASS** | All ticket-specific + regression checks pass; reload confirmed |
| **FAIL** | Any primary assertion or regression fail |
| **BLOCKED** | PR not merged, reload not confirmed, or prod table unreadable |

### Step 6 — Jira final-check comment (required)

**user-atlassian** `addCommentToJiraIssue` using template in [reference.md](reference.md).

Include:

- Post-prod validation — final check (read-only)
- PASS / FAIL / BLOCKED
- PR link + merge status
- Assertion table + regression table
- **Ready to close ticket:** Yes/No
- Explicit line: _Traffix-Medallion-Production workspace not modified._

**Duplicate detection:** skip if same outcome already posted for same merge/reload state.

### Step 7 — User approval before close (required on PASS)

After Step 6 and outcome is **PASS**:

1. **Stop** — do **not** call `transitionJiraIssue` yet.
2. Prompt the user for an explicit **yes/no** (use **AskQuestion** when available):

   > PostPR validation **PASS** for **{TIX-####}**. Move this ticket to **Done** and log work hours?
   >
   > **Yes** / **No**

3. Wait for the user’s answer in a **follow-up message**. A prior message like “close when PASS” is **intent only** — it does **not** skip this prompt.
4. If **No** → skip Step 8; note in chat summary: _Done transition skipped — user declined._
5. If **yes** → proceed to Step 8.

On **FAIL** or **BLOCKED**: skip Steps 7–8; do not prompt to close.

### Step 8 — Close ticket + log hours (only if user answered **yes**)

**Gate:** Step 5 **PASS** + Step 7 user answered **yes**. Never close on FAIL/BLOCKED.

See [reference.md](reference.md) § **Jira close and worklog** for transition chains, required screen fields, and MCP examples.

**8a — Discover path to Done**

1. `getTransitionsForJiraIssue` (`expand`: `transitions.fields`) — list available transitions from **current** status.
2. Chain transitions until **Done** (status id `10001`) or workflow equivalent. Common Traffix paths (status varies by starting point):

| Starting status (examples) | Typical chain → Done |
|----------------------------|----------------------|
| Backlog / Untriaged | `Commit` or `Triage` → `Start Work` / `Crash In` → **`Complete`** |
| To Do | `Start Work` → **`Complete`** |
| In Progress | **`Complete`** |

3. Re-call `getTransitionsForJiraIssue` after **each** transition — never assume transition ids across statuses.

**8b — Transition with required screen fields**

Use **user-atlassian** `transitionJiraIssue` with:

- `transition.id` — from current step in chain
- `fields` — assignee, `duedate`, `customfield_10218` (Stakeholder Groups), `timetracking.originalEstimate` as required by screen
- `update.comment` — ADF body (transition screens often require a comment even if not listed in `fields`)
- `update.worklog` — on **`Complete`**: `timeSpent` is often **required** (e.g. `"5h"`, `"3h 30m"`)

**Stakeholder Groups** (`customfield_10218`) — pick from ticket context: Customer Sales `10280`, HR & Payroll `10283`, Finance `10282`, Technology `10298`, etc.

**8c — Log work hours**

| When | How |
|------|-----|
| **Complete** transition | Include `update.worklog.add.timeSpent` + `started` (covers end-to-end implement + postPR) |
| Post-close supplement | `addWorklogToJiraIssue` for postPR-only time (e.g. `30m`) if not folded into Complete worklog |

Log **realistic** hours for the full ticket arc (analysis → Fabric validation → PR → postPR), not just the validation notebook runtime. Reference tickets: TIX-29903 **5h**, TIX-29894 **4h** (3h 30m + 30m).

### Step 9 — Chat summary

Return: Jira key, PR merge status, outcome, primary assertion numbers, final-check comment posted/skipped, **close prompt** sent (yes/no), **Done transition** (completed / skipped / pending), **worklog** logged (duration), ready-to-close recommendation.

## MCP / tool priority

| Task | Tool |
|------|------|
| Jira read/comment/transition/worklog | user-atlassian |
| PR status | user-ado `repo_get_pull_request_by_id` |
| Notebook run (validation WS) | Fabric REST + `az` |
| Prod schema peek | `spark.read` in validate notebook only |

## Relationship to implement skill

```
traffix-jira-fabric-implement (G5 pre-PR) → merge → prod reload → postpr-validation (this skill) → Jira PASS comment → user yes/no → Done + worklog
```

Pre-PR validation proves the fix in **validation** lakehouse; this skill proves it on **production** data read-only.

## When validation notebook needs a prod section

If `TIX-{key}_*_validate` lacks prod post-merge section, extend it in the **validation** workspace only (not prod WS): copy assertions from pre-PR “after” section but point reads at `production_lakehouse_*_abfss`. Commit notebook to a follow-up PR only if user asks — not required for final-check comment.
