---
name: jira-ado-requirements-validation
description: >-
  Automatically validate Jira requirements assigned to the current user against
  Azure DevOps repository and PR evidence for Traffix Medallion. Use when the
  user asks to validate my Jira, scan assigned Jira for ADO gaps, check Jira
  requirements against Azure DevOps repo/PRs, find missing requirements or data
  needed for validation, or post Jira comments tagging the Jira owner.
disable-model-invocation: false
---

# Jira assigned work -> ADO requirements validation

Validate Jira issues assigned to the current user against Azure DevOps repository
and pull request evidence, identify gaps, identify data still needed for
validation, and post a concise Jira comment tagging the Jira owner/reporter.

This skill is a **requirements-fit and evidence** workflow. It does not implement
code, merge PRs, cast ADO votes, or close Jira issues.

## Default scope

When the user omits org/project/repo, assume:

- **Jira site:** `traffixsupport.atlassian.net`
- **Jira cloudId:** `2d0b86b9-1fbf-4e1e-a322-8645bf37910f`
- **Jira project:** `TIX`
- **ADO organization:** `Traffix-Data-Infrastructure`
- **ADO project:** `Traffix Medallion`
- **ADO repository:** `Traffix Medallion Production`
- **ADO base branch:** `main`

Validate only issues assigned to the current Jira user unless the user names a
specific issue, JQL, assignee, project, or sprint.

## Preconditions

- **user-atlassian** MCP enabled and authenticated:
  - `atlassianUserInfo`
  - `searchJiraIssuesUsingJql`
  - `getJiraIssue`
  - `addCommentToJiraIssue`
- **user-ado** MCP enabled and authenticated:
  - `repo_list_pull_requests_by_repo_or_project`
  - `repo_get_pull_request_by_id`
  - `repo_get_pull_request_changes`
  - `repo_get_file_content`
  - `search_code` when available
- Optional Jira owner lookup fields:
  - Prefer reporter as the Jira owner when no explicit owner field is configured.
  - If a custom owner/stakeholder field is present in the ticket, mention it in the
    summary and tag that user instead of reporter.

If `user-ado` is unavailable in a Cloud Agent, use ADO REST only when a PAT is
available in the environment. If neither MCP nor REST is available, post no Jira
comments and report `Blocked: ADO access unavailable`.

## Inputs

| User asks | Behavior |
|-----------|----------|
| `validate my Jira` / `scan my assigned Jira` | Query open assigned Jira tickets and validate each. |
| `validate TIX-12345` | Validate that single issue even if not assigned to current user. |
| `validate my Jira due this week` | Add date criteria to the assigned-ticket JQL. |
| `chat only` / `draft only` | Do not post Jira comments. |
| `post Jira comments` / no posting override | Post one validation comment per issue after duplicate detection. |

Default JQL for batch mode:

```jql
assignee = currentUser()
AND statusCategory != Done
ORDER BY priority DESC, updated DESC
```

For large batches, paginate with `maxResults` and `nextPageToken`. Keep the chat
summary compact; post detailed evidence to Jira only when posting is allowed.

## Workflow

### Step 1 - Intake and Jira load

1. Resolve current Jira user with `atlassianUserInfo`.
2. Build JQL:
   - Specific issue: `key = TIX-####`.
   - Assigned batch: default JQL above, refined by user filters.
3. Search Jira with fields:
   - `summary`, `description`, `status`, `issuetype`, `priority`, `assignee`,
     `reporter`, `updated`, `duedate`, `comment`, `issuelinks`, `parent`.
4. For each issue, call `getJiraIssue` with `responseContentFormat: markdown` to
   read full description and comments. Comments often contain PR links, validation
   runs, screenshots, and implementation notes.

### Step 2 - Extract requirements

Extract a concise requirements list from:

- Jira summary and description.
- Explicit acceptance criteria, numbered sections, tables, desired outcomes,
  backend filters, target tables, target schemas, source systems, required
  columns, refresh/reload expectations, and validation examples.
- Jira comments that refine scope.
- Linked issues or parent issue only when needed to understand missing context.

Write each requirement as a testable row:

| Requirement type | Examples |
|------------------|----------|
| Deliverable | New notebook/table/model/column/pipeline/control row exists. |
| Behavior | Filter, dedupe, incremental logic, key default, RLS rule, DAX measure. |
| Data contract | Source table/column, target schema, grain, key, nullable/default rules. |
| Validation | Sample load/customer/date, row count, expected result, screenshot, reload. |

If a ticket has no testable requirement, classify it as **Needs requirements** and
ask the owner/reporter for the missing detail in the Jira comment.

### Step 3 - Resolve ADO evidence

Resolve candidate ADO evidence in this order:

1. **Explicit PR links** in Jira description/comments.
2. **Jira key in active PRs** from `repo_list_pull_requests_by_repo_or_project`
   across the ADO project. Match title, description, branch, and work item refs.
3. **Branch names** containing the Jira key.
4. **Repo file/content search** for:
   - Jira key (`TIX-####`)
   - target table names
   - notebook names
   - semantic model names
   - control-table rows
   - source column names
5. **Related artifacts** referenced by the ticket even when unchanged in a PR:
   batch notebooks, pipeline JSON, semantic model definitions, existing
   notebook/source patterns.

For PR evidence, fetch:

- PR metadata, status, title, description, author, source/target branch.
- Changed files and latest iteration diff.
- Work item refs when available.
- Relevant file content for related unchanged artifacts.

### Step 4 - Compare requirements to ADO evidence

For each requirement, assign one result:

| Result | Meaning |
|--------|---------|
| **Pass** | ADO evidence clearly satisfies the Jira requirement. |
| **Gap** | A requirement is missing or implemented contrary to the ticket. |
| **Partial** | Some evidence exists, but scope is incomplete or pending another PR/config step. |
| **Needs validation data** | Code evidence exists, but ticket lacks sample data, expected output, or validation criteria. |
| **Needs owner response** | Ambiguous requirement or business decision needed. |
| **No ADO evidence** | No PR/branch/file evidence found. |

When reviewing notebooks or semantic models, layer in the relevant established
skills:

- **Gold notebooks:** apply the Gold FK/surrogate-key `-1` default gate from
  `ado-universal-pr-review`.
- **Semantic models:** apply `ado-semantic-model-pr-review` guardrails when
  `.bim`, `.tmdl`, PBIP, or `*.SemanticModel` artifacts are involved.
- **Post-merge prod validation:** if the user asks for post-production validation,
  switch to `postpr-validation` instead of this pre/post-PR evidence scan.

### Step 5 - Identify data needed for validation

For every **Partial**, **Needs validation data**, or **Needs owner response** row,
list the minimum data needed to validate:

- Sample identifiers: load id, order id, customer id, invoice id, employee id,
  date range, organization/division.
- Expected before/after value or acceptance rule.
- Required source/target tables and columns.
- Required refresh/reload scope: single notebook, root batch, semantic model
  refresh, GraphQL/schema update, prod reload.
- Validation query idea: row count, anti-join, null-rate, duplicate/grain check,
  sample record assertion, DAX/relationship check.

Do not invent expected values. If expected values are missing, mark **Needs
validation data** and ask for them in the owner-tagged Jira comment.

### Step 6 - Outcome per Jira issue

Choose exactly one outcome:

| Outcome | When |
|---------|------|
| **Requirements fit: Pass** | All testable requirements have ADO evidence and required validation data. |
| **Requirements fit: Gaps found** | One or more requirements are missing or contradicted by ADO evidence. |
| **Requirements fit: Partial** | Evidence exists but scope is incomplete or split across PR/config/data steps. |
| **Requirements fit: Needs data** | Implementation evidence may exist, but validation cannot be completed without sample/expected data. |
| **Requirements fit: No ADO evidence** | No matching PR/branch/file evidence found. |
| **Requirements fit: Blocked** | Tool/auth/API access prevents a reliable comparison. |

Severity guidance:

- **Critical/High gap:** wrong target table/model, missing required filter,
  broken batch orchestration, data-loss overwrite risk, wrong grain, missing
  key default on Gold facts/bridges/dimensions, placeholder semantic model
  logicalId, or no implementation evidence for a concrete ticket deliverable.
- **Medium gap:** weak validation evidence, missing sample data, partial config
  follow-up, traceability/title mismatch.
- **Low gap:** typo, naming inconsistency, optional cleanup.

### Step 7 - Jira comment

Post one comment per issue unless the user said `chat only` / `draft only`.

**Tagging rule:**

1. Tag the explicit owner/stakeholder field if present and mentionable.
2. Otherwise tag the Jira reporter.
3. If the reporter is the current user or not mentionable, tag the assignee only
   when the assignee is not the current user; otherwise write `Owner follow-up:`
   without a mention.

Use a real Atlassian mention only when an account id is available. For reliable
tagging, prefer `addCommentToJiraIssue` with `contentFormat: adf` and an ADF
mention node:

```json
{
  "type": "mention",
  "attrs": {
    "id": "{accountId}",
    "text": "@{displayName}",
    "accessLevel": ""
  }
}
```

If posting markdown, use a plain `@Display Name` fallback only when the MCP does
not support ADF in the current environment. Do not guess account ids.

**Duplicate detection:** before posting, scan existing comments. Skip if a
comment already contains:

- `Jira requirements validation — ADO evidence`
- the same outcome label
- and the same evidence PR ids or `No ADO evidence` marker.

Post a new comment when the outcome changes, when a PR iteration changes, or when
new ADO evidence is found.

#### Jira comment template

```markdown
**Jira requirements validation — ADO evidence**

{ownerMentionOrOwnerLine} please review the gaps / validation data below.

**Outcome:** Requirements fit: {Pass | Gaps found | Partial | Needs data | No ADO evidence | Blocked}

**ADO evidence reviewed:**
- PRs: {links or `_None found_`}
- Branches/files: {branches/files or `_None found_`}
- Related artifacts: {batch notebooks / semantic models / pipelines / `_None_`}

**Requirements fit**
| Jira requirement | ADO evidence | Result |
|------------------|--------------|--------|
| ... | ... | Pass / Gap / Partial / Needs validation data / Needs owner response / No ADO evidence |

**Gaps / risks**
- {Critical/High/Medium/Low gap bullets, or `_None identified._`}

**Data needed to validate**
- {sample ids, expected values, source/target tables, validation query ideas, reload requirements, or `_None._`}

**Recommended next steps**
- {actionable next steps for assignee / owner / reviewer}
```

For **Pass**, keep next steps short and do not ask for more data. For **Gaps
found**, be direct about the requirement and the exact ADO file/PR evidence.

### Step 8 - Chat summary

Finish with a compact markdown table:

| Jira | Status | Outcome | ADO evidence | Gap count | Data needed | Jira comment |
|------|--------|---------|--------------|-----------|-------------|--------------|

Use counts like `0`, `1H`, `2M`, `1H/2M`. For comments use `Posted`, `Skipped
duplicate`, `Draft only`, or `Blocked`.

## Batch scheduling prompt

Use this prompt for Cursor Automations or the Cursor agent CLI:

```text
Follow skill jira-ado-requirements-validation. Validate every open Jira issue assigned to the current user in Traffix TIX against Azure DevOps project "Traffix Medallion" / repo "Traffix Medallion Production". For each issue, extract Jira requirements, resolve matching ADO PRs/branches/files, compare requirements to ADO evidence, list gaps and data needed to validate, then post a Jira comment tagging the Jira owner/reporter unless a duplicate validation comment already exists. Finish with a compact summary table.
```

## Guardrails

- Do **not** modify code, create branches, create PRs, merge PRs, or transition
  Jira statuses as part of this skill.
- Do **not** cast ADO votes.
- Do **not** post Jira comments when ADO evidence could not be accessed; report
  `Blocked` in chat instead.
- Do **not** claim requirements pass based only on screenshots. Screenshots may
  support validation, but repo/PR evidence or documented platform config is
  required for **Pass**.
- Do **not** preserve ambiguity. If the ticket does not specify expected samples,
  mark **Needs validation data** and ask the owner/reporter for exactly what is
  missing.
- Keep comments actionable and concise; batch minor observations in one comment.
