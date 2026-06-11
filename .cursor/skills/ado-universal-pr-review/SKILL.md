---
name: ado-universal-pr-review
description: >-
  Azure DevOps PR reviews for Traffix Medallion (Traffix-Data-Infrastructure)
  and any ADO PR: notebooks, Power BI / Fabric semantic models (BIM, TMDL,
  PBIP), and general code. Posts review threads with an explicit approval
  recommendation (Approved / Not Approved / etc.), critical issues, Gold
  notebook FK/surrogate-key (-1 default) validation, non-blocking notes via
  user-ado MCP, mandatory Jira requirements cross-check against mapped tickets
  (PR description/title) in every ADO overview, and Jira ticket comments via
  user-atlassian MCP when Approved or Approved with follow-ups. Use for
  dev.azure.com PR links,
  requests to review PRs,
  Tuesday/Thursday batch review of all Active PRs in Traffix Medallion,
  Traffix Medallion project-wide PR sweeps, or daily morning Cursor
  Automations batch review at 8:00am.
disable-model-invocation: false
---

# ADO universal PR review (recommendation + post to PR)

## Tone

Post **clear, actionable** review feedback on the PR. You may state an **approval recommendation** and call out **critical** or **high** issues plainly — but frame them as **reviewer guidance**, not as an ADO policy gate:

- Do **not** use phrases like “I am blocking this merge” or “required approver” unless the user asked you to cast an ADO vote.
- Do **not** call `repo_vote_pull_request` unless the user explicitly asks.
- Be direct about bugs, data-risk, and broken orchestration; separate **must-fix** items from **nice-to-have** items using the severity rubric below.

## Approval recommendation (required on every review)

Every overview thread and chat summary **must** include exactly one recommendation label from this list:

| Recommendation | When to use |
|----------------|-------------|
| **Approved** | No critical/high issues found; changes match intent; **Jira requirements fit Pass** (when mapped). **Gold FK check Pass** when gold notebooks change. Safe to merge from a review perspective. Minor suggestions only. |
| **Approved with follow-ups** | No critical issues; merge is reasonable; ticket gaps are documented follow-ups (not blockers). **Gold FK check Pass or N/A** — never **Fail**. Medium/low items or open questions tracked post-merge. |
| **Not Approved** | One or more **Critical** or **High** issues that should be resolved before merge (wrong target table, debug left in prod path, title/diff mismatch hiding scope, broken DAG dependency, data-loss overwrite risk, secrets, **Gold FK/surrogate keys left nullable instead of defaulting to -1**, **semantic model `.platform` placeholder/missing `logicalId` (blocks Fabric Git sync)**, **ticket deliverable missing with no acceptable platform path**, etc.). **Always list why** in **Critical / high issues**. |
| **Needs author response** | Cannot recommend approval yet — missing context, unanswered design questions, or dependency on another PR/branch. State what response or evidence would change the recommendation. |
| **Defer** | PR is draft, abandoned scope, superseded by another PR, or blocked by external failure (CI, policy, known platform error). Say what to do next. |

**Rules**

- **Not Approved** requires at least one **Critical** or **High** issue with a concrete **why** (file/symbol/behavior), not vague discomfort.
- If recommendation is **Approved** or **Approved with follow-ups**, the **Critical / high issues** section should say `_None identified._` or omit empty bullets — do not invent severity.
- **Gold FK gate:** If any **changed** gold notebook final write leaves `*_key` / surrogate columns without `-1` default → Gold FK check = **Fail** → recommendation **must be Not Approved** (or **Needs author response** if diff inconclusive). Do **not** use **Approved with follow-ups** to defer missing `coalesce(..., -1)` on gold bridge/fact/dimension outputs.
- **Semantic model `.platform` gate:** If any **new/changed** `*.SemanticModel/.platform` has placeholder `logicalId` (`00000000-0000-0000-0000-000000000000`), missing, or malformed GUID → Git integration check = **Fail** → recommendation **must be Not Approved** (or **Needs author response** if file not reviewed). Do **not** use **Approved with follow-ups** to defer placeholder `logicalId`.
- Changing the recommendation after a new iteration: post a **new** overview thread (do not edit old threads); duplicate detection applies per iteration.

## Issue severity rubric

Use these labels in **Critical / high issues** and optionally in line threads:

| Severity | Meaning | Examples |
|----------|---------|----------|
| **Critical** | Likely wrong data, prod outage, security exposure, or merge would break scheduled jobs | Wrong gold table name; overwrite of unrelated fact; hardcoded secrets; notebook path missing from batch; semantic model pointing at non-existent columns |
| **High** | Significant correctness or operability risk; should fix before merge | Debug `display()` on hot path; PR title/branch does not match changed files; filter change alters grain without validation; DAX grain mismatch; **Gold notebook writes nullable `*_key` / surrogate key columns without `coalesce(..., lit(-1))` or equivalent -1 default** |
| **Medium** | Should fix soon; acceptable to merge with follow-up if author acknowledges | Markdown/copy-paste noise; missing Jira link; weak test evidence |
| **Low** | Style, naming, optional cleanup | Typos in comments; minor refactors |

In the overview thread, list **Critical** and **High** under **Critical / high issues**. Put **Medium** / **Low** under **Suggestions** unless the user asked to show all severities.

## Default org / project (when user omits them)

If the user asks for a PR review or batch review without naming a project, assume:

- **Organization:** `Traffix-Data-Infrastructure`
- **Project:** `Traffix Medallion`
- **Project URL:** `https://dev.azure.com/Traffix-Data-Infrastructure/Traffix%20Medallion`

If they name another org/project, use theirs instead.

## Role

- **Single-PR mode:** one pull request end-to-end.
- **Batch mode (e.g. Tuesday / Thursday):** list **all Active** PRs in Traffix Medallion (across repos), then run **single-PR mode** for each. Work in repository order returned by the API; paginate with `top` / `skip` if there are more than 100 PRs.

Cursor does not run on a calendar by itself in a normal chat — use **Cursor Automations** (see below) for a daily schedule, or on Tuesdays/Thursdays start a chat and invoke this workflow manually (or rely on auto-invocation from the skill description).

## Preconditions

- **user-ado** MCP enabled and authenticated.
- **user-atlassian** MCP enabled and authenticated (for Jira requirements fetch and approval comments).
- Inputs: PR URL **or** (`project`, `repositoryId` / name, `pullRequestId`). For batch: `project` only.

## Tooling (read MCP tool JSON schemas before first use in a session)

| Goal | Tool |
|------|------|
| List PRs in a project (all repos) | `repo_list_pull_requests_by_repo_or_project` — pass **`project` only** (omit `repositoryId`), `status` **Active**, paginate `top` / `skip` |
| PR metadata + files + work items | `repo_get_pull_request_by_id` — `includeChangedFiles`, `includeWorkItemRefs` true when useful |
| Diffs | `repo_get_pull_request_changes` — `includeDiffs` / `includeLineContent` true unless too large; then paginate or sample |
| PR-level summary thread | `repo_create_pull_request_thread` — **omit** `filePath` |
| Line-level note | `repo_create_pull_request_thread` — `filePath` + line anchors |
| Avoid duplicate spam | `repo_list_pull_request_threads` — skip a new overview if a recent thread already starts with `## PR review — Cursor (non-blocking)` **for the same iteration** and already contains the same **Approval recommendation** context (re-post if iteration changed or recommendation would change) |
| Follow-up in thread | `repo_reply_to_comment` |
| ADO approve/reject vote | `repo_vote_pull_request` — **only if user explicitly asks** |
| Resolve Jira cloud | `getAccessibleAtlassianResources` — use Traffix `cloudId` (`traffixsupport.atlassian.net` → UUID) |
| Read mapped Jira ticket | `getJiraIssue` — `issueIdOrKey`, `responseContentFormat`: `markdown` |
| Read related repo artifacts | `repo_get_file_content` — pipelines/notebooks referenced by ticket but not in PR diff |
| Post Jira ticket comment | `addCommentToJiraIssue` — `contentFormat`: `markdown` (approved PRs only) |

## Jira requirements cross-check (mandatory on every review)

**Always** map Jira from the PR and cross-check ticket requirements against the PR diff **before** finalizing the **Approval recommendation**. This is not optional for approved PRs only — it informs **every** recommendation and **every** ADO overview explanation.

### Map Jira key from the PR

Resolve **one primary** Jira issue key in this order:

1. **PR description** — `atlassian.net/browse/TIX-12345` (or any `PROJECT-123` browse URL).
2. **PR description / title** — regex `\b([A-Z][A-Z0-9]+-\d+)\b` (Traffix default: `TIX-\d+`).
3. **PR title** — same regex if description has no key.

If **multiple distinct keys** appear (e.g. title `TIX-29547` but branch `TIX-29652`):

- Prefer the key from an **explicit Jira URL in the description**.
- Otherwise prefer the key in the **title**.
- Note branch/other keys under **Work items / traceability** — do not post to every key unless the user asks.

If **no Jira key** is found: omit the **Jira requirements fit** table; in **Work items / traceability** state `_No Jira key in PR title or description._` Chat summary: `Jira req check: N/A (no key)`.

### Requirements check (when Jira is mapped) — required steps

1. `getJiraIssue` for the mapped key (`cloudId`: `2d0b86b9-1fbf-4e1e-a322-8645bf37910f` for `traffixsupport.atlassian.net`, or from `getAccessibleAtlassianResources`).
2. Extract ticket **summary**, **description**, and any explicit acceptance criteria (tables, watermarks, targets, frequencies, deliverables).
3. Compare each requirement to the **PR diff** and, when needed, **related repo artifacts** (e.g. pipeline JSON, batch notebooks) via `repo_get_file_content` or `search_code` — tickets often expect behavior delivered by existing platform code not changed in the PR.
4. Build a **Jira requirements fit** table (see overview template) with **Pass** / **Fail** / **Follow-up** / **Needs validation** per requirement row.
5. Tie **Why (recommendation)** directly to this table — e.g. “Approved because all ticket requirements Pass via control-table row + existing incremental pipeline.”

### How Jira fit affects the recommendation

| Jira fit finding | Typical severity | Approval impact |
|------------------|------------------|-----------------|
| Ticket deliverable missing from PR with no documented follow-up / existing platform path | **High** | **Not Approved** or **Needs author response** |
| Partial ticket scope in PR; remainder is ops/Fabric config or separate story | **Medium** | **Approved with follow-ups** if code path is correct |
| All ticket requirements Pass (including via existing pipelines/notebooks) | — | Supports **Approved** |
| `getJiraIssue` fails (auth/permission) | — | Proceed with code review; table = **Needs validation**; do not claim ticket alignment |

Ticket mismatch alone is not enough for **Not Approved** without a concrete **why** (which requirement, which file/behavior).

### ADO overview (always include when Jira mapped)

Include the **Jira requirements fit** section in **every** overview thread when a key was resolved. Put it **before** **Critical / high issues** so reviewers see traceability first.

### Jira ticket comment (approved PRs only)

After the ADO overview, when recommendation is **Approved** or **Approved with follow-ups**:

1. Post a short comment on the mapped Jira issue using the template below.
2. Include the same **Requirements fit** bullets (condensed).
3. **Duplicate detection:** skip if an existing comment already contains **`PR review — ADO PR {pullRequestId}`** with the **same** recommendation label; post anew if iteration or recommendation changed.
4. Skip Jira **comment** (not the requirements check) for **Not Approved**, **Needs author response**, and **Defer** unless the user explicitly asks.

### Jira comment template (short)

```markdown
**PR review — ADO PR {N}** ([link]({ado_pr_url}))

**Recommendation:** {Approved | Approved with follow-ups}

**Requirements fit ({JIRA-KEY}):**
- … (1–4 bullets: Pass / gap / follow-up per ticket requirement)

**Why approved:** One sentence tied to requirements fit.

**Follow-ups (if any):** …

**Next steps:** …
```

### Chat / batch reporting

- Single-PR chat summary: **Jira req check:** `TIX-##### — Pass` | `Partial (follow-ups)` | `Gap (see review)` | `Needs validation (auth)` | `N/A (no key)`; plus **Jira comment:** `Posted` | `Skip dup` | `N/A`.
- Batch table columns: **Jira req check** (`Pass` / `Partial` / `Gap` / `N/A` / `Needs validation`) and **Jira comment?** (`Yes` / `Skip dup` / `N/A`).

## Single-PR workflow

1. Resolve **repositoryId**, **pullRequestId**, **project** (from URL or user).
2. `repo_get_pull_request_by_id` (+ changed files / WI refs as needed).
3. `repo_get_pull_request_changes` (latest iteration unless user specifies another).
4. **Classify** changed files: notebooks (`*.ipynb`, `notebook-content.py`), semantic model artifacts (`.bim`, `.tmdl`, `SemanticModel/**`, PBIP / `definition/**`, etc.), **other**. Flag **Gold notebooks** (see below).
5. **Map Jira** from PR description/title; run **Jira requirements cross-check** (`getJiraIssue` + diff comparison + related artifacts if needed). Draft the **Jira requirements fit** table.
6. **Review** using the checklists below (only buckets that appear). For any **Gold notebook** in the diff, run the **FK / surrogate key (-1 default) approval validation**. Identify **Critical / High** issues — include ticket gaps from step 5 when they qualify.
7. Choose **Approval recommendation** using the approval table and **How Jira fit affects the recommendation**. Write **Why (recommendation)** referencing Jira fit when a key was mapped.
8. **Post** threads to ADO:
   - One **overview** thread (no `filePath`) using the template below — **must** include **Jira requirements fit** when a key was mapped.
   - Optional extra threads: one theme per thread; prefer line anchors for specific Critical/High findings when helpful.
9. **Jira ticket comment** — if **Approved** or **Approved with follow-ups**, post short comment per **Jira requirements cross-check** (unless duplicate or no key).
10. Short **chat summary**: PR link, **Approval recommendation**, **Jira req check** status, **Gold FK check** (if applicable), Critical/High counts, threads created, duplicate skips, **Jira comment** status.

## Batch workflow (Traffix Medallion)

1. `repo_list_pull_requests_by_repo_or_project` with `project`: **`Traffix Medallion`**, `status`: **Active**, `top`: 100, `skip`: 0; repeat with higher `skip` until the list is empty.
2. For **each** PR in the list, run **Single-PR workflow** using that PR’s `repository` / `repositoryId` and `pullRequestId` from the list payload.
3. End with a **markdown table** in chat:

| Repository | PR | Title | Approval recommendation | Jira req check | Gold FK check | Critical/High | Posted overview? | Jira comment? | Skipped (duplicate)? |

Use short recommendation labels (`Approved`, `Not Approved`, etc.). **Jira req check:** `Pass`, `Partial`, `Gap`, `N/A`, `Needs validation`. **Gold FK check:** `Pass`, `Fail`, `N/A`, or `Needs response`. In **Critical/High**, use counts like `0`, `1C`, `2H`, `1C/1H`. **Jira comment?:** `Yes`, `Skip dup`, `No key`, `N/A` (not approved).

## Gold notebook detection

Treat a changed notebook as a **Gold notebook** when **any** of the following apply:

- Path/name matches `gold_*`, `gold_fact_*`, `gold_dimension*`, `gold_obt_*`, or similar Traffix gold naming.
- Notebook markdown or code targets `current_lakehouse_gold_abfss`, `production_lakehouse_gold_abfss`, or paths under `/Tables/` used for gold facts/dimensions/bridges (not silver/bronze-only paths).
- PR title/description says gold fact, gold dimension, or OBT build.

Silver-only or dimension-batch notebooks that **only** touch silver/bronze are out of scope for this check unless they write gold tables.

## Gold notebook approval validation: FK / surrogate keys (-1 default)

**Team standard (Traffix Medallion gold):** dimension foreign keys and surrogate keys written to gold facts/dimensions/bridge tables must **not** remain `NULL` when a join misses. Use **`-1`** as the unknown/default member key unless the PR explicitly documents an approved exception.

### Required on every Gold notebook review

1. Scan the diff (especially **final select**, `df_fact`, `df.select`, merge/overwrite write paths) for columns whose names end in `_key` or match known surrogate keys (`load_key`, `customer_key`, `carrier_key`, `division_key`, `*_date_key`, etc.).
2. For each such column in **new or changed** gold output logic, verify a **-1 default** when the source join can miss, e.g.:
   - `F.coalesce(F.col("customer_key"), F.lit(-1))`
   - `F.when(F.col("load_key").isNull(), F.lit(-1)).otherwise(F.col("load_key"))`
   - SQL `coalesce(customer_key, -1)` / `ifnull(..., -1)`
3. Flag **left joins** to dimension tables that feed `*_key` columns without a subsequent coalesce/default — common miss pattern.
4. Do **not** flag nullable **non-key** attributes (names, amounts, dates, status strings) unless they are misnamed `*_key` columns.

### Key approval gate (mandatory)

**Before recommending Approved or Approved with follow-ups on any PR that touches gold notebooks:**

1. Complete the FK scan on **every changed gold write path** (facts, dimensions, **bridges**).
2. Set **Gold FK / surrogate key check** to **Pass**, **Fail**, **Needs author response**, or **N/A**.
3. If **Fail** → recommendation **Not Approved** until every output `*_key` uses `coalesce(..., -1)` (or documented team exception). **No exceptions** for “logic fix looks good” or “only bridge keys missing defaults.”
4. In overview **Why (recommendation)**, state explicitly when Gold FK gate blocked approval.

Bridge tables (`bridge_*`) are gold outputs — same `-1` rule as facts. Pattern in same notebook is not sufficient; the **changed section** must comply.

### How results affect approval

| Finding | Severity | Approval impact |
|---------|----------|-----------------|
| New/changed gold write leaves one or more `*_key` columns nullable (no -1 default) | **High** | **Not Approved** — hard gate; fix required before merge approval |
| Gold notebook in PR but FK check inconclusive (diff truncated, logic in unchanged cells) | — | **Needs author response** — ask author to confirm all output keys default to -1 |
| Gold notebook passes FK check | — | Note **Pass** in overview; eligible for Approved / Approved with follow-ups |
| Silver-only PR (no gold notebooks) | — | State **N/A** in overview |

### Line-thread guidance

When a specific nullable key is visible in the diff, prefer one line thread on the final-select/join site citing the column name and suggested `coalesce(..., lit(-1))` pattern, in addition to the overview bullet.

## Review checklists

### Notebooks (`*.ipynb`, `notebook-content.py`)

Intent in markdown; secrets not in source/outputs; seeds/docs where randomness matters; reasonable outputs; imports/deps consistent with team norms; **no debug cells** (`display`, sample load filters) left on production write paths; batch DAG entries reference notebooks that exist; target table names match PR intent.

### Gold notebooks (additional — mandatory when detected)

Run **FK / surrogate key (-1 default) approval validation** (section above). Also check: gold target path/table name correct; overwrite/merge mode intentional; grain documented in markdown; joins to dims use expected keys; date keys (`yyyyMMdd` int) separate from nullable FK rule unless named `*_date_key` and team treats them as keys (still prefer -1 for missing dim keys, not missing dates — use null or sentinel only if existing notebook pattern does so consistently).

### Semantic models (BIM / TMDL / PBIP / Fabric)

**Mandatory:** read and follow **ado-semantic-model-pr-review** when the diff includes semantic model artifacts. Run its **guardrail checklist**, include the **Semantic model guardrails** table in the overview thread, and use its approval conditions for semantic-model-specific Fail → Critical/High mapping.

**Semantic model `.platform` hard gate:** If the PR adds or changes any `*.SemanticModel/.platform` and `config.logicalId` is missing, malformed, or the placeholder `00000000-0000-0000-0000-000000000000` → Git integration check = **Fail** → recommendation **must be Not Approved** (Fabric prod sync fails with “Missing or corrupted system files”). Do **not** use **Approved with follow-ups** to defer a placeholder `logicalId`.

Quick triage only if the child skill is unavailable: **`.platform` `logicalId` valid and non-zero**; DAX vs grain; relationships and filter direction; RLS/OLS intent; partitions/refresh mode; source columns exist in lakehouse/gold. Optional **user-powerbi-modeling-mcp** only if configured and the artifact is local—never required.

### Other code / infra / docs

Correctness, security, tests/observability, CI/CD safety.

## Overview comment template (PR-level thread)

First `repo_create_pull_request_thread` call: **no** `filePath`. Title line must include **`(non-blocking)`** for duplicate detection (feedback is reviewer guidance; ADO vote is separate unless requested).

```markdown
## PR review — Cursor (non-blocking) — PR _N_

**Approval recommendation:** [Approved | Approved with follow-ups | Not Approved | Needs author response | Defer]

**Why (recommendation):** One or two sentences tying the label to **Jira requirements fit** (when mapped) and code findings below.

**Change type:** [Notebooks | Semantic model | Mixed | General]

**Jira requirements fit** _(mandatory when a Jira key is mapped from PR description/title; omit if no key)_
| Ticket requirement | PR / platform evidence | Result |
|--------------------|------------------------|--------|
| … (from ticket summary/description) | … (file, table, pipeline, behavior) | Pass / Fail / Follow-up / Needs validation |

**Jira fit summary:** _All Pass_ OR _N gap(s) / follow-up(s) — see table and Critical/high issues_

**Semantic model guardrails** _(include when Change type is Semantic model or Mixed; see ado-semantic-model-pr-review)_
| Category | Result | Notes |
|----------|--------|-------|
| Traceability & scope | Pass / Fail / Needs validation / N/A | … |
| Git integration (`.platform`) | Pass / Fail / Needs validation / N/A | `logicalId` valid GUID; not `00000000-…` placeholder |
| Fabric connection (`expressions.tmdl`) | Pass / Fail / Needs validation / N/A | `DatabaseQuery` → prod Fabric SQL host + warehouse id |
| Source binding | … | … |
| Relationships & grain | … | … |
| DAX & measures | … | … |
| RLS / OLS / security | … | … |
| Formatting & parameters | … | … |
| Performance & refresh | … | … |
| Deployment hygiene | … | … |

**Guardrail summary:** _All passed_ OR _N Fail → see Critical/high issues_ (**.platform placeholder `logicalId` Fail = Not Approved hard gate**, same as Gold FK Fail)

**Summary:** …

**Gold FK / surrogate key check** _(N/A if no gold notebooks in PR; **Fail blocks Approved / Approved with follow-ups**)_
- **Result:** [Pass | Fail | Needs author response | N/A]
- **Gate:** _Pass or N/A required for merge approval recommendation._
- **Notes:** List any `*_key` columns missing `-1` default, or confirm all checked output keys use `coalesce(..., -1)` / equivalent.

**Critical / high issues**
- **[Critical|High]** — … _(file / area)_ — …

_(If none: `_None identified._`)_

**Observations**
- …

**Suggestions** _(medium/low or optional improvements)_
- …

**Questions**
- …

**Test / validation ideas** _(optional)_
- …

**Work items / traceability** _(if any)_: …
```

Replace `_N_` with the pull request id.

## Guardrails

- Few threads per PR: batch small nits into the overview.
- **Posting** is assumed when the user asks to “review” ADO PRs; if they say “draft only” or “chat only”, do not call `repo_create_pull_request_thread`.
- Professional tone; no personal remarks.
- On MCP auth or permission errors, stop and report—do not claim comments were posted.
- **Not Approved** must be earned by Critical/High findings — do not withhold approval for stylistic preferences alone.
- **Jira requirements cross-check** is mandatory on every review when a key is mapped; include the fit table in the ADO overview.
- **Jira ticket comments** only for **Approved** / **Approved with follow-ups**; do not transition Jira status or edit the issue unless the user asks.
- On Jira MCP auth failure: complete the code review, mark requirements **Needs validation** in the overview, and report auth failure in chat — do not claim ticket alignment was verified.

## User Rule (installed)

Global rule file: `~/.cursor/rules/ado-pr-review-jira.mdc` (`alwaysApply: true`). PR-review chats should pick this up without @mentioning the skill.

Optional duplicate under **Cursor → Settings → Rules → User Rules** if you want the same text in the UI:

```text
When I ask to review Azure DevOps (ADO) pull requests, paste a dev.azure.com PR link, or mention Traffix Medallion PRs, follow the skill ado-universal-pr-review: use user-ado MCP; always map Jira from PR description/title when present, fetch the ticket via user-atlassian MCP, and cross-check ticket requirements against the PR diff (and related repo artifacts) before choosing Approval recommendation; include a Jira requirements fit table and tie Why (recommendation) to that fit in every ADO overview; post overview with Approval recommendation, Gold FK/surrogate-key check for gold notebooks (**Fail on missing coalesce(...,-1) on *_key outputs = Not Approved hard gate, including bridge tables**), semantic model guardrails when applicable (**ado-semantic-model-pr-review**; **Fail on placeholder/missing `.platform` `logicalId` e.g. `00000000-…` = Not Approved hard gate**), Critical/high issues; default project Traffix Medallion when not specified; post on the PR unless chat-only; for Approved or Approved with follow-ups also post a short requirements-fit comment on the mapped Jira ticket; do not cast ADO votes unless I ask.
```

## Daily morning automation (Cursor Automations — recommended)

Use a **scheduled Cursor Automation** to batch-review every Active PR each weekday morning without starting a chat manually.

### Setup (one time)

1. Open **[cursor.com/automations](https://cursor.com/automations)** (or **Agents → Automations** in the IDE).
2. **New automation → Blank** (or duplicate an existing ADO review automation if you have one).
3. **Trigger:** Scheduled
   - **Weekdays 8:00am:** cron `0 8 * * 1-5` (Mon–Fri)
   - **Every day 8:00am:** cron `0 8 * * *`
   - Set **timezone** to your team default (e.g. `America/New_York` for US Eastern).
4. **Repository:** **No repository** — this workflow uses ADO/Jira MCP only (no code edits or PRs from the agent).
5. **Tools / MCP:** enable **user-ado** and **user-atlassian** (both must be authenticated in Cursor before the automation runs).
6. **Prompt:** paste the **Daily batch starter prompt** below.
7. **Save → Run now** once to verify MCP auth and posting; then leave enabled.

**Notes**

- Requires a Cursor plan with **Cloud Agents / Automations** (Individual or Teams).
- Automations are configured in the Cursor UI today — not yet driven by a repo file. Keep this skill as the source of truth for the prompt text.
- If a schedule change does not take effect (known cron bug), delete and recreate the automation, or re-save after a small schedule edit.
- Long batches (many Active PRs) may hit automation time limits; the agent should paginate PR lists and skip duplicate overviews per the skill.

### Daily batch starter prompt (paste into the automation)

```text
Follow skill ado-universal-pr-review. Batch-review every Active pull request in Azure DevOps project "Traffix Medallion" (Traffix-Data-Infrastructure). List with repo_list_pull_requests_by_repo_or_project (project only, status Active, paginate). For each PR, run the full review: mandatory Jira requirements cross-check when a ticket key is mapped (getJiraIssue, compare to diff, Jira requirements fit table in ADO overview), Gold FK validation when gold notebooks change, Approval recommendation with Why tied to Jira fit, post ADO threads per the skill; skip duplicate overviews when appropriate. For Approved or Approved with follow-ups, post Jira ticket comments with requirements fit. Finish with a markdown table including Approval recommendation, Jira req check, Gold FK check, Critical/High counts, and Jira comment status.
```

## Tuesday / Thursday starter prompt (manual chat — copy into a new agent chat)

Same workflow as the daily automation; use when you want an ad-hoc batch outside the schedule:

```text
Follow skill ado-universal-pr-review. Batch-review every Active pull request in Azure DevOps project "Traffix Medallion" (Traffix-Data-Infrastructure). List with repo_list_pull_requests_by_repo_or_project (project only, status Active, paginate). For each PR, run the full review: mandatory Jira requirements cross-check when a ticket key is mapped (getJiraIssue, compare to diff, Jira requirements fit table in ADO overview), Gold FK validation when gold notebooks change, Approval recommendation with Why tied to Jira fit, post ADO threads per the skill; skip duplicate overviews when appropriate. For Approved or Approved with follow-ups, post Jira ticket comments with requirements fit. Finish with a markdown table including Approval recommendation, Jira req check, Gold FK check, Critical/High counts, and Jira comment status.
```
