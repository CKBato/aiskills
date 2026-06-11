---
name: ado-semantic-model-pr-review
description: >-
  Azure DevOps PR review guardrails for Traffix Medallion Power BI / Fabric
  semantic models (TMDL, BIM, PBIP). Use when a PR changes dl_* SemanticModel
  artifacts, or when ado-universal-pr-review classifies a PR as semantic model
  or mixed with semantic model files. Produces structured approval conditions,
  guardrail checklist results, and ADO overview threads via user-ado MCP.
disable-model-invocation: false
---

# ADO semantic model PR review

Use with **ado-universal-pr-review** for posting, severity labels, ADO tooling, and **mandatory Jira requirements cross-check** (map ticket from PR, `getJiraIssue`, requirements fit table in overview). This skill adds **semantic-model-specific approval conditions** and a **guardrail checklist**.

## When to apply

Apply when the PR diff includes any of:

- `*.SemanticModel/**`, `*.tmdl`, `*.bim`, PBIP `definition/**`
- Traffix `dl_*` models in Fabric workspace git artifacts

If the PR is **mixed** (semantic model + notebooks/gold), run this checklist **and** the gold notebook FK check from ado-universal-pr-review.

## Approval conditions (semantic model)

| Recommendation | Merge when… |
|----------------|-------------|
| **Approved** | All guardrail categories **Pass** or acceptable **N/A**; no Critical/High findings; PR description matches diff; **Jira requirements fit Pass** (when mapped); source bindings and relationships are valid at intended grain. |
| **Approved with follow-ups** | No Critical/High; Jira gaps documented as follow-ups; one or more **Needs validation** items that require post-merge Fabric refresh / visual smoke-test; or medium gaps acknowledged. |
| **Not Approved** | Any **Critical** or **High** guardrail failure below. |
| **Needs author response** | Missing model name, Jira, or intent; gold column dependency unclear; diff truncated; author must confirm source/RLS behavior. |
| **Defer** | Draft, wrong model/workspace, superseded PR, or Fabric publish blocked. |

**Not Approved** requires a concrete file/column/relationship/DAX cite — not stylistic preference.

## Guardrail checklist (run every semantic model PR)

Record each category as **Pass**, **Fail**, **Needs validation**, or **N/A**. **Fail** maps to Critical/High per severity table.

### 1. Traceability and scope

| Check | Fail severity |
|-------|---------------|
| PR names **model** (`dl_*`) and **intent** (relationships, measures, RLS, formatting, parameters) | **High** if title/diff mismatch |
| Jira mapped and requirements cross-checked (parent skill) | Medium if no key; **High** if ticket deliverable missing from PR |
| Changed files belong to expected model (not accidental multi-model mix) | **High** |

### 2. Source binding

| Check | Fail severity |
|-------|---------------|
| New/changed `sourceColumn` exists in lakehouse/gold source | **Critical** |
| New table partition entity / connection mode change documented | **High** |
| PR depends on gold columns not yet merged | **Needs author response** |
| **`expressions.tmdl` `DatabaseQuery` uses Traffix prod Fabric SQL endpoint** (see below) | **Critical** if wrong/changed endpoint; verify if file in diff |
| Import/DirectQuery table partitions use `expressionSource: DatabaseQuery` | **High** if new SQL table omits shared expression |

### 2a. Required Fabric connection — `definition/expressions.tmdl`

Traffix Medallion Production `dl_*` semantic models must use the shared **`DatabaseQuery`** expression pointing at the **Traffix-Medallion-Production** Fabric warehouse (gold lakehouse SQL endpoint).

**File:** `{model}.SemanticModel/definition/expressions.tmdl` (TMDL name is **`expressions.tmdl`**, not `expression.tmdl`).

**Required expression** (whitespace/tab may vary; server + database GUID must match exactly):

```tmdl
expression DatabaseQuery =
		let
			 database = Sql.Database("c37dtetbzmlulmsovead5czqyi-j7bdulap2bhupfy2mnyu3zscna.datawarehouse.fabric.microsoft.com", "b75886a9-bb96-480c-b029-9452c26eaeec")
		in
		    database
```

**Required connection constants**

| Setting | Required value |
|---------|----------------|
| Expression name | `DatabaseQuery` |
| Fabric SQL host | `c37dtetbzmlulmsovead5czqyi-j7bdulap2bhupfy2mnyu3zscna.datawarehouse.fabric.microsoft.com` |
| Database / warehouse id | `b75886a9-bb96-480c-b029-9452c26eaeec` |
| M body shape | `let` → `Sql.Database(...)` assigned to `database` → `in database` |

**Review checks**

1. If **`expressions.tmdl` is in the PR diff** (added or modified): confirm the `Sql.Database` host and database GUID match the table above. **Fail = Critical** if either differs, or if `DatabaseQuery` is renamed/removed without team approval.
2. If **`expressions.tmdl` is not in the diff**: read it from the PR branch when possible (ADO search or repo file). **Pass** if unchanged and already compliant; **Needs validation** if file could not be loaded.
3. For **new or changed SQL-backed table partitions** in `tables/*.tmdl`: confirm `expressionSource: DatabaseQuery` (not a one-off `Sql.Database` embedded only in that table unless explicitly approved).
4. **`model.tmdl`** should list `DatabaseQuery` in `PBI_QueryOrder` when the model uses warehouse-backed tables.

**Approval impact**

| Finding | Severity | Recommendation impact |
|---------|----------|------------------------|
| Wrong Fabric host or warehouse GUID in `expressions.tmdl` | **Critical** | **Not Approved** |
| New SQL partition bypasses `DatabaseQuery` with different endpoint | **Critical** | **Not Approved** |
| `expressions.tmdl` not reviewed (diff truncated, file missing) | — | **Needs author response** |
| `expressions.tmdl` unchanged and compliant; not in diff | — | **Pass** (note N/A in guardrail table) |
| Calculated/local tables only; no warehouse partitions touched | — | **N/A** for connection check |

### 2b. Git integration — `{model}.SemanticModel/.platform` (`logicalId`)

Fabric Git sync maps workspace items to repo folders via **`.platform`**. A placeholder or missing **`config.logicalId`** causes prod sync failures such as **“Missing or corrupted system files”** on the semantic model directory (see PR 1392 / `dl_customer_sales_commission`).

**File:** `{model}.SemanticModel/.platform`

**Required shape** (schema version may vary; `logicalId` must be a real GUID):

```json
{
  "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
  "metadata": {
    "type": "SemanticModel",
    "displayName": "dl_example"
  },
  "config": {
    "version": "2.0",
    "logicalId": "deb87cdb-1c08-a61a-4384-16ae5765e403"
  }
}
```

**Invalid / blocked values**

| `logicalId` value | Result |
|-------------------|--------|
| `00000000-0000-0000-0000-000000000000` | **Fail — Critical** (placeholder; Fabric rejects as corrupted) |
| Missing, empty, or not a GUID | **Fail — Critical** |
| `.platform` missing for a **new** `*.SemanticModel` folder in the PR | **Fail — Critical** |

**Review checks**

1. If the PR **adds or modifies** any `*.SemanticModel` folder, **always** read `.platform` from the PR branch (diff or `repo_get_file_content` at source commit).
2. Confirm `metadata.type` is `SemanticModel` and `metadata.displayName` matches the folder/model name.
3. Confirm `config.logicalId` is a **non-zero, valid GUID**. Compare against other Traffix `dl_*` models in the repo — none use all-zero IDs.
4. **New model in prod:** `logicalId` must come from (a) Fabric **Commit to Git** on the target workspace item, or (b) a newly generated GUID that Fabric will bind on first **Update from Git** — never ship the placeholder.
5. **Existing model:** `logicalId` must **not** change unless the PR explicitly documents a workspace item rebind/migration.

**Approval impact**

| Finding | Severity | Recommendation impact |
|---------|----------|------------------------|
| Placeholder `00000000-0000-0000-0000-000000000000` | **Critical** | **Not Approved** — hard gate |
| Missing / malformed `.platform` or `logicalId` on new semantic model | **Critical** | **Not Approved** |
| `.platform` not reviewed (file not in diff, branch unreadable) | — | **Needs author response** |
| Valid non-zero `logicalId`; compliant with sibling models | — | **Pass** |
| `.platform` unchanged; not in diff | — | **Pass** (note in guardrail table) |

**Author fix (when Fail):** Export `.platform` from Fabric (**Commit to Git** on the workspace item) or replace placeholder with the workspace-assigned `logicalId`, then re-push.

### 3. Relationships and grain

| Check | Fail severity |
|-------|---------------|
| New/changed relationship keys match fact grain | **Critical** |
| Unintentional **many-to-many** or **bidirectional** filter on large tables | **High** |
| Role-playing date uses `*_date_key` → `dim_date.date_key` (Traffix pattern) | **High** if wrong column type used |
| Inactive relationship without `USERELATIONSHIP` in affected measures | **High** |
| Unknown `-1` FK behavior on new date/FK relationships | **Needs validation** (post-merge) |

### 4. DAX and measures

| Check | Fail severity |
|-------|---------------|
| New/changed measure references missing columns/tables | **Critical** |
| Measure changes filter context / totals without duplicate-safe pattern | **High** |
| Display folder / rename only | N/A |

### 5. RLS / OLS / security

Distinguish **RLS filter rules** from **RLS display label measures**.

| Check | Fail severity |
|-------|---------------|
| New/changed **RLS filter** DAX without security review | **Critical** |
| RLS references wrong bridge/table | **Critical** |
| Hardcoded user/email in RLS | **Critical** |
| Label measures only (e.g. IF/MAX on `role_display`) | Pass if documented as non-security |

### 6. Formatting, parameters, UX

| Check | Fail severity |
|-------|---------------|
| `formatString` / display name changes | Pass (note in observations) |
| Field parameter / dynamic axis table | **Needs validation** — smoke-test sort and field names |
| Column unhidden — confirm no PII exposure | **High** if sensitive |

### 7. Performance and refresh

| Check | Fail severity |
|-------|---------------|
| Import mode / storage mode change on large table | **High** |
| DirectLake → Import without approval | **Critical** |
| Heavy calculated table in refresh path | **Needs validation** |

### 8. Deployment hygiene

| Check | Fail severity |
|-------|---------------|
| Secrets, local paths, or personal credentials in TMDL | **Critical** |
| Post-merge refresh path understood (Traffix prod `dl_*`) | Note in test plan |

## Required PR description fields (ask if missing)

1. **Model:** e.g. `dl_logistics_active_loads`
2. **Intent:** relationships / measures / RLS / formatting / parameters
3. **Tables touched**
4. **Report impact:** pages/visuals affected
5. **Validation done:** refresh, screenshots, measure checks
6. **Jira** link
7. **Gold dependency:** new gold columns? link gold PR if yes

Missing 1–3 → **Needs author response** unless inferable from diff.

## Traffix `dl_*` defaults

- **Warehouse connection:** all production `dl_*` models share `DatabaseQuery` in `definition/expressions.tmdl` → Fabric SQL host `c37dtetbzmlulmsovead5czqyi-j7bdulap2bhupfy2mnyu3zscna.datawarehouse.fabric.microsoft.com`, database `b75886a9-bb96-480c-b029-9452c26eaeec`. SQL table partitions use `expressionSource: DatabaseQuery`.
- Role-playing dates: fact `*_date_key` → date dimension `date_key`, not raw datetime, when gold provides keys.
- Unknown member `-1` on FK/date keys: post-merge visual check (blank vs error).
- RLS enforcement uses bridges like `rls_user_load_access`; **label measures on User are not RLS rules**.
- If model references new gold columns, **gold must land before semantic model refresh**.
- **Git integration:** every `*.SemanticModel` must have a valid `.platform` with non-zero `logicalId` — placeholder IDs block prod sync.

## Review workflow

1. Classify files as semantic model (this skill) ± gold/notebooks (parent skill).
2. `repo_get_pull_request_by_id` + `repo_get_pull_request_changes`.
3. Scan diffs in order: **`.platform`** (`logicalId` / Git integration) → **`definition/expressions.tmdl`** (DatabaseQuery connection) → `relationships.tmdl` → new/changed tables → measures → RLS sections → partitions.
4. Run **guardrail checklist**; map **Fail** to Critical/High in overview.
5. Choose recommendation from **Approval conditions**.
6. `repo_list_pull_request_threads` — skip duplicate if same iteration and same recommendation + guardrail summary; else post **new** overview.
7. Optional: **user-powerbi-modeling-mcp** when model is local — never required.

## Overview thread template (semantic model PRs)

Use with parent skill title line `## PR review — Cursor (non-blocking) — PR _N_`.

Add after **Change type:** Semantic model:

```markdown
**Semantic model guardrails** _(skill: ado-semantic-model-pr-review)_
| Category | Result | Notes |
|----------|--------|-------|
| Traceability & scope | Pass / Fail / Needs validation / N/A | … |
| Git integration (`.platform`) | Pass / Fail / Needs validation / N/A | `logicalId` valid GUID; not `00000000-…` placeholder |
| Fabric connection (`expressions.tmdl`) | Pass / Fail / Needs validation / N/A | `DatabaseQuery` → prod Fabric SQL host + `b75886a9-…` warehouse id |
| Source binding | … | … |
| Relationships & grain | … | … |
| DAX & measures | … | … |
| RLS / OLS / security | … | … |
| Formatting & parameters | … | … |
| Performance & refresh | … | … |
| Deployment hygiene | … | … |

**Guardrail summary:** _All passed_ OR _N Fail → see Critical/high issues_
```

Keep **Gold FK / surrogate key check** from parent skill (N/A for semantic-only PRs).

## Example approval reasons

**Approved:** All guardrails Pass; relationship is single-direction role-playing date; RLS changes are display labels only; source columns present on fact table.

**Approved with follow-ups:** Guardrails Pass except **Needs validation** on `-1` date keys and Graph Parameter UX — merge OK with post-deploy checklist.

**Not Approved:** New bidirectional relationship on fact bridge inflates totals — fix filter direction or document measure isolation before merge.

**Not Approved:** `expressions.tmdl` points `DatabaseQuery` at a non-production Fabric warehouse — restore standard Sql.Database host and `b75886a9-bb96-480c-b029-9452c26eaeec` database id before merge.

**Not Approved:** `.platform` has placeholder `logicalId` `00000000-0000-0000-0000-000000000000` — Fabric Git sync will fail with missing/corrupted system files; export `.platform` from Fabric or assign a valid GUID before merge.

**Needs author response:** PR adds Customer column but partition unchanged — confirm gold column deployed and refresh order.

## Fine-tuning

Team should revise severity mappings and Traffix defaults after each semantic model PR retro. Update this skill file; do not duplicate long checklists in ado-universal-pr-review.
