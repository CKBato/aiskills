---
name: traffix-jira-fabric-implement
description: >-
  Implement Traffix Medallion Fabric notebook/pipeline changes from a Jira TIX
  ticket in a pre-provisioned validation workspace (TIX-{key}-{shortDescription})
  connected to an ADO feature branch. Runs analysis, diagnostics, implementation,
  Fabric validation, PR submission, and chained ADO PR review. Supports Standard
  (gate-by-gate) and Fast (GO) modes. Use when the user gives a Jira ticket and
  Fabric workspace, or says implement TIX-#### in Fabric validation workspace.
disable-model-invocation: false
---

# Traffix Jira → Fabric implement (with approvals)

Implement medallion changes the way **TIX-29894** and **TIX-29903** were done: analyze Jira, sync workspace git, diagnose in Fabric, implement minimal notebook diff, validate, submit PR, update Jira.

Read [config.json](config.json) for org/repo defaults. Details: [reference.md](reference.md). Ticket playbooks: [playbooks.md](playbooks.md).

**Restore prior skill version:** [backup/2026-06-05-pre-playbooks-g2-modes/](backup/2026-06-05-pre-playbooks-g2-modes/BACKUP_README.md)

## Preconditions

- **user-atlassian** MCP — Jira read/comment
- **user-ado** MCP — branch, file read, PR (push via REST if needed)
- **user-fabric** MCP — workspace/items/tables discovery
- **Azure CLI** — `az login` for Fabric + ADO REST tokens
- Fabric workspace connected to ADO (**ideally** `feature/TIX-{key}-{shortDescription}`; reused workspaces allowed with G2 warnings)

## Invocation modes

Detect mode from user message. Default: **Standard**.

| Mode | User says | Gate behavior |
|------|-----------|---------------|
| **Standard** | "step by step", "analyze first", no GO keyword | Stop at **every** gate G1–G8 until approved |
| **Fast** | **"GO"**, "go ahead", "approved — run it all", "don't stop between phases" | Run G1 + **G2 sync gate** report, then continue through G8 without stopping unless blocked |

Fast mode still **must** pass G2 sync checks before notebook runs. Fast mode does **not** skip validation, PR/Jira templates, or the chained PR review (unless user says **skip PR review**).

### Voice / text examples

**Standard:**
- `Implement TIX-29903 in workspace TIX-28531-Dimension-Undefined step by step`
- `https://traffixsupport.atlassian.net/browse/TIX-29894` + workspace name

**Fast:**
- `Implement TIX-29903 in workspace TIX-28531-Dimension-Undefined — diagnostic, fix, validate, PR, Jira. GO`
- `TIX-30100 in workspace TIX-30100-my-fix GO`

**Workspace sync explicit:**
- `Before TIX-29903 work, verify workspace TIX-28531-Dimension-Undefined is synced to feature/TIX-29903-...`

## Naming defaults

| Artifact | Pattern |
|----------|---------|
| Fabric workspace | `TIX-{key}-{shortDescription}` |
| ADO branch (PR) | `feature/TIX-{key}-{shortDescription}` |
| Validate notebook | `TIX-{key}_{topic}_validate` (before + after sections; see playbooks.md) |
| Diagnostic notebook | `TIX-{key}_{topic}_diagnostic` (optional; clone lakehouse deps from existing notebook) |

`shortDescription` = 2–4 kebab-case words from Jira summary.

## Mandatory approval gates

| Gate | Deliverable | Standard | Fast |
|------|-------------|----------|------|
| **G1 — Analysis** | Problem, playbook, lineage, root cause, files, risks | Stop | Report then continue |
| **G2 — Workspace sync** | Branch, head vs ADO, sync recommendation | Stop | **Must pass** before runs |
| **G3 — Diagnostics** | Baseline counts / validate-before section | Stop | Continue |
| **G4 — Implementation** | Minimal diff intent | Stop | Continue |
| **G5 — Validation** | Run notebook(s), assertions, distribution | Stop | Continue |
| **G6 — PR** | Draft with validation tables; create PR | Stop | Continue (user already said GO) |
| **G8 — PR review** | Chained **ado-universal-pr-review** on new PR | Stop before posting ADO threads | Run full review + post overview |
| **G7 — Jira** | Comment with PR link + validation + review rec | Stop | Continue unless user declines Jira |

## Workflow

### Phase 0 — Intake

1. Parse Jira key (`TIX-####`), workspace name, mode (Standard / Fast).
2. If workspace omitted, derive `TIX-{key}-{shortDescription}` from summary.
3. Load ticket via **user-atlassian** `getJiraIssue`. Check linked issues, sample SQL in description.
4. **→ G1**

### Phase 1 — Analysis (G1)

Produce:

- **Playbook** (A dimension / B silver datamart / C gold) — see [playbooks.md](playbooks.md)
- **Problem** / intended outcome
- **Data lineage** (bronze → silver → gold → report)
- **Root-cause buckets**
- **Canonical pattern** notebook (search ADO for same column/join elsewhere)
- **Files in scope** — prefer single notebook section
- **Out of scope**

**Standard → STOP.** Fast → brief report, continue.

### Phase 2 — Workspace sync gate (G2) — REQUIRED

**Do not run implementation or validation notebooks until G2 is resolved or user explicitly accepts mismatch.**

1. Run `node scripts/verify-workspace-git.js "<workspace>" "<expectedBranch>"`
2. Run `node scripts/compare-workspace-head-to-ado.js "<workspace>" "<expectedBranch>"`
   - `expectedBranch` = `feature/TIX-{key}-{shortDescription}` unless user names another (e.g. `main`)
3. Report block:

| Check | Pass criteria |
|-------|---------------|
| Git state | `ConnectedAndInitialized` |
| Branch | Matches expected **or** user accepted reuse + documented mismatch |
| Head | `workspaceHead === adoHead` (exit 0 from compare script) |
| Last sync | `lastSyncTime` noted; if head mismatch → instruct **Update from Git** in Fabric |

4. If exit **4** (branch mismatch): warn — PR branch will be **new from main**; workspace fix may use `updateDefinition`.
5. If exit **5** (head mismatch): **STOP** until user syncs workspace (unless Fast + user already said sync is done).

**Standard → STOP after report if any fail.** Fast → proceed only when synced or user confirms.

### Phase 3 — Diagnostics (G3)

1. Read `main` notebook from ADO for affected path.
2. Deploy/run **`TIX-{key}_{topic}_validate`** *before section* OR prod SQL cohort counts.
3. Summarize bad cohort size; pick fix approach per playbook.

**Standard → STOP.** Fast → continue.

### Phase 4 — Implementation (G4)

- **Minimal diff** — one notebook section; match conventions ([playbooks.md](playbooks.md) checklist)
- Qualify columns after joins
- Workspace: `updateDefinition` on existing git notebook OR push to feature branch
- If workspace branch ≠ PR branch: push same file to **`feature/TIX-{key}-...` from `main`** for PR

**Standard → STOP.** Fast → continue.

### Phase 5 — Fabric validation (G5)

1. Run target batch notebook (full reload if historical fix — playbook B).
2. Run **`TIX-{key}_{topic}_validate`** (after section): assertions, Jira sample, distribution.
3. Capture numbers for PR/Jira (prod baseline vs post-fix).

**Standard → STOP.** Fast → continue.

### Phase 6 — PR submission (G6)

Repo: **Traffix Medallion Production** → **`main`**.

1. Branch `feature/TIX-{key}-{shortDescription}` from `main` if needed.
2. Push via `scripts/ado-push-notebook-curl.ps1` or curl (see reference.md).
3. Title: `TIX-{key}: {Jira summary}`
4. Body: Summary, Context, Changes, **Fabric validation tables**, Test plan, Out of scope.
5. `repo_create_pull_request` → capture **`pullRequestId`** and PR URL.

**Standard → show draft first.** Fast → create if GO already given.

**→ G8 immediately** unless user said **skip PR review**.

### Phase 7 — Chained ADO PR review (G8)

**Mandatory after G6** (PR exists). Read and follow **[ado-universal-pr-review](../ado-universal-pr-review/SKILL.md)** end-to-end for the PR just created.

**Inputs to pass into the review skill:**

| Field | Value |
|-------|-------|
| `project` | `Traffix Medallion` |
| `repositoryId` | `Traffix Medallion Production` (or GUID from G6) |
| `pullRequestId` | From `repo_create_pull_request` |
| Jira key | `TIX-{key}` from intake (cross-check vs PR title/description) |

**Routing:**

| PR diff contains | Also read |
|------------------|-----------|
| `dl_*` / `.tmdl` / `.bim` / `SemanticModel/**` | **[ado-semantic-model-pr-review](../ado-semantic-model-pr-review/SKILL.md)** |
| `gold_*` notebooks only | Gold FK / `-1` default check (in universal skill) |
| Silver/datamart notebooks only (typical playbook B) | Notebook checklist only |

**Implement-skill extras for G8** (layer on universal review):

- Confirm PR **Fabric validation** section matches G5 numbers (bad cohort, assertions, distribution).
- Confirm **post-merge full reload** is in PR test plan when playbook B fixed historical rows.
- Confirm **known limitations** from G5 are documented (e.g. multi-rep `CAD,USD`).
- **Jira requirements fit** must reference ticket acceptance criteria from G1, not only the diff.

**Posting behavior:**

| Mode | G8 behavior |
|------|-------------|
| **Standard** | Run analysis + draft recommendation + Jira fit table → **STOP** before `repo_create_pull_request_thread` unless user approves posting |
| **Fast** | Full single-PR workflow: post ADO overview (+ line threads for Critical/High); Jira review comment per universal skill if **Approved** / **Approved with follow-ups** |

**Duplicate handling:** If overview thread already exists for this iteration (`## PR review — Cursor (non-blocking)`), skip re-post per universal skill.

**Chat summary (required):** PR link, **Approval recommendation**, Jira req check, Critical/High counts, whether ADO/Jira threads were posted.

**Do not** call `repo_vote_pull_request` unless user explicitly asks.

**→ G7** after G8 (or after G8 draft approval in Standard).

### Phase 8 — Jira comment (G7)

`addCommentToJiraIssue` — PR link, fix summary, **same validation tables as PR**, **G8 approval recommendation** (if review ran), post-merge steps, limitations.

If G8 already posted the universal-skill **PR review** Jira comment, G7 is the **implementation** comment only — do not duplicate the review template; link PR and note review recommendation in one line.

**Standard → draft first.** Fast → post unless user declines Jira updates.

## Guardrails

- Do **not** merge PR or deploy to prod unless explicitly asked.
- Do **not** skip G8 unless user says **skip PR review** — chaining is the default after G6.
- Do **not** run notebooks when G2 head mismatch unresolved (unless user overrides in writing).
- Seed `-1` only for dimension FK gaps, not attribute classification on real users.
- If workspace reused on wrong branch, **document two-branch workflow** in PR.
- Prefer **one validate notebook** over API-created diagnostic without lakehouse deps.

## MCP / tool priority

| Task | Tool |
|------|------|
| Jira | user-atlassian |
| Read code, branch, PR | user-ado |
| Workspace/items | user-fabric |
| Notebook run / git | Fabric REST + `az` |
| ADO push | `scripts/ado-push-notebook-curl.ps1` |

## Scripts

| Script | Purpose |
|--------|---------|
| [scripts/verify-workspace-git.js](scripts/verify-workspace-git.js) | Workspace exists + branch name |
| [scripts/compare-workspace-head-to-ado.js](scripts/compare-workspace-head-to-ado.js) | **G2 sync gate** — head vs ADO tip |
| [scripts/ado-push-notebook-curl.ps1](scripts/ado-push-notebook-curl.ps1) | Push notebook-content.py to ADO |

## Related skills (chained)

| After gate | Skill | When |
|------------|-------|------|
| **G6 → G8** | **ado-universal-pr-review** | **Always** (default) — single-PR mode on new PR |
| G8 (if semantic model files) | **ado-semantic-model-pr-review** | `dl_*`, TMDL, BIM, PBIP in diff |
| **After merge + prod reload** | **postpr-validation** | Hours/next day — read-only prod final check + Jira comment; yes/no prompt before Done + worklog |

## Post-merge (user / ops)

1. Merge PR, prod full reload per playbook.
2. Later: invoke **postpr-validation** with Jira key only — see `~/.cursor/skills/postpr-validation/SKILL.md`.
3. On **PASS**: agent posts final-check comment and prompts **yes/no** to move to Done and log hours; user confirms **yes** (Step 7–8) or closes Jira manually.
