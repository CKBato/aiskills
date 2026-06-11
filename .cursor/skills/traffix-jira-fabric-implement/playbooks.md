# Traffix Jira → Fabric playbooks

Classify the ticket in **G1** and follow the matching playbook. Reference tickets: **TIX-29894**, **TIX-29903**.

## How to pick a playbook

| Signal in Jira / analysis | Playbook |
|---------------------------|----------|
| `dim_*`, seed `-1`, NULL in reports, `gold_dimension_*` | **A — Dimension** |
| `mrt_*`, silver datamart, column null/empty on fact table, `silver_*_datamart_*` | **B — Silver datamart** |
| Gold fact / bridge only (no new dimension seed) | **C — Gold fact** (extend as needed) |

When unsure, default to **B** if the failing table is in `LH_Silver`.

### After PR submit (all playbooks — skill G8 → G7)

1. **G8** — chain **ado-universal-pr-review** on the new PR (semantic-model PRs also use **ado-semantic-model-pr-review**)
2. **G7** — Jira implementation comment; one-line G8 recommendation; skip duplicate review template if G8 posted it

Opt out only if user says **skip PR review**.

---

## Playbook A — Dimension / seed / attribute (TIX-29894, TIX-28531)

### Typical problem
- Missing seed row (`-1`) or invalid attribute (`undefined`, `Unclassified`) on dimension FK joins
- Reports show NULL where a conformed value is expected

### Lineage
`bronze` → `silver` → **`gold_dimension_v2_batch_01`** / `gold_dimensions_batch_01` → semantic model

### Notebooks in scope (prefer one)
- `gold_dimension_v2_batch_01`
- Related bridge if ticket requires: `gold_bridge_tables_batch_01`

### G1 analysis extras
- Which `dim_*` table and surrogate key?
- Is this **seed `-1`** vs **attribute classification** on real rows? (seed only for FK gaps)
- Search ADO for existing seed patterns in same dimension notebook

### Validation (`TIX-{key}_{topic}_validate`)
- [ ] Prod baseline: count rows with bad cohort (e.g. `employee_type = 'undefined'`)
- [ ] Post-fix: cohort count → 0 (or documented exception list)
- [ ] `dim_*` seed `-1` exists if applicable
- [ ] Sample user/load from Jira attachment or SQL

### Post-merge
- [ ] Run gold dimension batch activity in orchestration
- [ ] **postpr-validation** skill — read-only prod final check + Jira comment (yes/no prompt → Done + worklog on confirm)
- [ ] Refresh affected `dl_*` semantic models if ticket touches report fields
- [ ] Analyst sign-off on report visual

### Out of scope (unless ticket says otherwise)
- HR / Entra source-data fixes
- Widening production filters without stakeholder note

---

## Playbook B — Silver datamart / mrt column (TIX-29903)

### Typical problem
- Column null/empty on `LH_Silver.*.mrt_*` despite related IDs populated (e.g. `sales_rep_id` set, `home_currency` blank)
- Large historical cohort (~100k+ rows)

### Lineage
`bronze` (3G / NetSuite) → `silver_*_hub_*` → **`silver_*_hub_datamart_batch_*`** → `mrt_*` → gold facts / reports

### Notebooks in scope (prefer one section)
- `silver_3gtms_hub_datamart_batch_01` (and peers per domain)

### G1 analysis extras
- Search ADO for **same column** in gold notebooks (canonical join pattern, e.g. `gold_customer_sales_fact`)
- Row-level vs **aggregated** logic (`groupBy` + `concat_ws` comma lists)
- **`coalesce('', 'USD')` trap** — empty string is not null
- **1 row per `load_id`** — multi-rep = comma-delimited fields, not duplicate rows
- Dual join keys: email vs `employee_3g_un` / 3G username

### Validation (`TIX-{key}_{topic}_validate`)
Use **one** validate notebook (sections: before / after). Prefer over standalone `*_diagnostic` unless deep join trace needed.

- [ ] Prod baseline: count bad cohort (read-only `production_lakehouse_*`)
- [ ] Post-fix assertion: bad cohort with key populated = **0**
- [ ] Jira sample row (load number / SQL from ticket)
- [ ] Distribution table for fixed column
- [ ] Document **known limitation** (e.g. multi-rep `CAD,USD` + `when()` USD fallback)

### Fabric run
- [ ] Full reload target `mrt_*` in validation LH (incremental won't fix historical blanks)
- [ ] Orchestration activity: e.g. `silver_3gtms_hub_datamart`

### Post-merge
- [ ] **Full reload** production `mrt_*` table (not incremental-only)
- [ ] **postpr-validation** skill — read-only prod final check + Jira comment (yes/no prompt → Done + worklog on confirm) (Jira key only)
- [ ] Notify linked report tickets (e.g. Lane History)
- [ ] Gold downstream only if ticket requires propagation check

### Silver datamart checklist (implementation)
- [ ] `coalesce` handles null **and** `trim(col) = ''`
- [ ] Don't pre-filter dimension rows needed for join (e.g. NULL `home_currency`)
- [ ] Row-level `coalesce(currency, 'USD')` **before** `groupBy`
- [ ] Qualify columns after new joins

### PR / workspace branch note
If workspace git branch ≠ `feature/TIX-{key}-...`: fix via workspace `updateDefinition`, then push same file to **new branch from `main`** for PR.

---

## Playbook C — Gold fact (stub)

Extend when a third ticket type is completed end-to-end.

### Typical problem
- Fact measure or FK wrong at gold layer

### Notebooks
- `gold_*_batch_*`, `gold_*_fact`

### Validation
- Prod vs validation fact counts, sample keys from Jira

### Post-merge
- Targeted gold batch + semantic refresh per ticket
- **postpr-validation** skill — read-only prod final check + Jira comment (yes/no prompt → Done + worklog on confirm)

---

## Cross-playbook validation outputs (required in PR + Jira)

| Output | Required |
|--------|----------|
| Workspace name | Yes |
| Notebooks run + pass/fail | Yes |
| Prod baseline count (bad cohort) | Yes |
| Post-fix assertion | Yes |
| Sample from Jira | Yes |
| Distribution / top values | Yes when categorical fix |
| Known limitations | Yes when applicable |
