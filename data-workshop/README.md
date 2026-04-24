# Upwork Clickstream — Data Engineering Workshop

A hands-on Databricks workshop covering five canonical data engineering steps end-to-end:
from raw CSV ingestion through a Delta Live Tables medallion pipeline to governance, exploration,
and dashboard-ready analytics.

**Domain:** Upwork job marketplace clickstream — job impressions, click events and job opening
metadata used to analyse search relevance, position bias and job performance.

---

## Architecture

```
GitHub Repo  (data-workshop/data/*.csv + etl notebooks)
        │
        │  git clone + databricks CLI
        ▼
 UC Volume: workspace.de.raw
        │
        │  DLT pipeline  (Steps 1 & 2)
        ▼
┌─────────────────────────────────────────────────────────────────┐
│  Bronze  ──►  Silver  ──►  Gold  ──►  Star Schema               │
│  (raw)       (clean)      (metrics)   (fact + dimensions)       │
└─────────────────────────────────────────────────────────────────┘
        │              │               │
        ▼              ▼               ▼
  Step 2: Quality  Step 3: Gov    Step 4: Genie   Step 5: Dashboard
  Dashboard        RBAC / masks   AI/BI queries   Clickstream Insights
```

### Datasets

| File | Description | Key column |
|------|-------------|------------|
| `impression_events.csv` | A job listing was shown to a visitor in search results or featured jobs | `visitor_id` → freelancers |
| `click_events.csv` | A visitor clicked on a job listing after seeing it | `visitor_id` → freelancers |
| `job_openings.csv` | Job posting metadata — title, category, budget, client, status | `client_uid` → clients |
| `clients.csv` | Client company profiles — country, industry, spend, rating | `client_uid` |
| `freelancers.csv` | Freelancer profiles — skill, rate, job success score, verification | `visitor_id` |

The three event files form a funnel: a search session produces **impressions** for every job shown,
and a subset of those impressions result in **clicks**. The profile files enrich the funnel with
who posted the job (`clients`) and who was searching (`freelancers`).

### Data quality challenges in the raw data

| Dataset | Issue | How silver handles it |
|---------|-------|-----------------------|
| `impression_events` / `click_events` | Null `event_id` | `expect_or_drop` removes the row |
| `click_events` | `time_to_click_secs` contains `"instant"`, `"fast"`, and negatives | `regexp_extract` → null for non-numeric; `expect` warns on negatives |
| `click_events` | Duplicate `event_id` rows | `dropDuplicates(["event_id"])` |
| `click_events` | Future-dated `event_ts` | `expect` warning |
| `click_events` | Null `opening_uid` (organic browse clicks) | Allowed — kept as null |
| `job_openings` | `budget_amount` = `"negotiable"` (text) | `regexp_extract` → null |
| `job_openings` | Negative `budget_amount` | `expect` warning |
| `job_openings` | Null `title` | `expect_or_drop` removes the row |
| `job_openings` | Future `posted_at` | `expect` warning |
| `clients` | `total_spend_usd` = `"negotiable"` or `"not disclosed"` | `regexp_extract` → null |
| `clients` | Null `avg_rating` for new accounts | Kept as null |
| `clients` | Future `member_since` date | `expect` warning |
| `freelancers` | `hourly_rate` = `"negotiable"` | `regexp_extract` → null |
| `freelancers` | `job_success_score` = `"N/A"` for new accounts | `regexp_extract` → null |
| `freelancers` | Negative `job_success_score` | `expect` warning |
| All | Bot traffic (`is_bot = True`) | `expect_or_drop` removes bots in silver |

---

## Getting Started

**1 — Install the Databricks CLI**

| OS | Command |
|----|---------|
| macOS (Homebrew) | `brew tap databricks/tap && brew install databricks` |
| Windows (winget) | `winget install Databricks.DatabricksCLI` |
| Linux | `curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh \| sh` |

> **macOS — Homebrew permission error?** If you see `Error: /opt/homebrew is not writable`, run:
> ```bash
> sudo chown -R $(whoami) /opt/homebrew
> ```
> Then retry the `brew` command above.

Verify: `databricks --version` should print `0.200.0` or later.

**2 — Authenticate**

```bash
databricks auth login --host https://dbc-xxxxxxxx-xxxx.cloud.databricks.com
```

This opens a browser window. Log in with your Databricks credentials — the token is
stored automatically.

**3 — Create the schema and volume**

```bash
databricks schemas create --json '{"name": "de", "catalog_name": "workspace"}'
databricks volumes create --json '{"name": "raw", "catalog_name": "workspace", "schema_name": "de", "volume_type": "MANAGED"}'
```

**4 — Clone the repo and upload the data files**

```bash
git clone https://github.com/lakshmi-rajagopalan/de-with-databricks.git
cd de-with-databricks

# dbfs:/Volumes/ routes to Unity Catalog Volumes (not legacy DBFS)
databricks fs cp --recursive ./data-workshop/data/ \
    dbfs:/Volumes/workspace/de/raw/
```

Add `--overwrite` if files are already there and you want to replace them.

**5 — Upload the notebooks to Databricks**

```bash
databricks workspace import-dir ./data-workshop/etl \
    /Workspace/Shared/clickstream-workshop \
    --overwrite
```

This uploads the entire `etl/` folder into `/Workspace/Shared/clickstream-workshop/`,
accessible to all users in the workspace.

**6 — Create and start the DLT pipeline**

```bash
PIPELINE_ID=$(databricks pipelines create --json '{
  "name": "clickstream-workshop",
  "catalog": "workspace",
  "target": "de",
  "channel": "CURRENT",
  "serverless": true,
  "configuration": {
    "raw_data_path": "/Volumes/workspace/de/raw"
  },
  "libraries": [
    {"notebook": {"path": "/Workspace/Shared/clickstream-workshop/transformations/01_bronze"}},
    {"notebook": {"path": "/Workspace/Shared/clickstream-workshop/transformations/02_silver"}},
    {"notebook": {"path": "/Workspace/Shared/clickstream-workshop/transformations/03_gold"}},
    {"notebook": {"path": "/Workspace/Shared/clickstream-workshop/transformations/04_star_schema"}}
  ]
}' | jq -r '.pipeline_id')

databricks pipelines start-update $PIPELINE_ID
```

Monitor progress in the Databricks UI under **Jobs & Pipelines**.

**7 — Build the dashboards**

Two SQL notebooks were uploaded in step 5:

| Notebook | Dashboard |
|----------|-----------|
| `/Workspace/Shared/clickstream-workshop/04_insights_dashboard` | Search funnel, CTR, position bias, session funnel |
| `/Workspace/Shared/clickstream-workshop/05_quality_dashboard` | DLT expectation pass/fail rates |

Import the dashboards using the CLI:

Data quality dashboard:
```bash
databricks lakeview create \
  --display-name "Data Quality Dashboard" \
  --serialized-dashboard "$(cat data-workshop/etl/dashboards/quality_dashboard.lvdash.json)"
```

Insights dashboard:
```bash
databricks lakeview create \
  --display-name "Clickstream Insights" \
  --serialized-dashboard "$(cat data-workshop/etl/dashboards/insights_dashboard.lvdash.json)"
```

---

## Workshop Modules

The workshop covers five data engineering steps in order:

### Step 1 — Data Modelling

| Notebook | Layer | Key concepts |
|----------|-------|--------------|
| `etl/transformations/01_bronze.py` | Bronze | `@dlt.table`, CSV ingestion as strings, lineage metadata |
| `etl/transformations/02_silver.py` | Silver | Expectations (warn / drop / fail), mixed-type parsing, deduplication; includes `silver_clients` and `silver_freelancers` |
| `etl/transformations/03_gold.py` | Gold | Joins across layers, CTR funnel, position bias, window functions |
| `etl/transformations/04_star_schema.py` | Star Schema | Fact & dimension tables, `dim_client` enriched from clients.csv, `dim_freelancer` from freelancers.csv, event-grain fact |

### Step 2 — Data Quality & Observability

| Notebook | Key concepts |
|----------|--------------|
| `etl/transformations/02_silver.py` | Three expectation levels: `expect` (warn), `expect_or_drop`, `expect_or_fail` |
| `etl/transformations/03_gold.py` | Cross-metric expectations: `clicks <= impressions`, `ctr_pct` range check |
| `etl/05_quality_dashboard.sql` | DLT event log queries, per-expectation pass/fail rates, drop rate trends |

### Step 3 — Data Governance

| Notebook | Key concepts |
|----------|--------------|
| `etl/06_governance.sql` | UC tags, GRANT/REVOKE RBAC, column masking with `CREATE FUNCTION`, row-level security with `SET ROW FILTER`, lineage via `system.access.audit` |

### Step 4 — Data Conversations (Genie)

| Notebook | Key concepts |
|----------|--------------|
| `etl/07_genie_setup.sql` | Grant access to Genie users, verify tables, create a Genie Space, sample natural language questions |

### Step 5 — Data Insights

| Notebook | Dashboard |
|----------|-----------|
| `etl/04_insights_dashboard.sql` | Search funnel KPIs, position bias, category and query analytics, session-level funnel from star schema |

---

## Learning Objectives

- Understand the **medallion architecture** (bronze → silver → gold)
- Write **DLT pipeline notebooks** in Python using `@dlt.table` decorators
- Apply **data quality expectations** at three enforcement levels (`expect`, `expect_or_drop`, `expect_or_fail`)
- Add **cross-metric business-rule expectations** in gold (e.g. clicks ≤ impressions)
- Build a **star schema** (fact + dimension tables) on top of silver for self-service analytics
- Parse **mixed-type columns** (strings in numeric fields) safely in silver
- Build **funnel metrics** (impressions → clicks → CTR) and **position bias** curves in gold
- Apply **Unity Catalog governance**: RBAC grants, column masking, row-level security, system tags
- Set up a **Genie (AI/BI)** space for natural language exploration of your data
- Create a **Databricks dashboard** from SQL queries against Delta tables
