# Upwork Clickstream - Data Engineering Workshop

A hands-on Databricks workshop covering five canonical data engineering steps end-to-end:
from raw CSV ingestion through a Delta Live Tables medallion pipeline to governance, exploration,
and dashboard-ready analytics.

Domain: Upwork job marketplace clickstream - job impressions, click events and job opening
metadata used to analyze search relevance, position bias and job performance.

---

## Architecture

GitHub repo (`data/*.csv` + `src/` assets)
-> Unity Catalog volume (workspace.de.raw)
-> DLT pipeline (bronze -> silver -> gold -> star schema)
-> Governance + Genie + Dashboards

### Datasets

| File | Description | Key column |
|------|-------------|------------|
| impression_events.csv | A job listing was shown to a visitor in search results or featured jobs | visitor_id -> freelancers |
| click_events.csv | A visitor clicked on a job listing after seeing it | visitor_id -> freelancers |
| job_openings.csv | Job posting metadata - title, category, budget, client, status | client_uid -> clients |
| clients.csv | Client company profiles - country, industry, spend, rating | client_uid |
| freelancers.csv | Freelancer profiles - skill, rate, job success score, verification | visitor_id |

The three event files form a funnel: a search session produces impressions for every job shown,
and a subset of those impressions results in clicks. The profile files enrich the funnel with
who posted the job (clients) and who was searching (freelancers).

### Data quality challenges in the raw data

Moved to [docs/DATA_QUALITY_CHALLENGES.md](docs/DATA_QUALITY_CHALLENGES.md).

### Repo layout

| Path | Purpose |
|------|---------|
| `data/` | Raw CSV inputs used for the workshop |
| `src/dlt/` | DLT Python pipeline assets |
| `src/sql/` | SQL notebooks for governance, metric views, and Genie setup |
| `src/dashboards/` | Lakeview dashboard JSON assets |
| `src/genie/` | Tracked Genie template and optional exported payload |
| `resources/` | Databricks bundle resource definitions |
| `scripts/` | Local helper scripts for setup and rendering |

---

## Getting Started (Asset Bundles Workflow)

This README uses a bundle-first workflow for deployment. No direct CLI `pipelines create`
or `lakeview create` commands are required.

**1 - Clone the repo**

```bash
git clone https://github.com/lakshmi-rajagopalan/de-with-databricks.git
cd de-with-databricks
```

**2 - Install Databricks CLI**

| OS | Command |
|----|---------|
| macOS (Homebrew) | `brew tap databricks/tap && brew install databricks` |
| Windows (winget) | `winget install Databricks.DatabricksCLI` |
| Linux | `curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh \| sh` |

Verify:

```bash
databricks --version
```

**3 - Authenticate**

```bash
databricks auth login --host https://dbc-xxxxxxxx-xxxx.cloud.databricks.com
```

**4 - One-time Unity Catalog setup (catalog/schema/volume)**

Set a real SQL warehouse ID:

```bash
export WAREHOUSE_ID=$(databricks warehouses list -o json | jq -r '.[] | select(.name=="Serverless Starter Warehouse") | .id' | head -n1)
echo "$WAREHOUSE_ID"
```

Run the setup script once:

```bash
bash ./scripts/setup_unity_catalog.sh workspace de raw "$WAREHOUSE_ID"
```

**5 - Upload raw data files to volume**

```bash
databricks fs cp --recursive ./data/ \
  dbfs:/Volumes/workspace/de/raw/ --overwrite
```

**6 - Validate and deploy bundle resources**

Validate and deploy bundle resources (pipeline + dashboards + jobs):

```bash
databricks bundle validate --target dev --var="warehouse_id=$WAREHOUSE_ID"
databricks bundle deploy --target dev --var="warehouse_id=$WAREHOUSE_ID"
databricks bundle summary --target dev --var="warehouse_id=$WAREHOUSE_ID"
```

Run the deployed DLT pipeline:

```bash
databricks bundle run --target dev --var="warehouse_id=$WAREHOUSE_ID" clickstream_workshop
```

After the first successful pipeline run, render the quality dashboard with the pipeline UUID and redeploy once:

```bash
export PIPELINE_ID=$(databricks pipelines list-pipelines -o json | jq -r '.[] | select(.name | test("clickstream-workshop")) | .pipeline_id' | head -n1)
./scripts/render_quality_dashboard.sh "$PIPELINE_ID"
databricks bundle deploy --target dev --var="warehouse_id=$WAREHOUSE_ID"
```

**7 - Create metric views**

Create reusable metric views through a bundle job resource:

```bash
databricks bundle run --target dev --var="warehouse_id=$WAREHOUSE_ID" metric_views
```

**8 - Create Genie Space (bundle auth context)**

Run the Genie setup with the tracked template payload:

```bash
databricks bundle run --target dev --var="warehouse_id=$WAREHOUSE_ID" -- ./scripts/create_genie_space.sh "Clickstream Analytics" "$WAREHOUSE_ID"
```

The Genie script is idempotent: if the same space already exists, it exits without creating a duplicate.

**9 - Create Databricks App (optional)**

Bundle deploy creates the Databricks App resource, but you still need to deploy the app source code.

Export the published dashboard URLs from the deployed bundle state:

```bash
export CLIENT_DASHBOARD_URL=$(databricks bundle summary --target dev --var="warehouse_id=$WAREHOUSE_ID" -o json | jq -r '.resources.dashboards.client_demand_intelligence.url')
export INSIGHTS_DASHBOARD_URL=$(databricks bundle summary --target dev --var="warehouse_id=$WAREHOUSE_ID" -o json | jq -r '.resources.dashboards.clickstream_insights.url')
export WORKSPACE_HOST=$(printf '%s\n' "$CLIENT_DASHBOARD_URL" | sed -E 's#(https?://[^/]+)/.*#\1#')
export GENIE_SPACE_ID=$(databricks genie list-spaces -o json | jq -r '.spaces[] | select(.title=="Clickstream Analytics") | .space_id' | head -n1)
export GENIE_SPACE_URL="$WORKSPACE_HOST/genie/rooms/$GENIE_SPACE_ID"
```

Run the app resource once:

```bash
databricks bundle run --target dev \
  --var="warehouse_id=$WAREHOUSE_ID" \
  --var="client_dashboard_url=$CLIENT_DASHBOARD_URL" \
  --var="insights_dashboard_url=$INSIGHTS_DASHBOARD_URL" \
  --var="genie_space_url=$GENIE_SPACE_URL" \
  client_demand_shell
```

After deploy, open Databricks UI -> Apps and launch `clickstream-client-demand-app`.

The app shell is designed to sit on top of:

1. `[dev <user>] Client Demand Intelligence`
2. `[dev <user>] Clickstream Insights`
3. your Genie space

In dev mode, bundle resources are prefixed (example: `[dev <user>] Data Quality Dashboard`).
Use bundle summary to get direct URLs.

Optional deploy overrides:

```bash
databricks bundle deploy --target dev \
  --var="warehouse_id=$WAREHOUSE_ID" \
  --var="catalog_name=workspace" \
  --var="schema_name=de" \
  --var="raw_data_path=/Volumes/workspace/de/raw"
```

---

## Workshop Modules

The workshop is designed as a bundle-first, governed analytics flow. Each module builds on the assets from the previous one.

### Module 1 - Databricks Asset Bundles

| Asset | Purpose | Key concepts |
|-------|---------|--------------|
| databricks.yml | Bundle entry point | variables, targets, sync rules, deployment workflow |
| resources/pipeline.yml | Pipeline resource | DLT resource definition, source asset wiring |
| resources/dashboards.yml | Dashboard resources | Lakeview deployment, warehouse binding |
| resources/jobs.yml | Job resources | SQL notebook execution through bundles |
| resources/apps.yml | App resource | app deployment, source code packaging, runtime env |

### Module 2 - Unity Catalog and Controlled Access

| Asset | Purpose | Key concepts |
|-------|---------|--------------|
| scripts/setup_unity_catalog.sh | One-time catalog/schema/volume bootstrap | catalog, schema, volume creation |
| src/sql/06_governance.sql | Governance exercises | RBAC, masking, row filters, tags, lineage |

Suggested Unity Catalog use cases for this workshop:

1. Govern the raw landing zone in `workspace.de.raw` before any ingestion begins.
2. Separate engineer access to silver tables from analyst access to gold tables.
3. Mask behavioral identifiers such as `visitor_id` for broader audiences.
4. Apply row filters so client-facing consumers only see their own jobs or portfolios.
5. Tag sensitive clickstream assets and trace downstream lineage into dashboards and Genie.

### Module 3 - Pipelines and Medallion ETL

| Asset | Layer | Key concepts |
|-------|-------|--------------|
| src/dlt/01_bronze.py | Bronze | raw CSV ingestion, lineage metadata, schema-on-read landing |
| src/dlt/02_silver.py | Silver | type cleanup, deduplication, enforceable data contracts |
| src/dlt/03_gold.py | Gold | CTR funnel metrics, position bias, category and daily performance |

### Module 4 - Data Modeling and Star Schema

| Asset | Output | Key concepts |
|-------|--------|--------------|
| src/dlt/04_star_schema.py | fact_search_events + dimensions | dimensional modeling, reusable analytics grain, semantic join model |

### Module 5 - Expectations and Data Quality

| Asset | Purpose | Key concepts |
|-------|---------|--------------|
| src/dlt/02_silver.py | Row-level quality rules | expect, expect_or_drop, expect_or_fail |
| src/dlt/03_gold.py | Business-rule quality rules | clicks <= impressions, CTR bounds, aggregate consistency |
| src/sql/05_quality_dashboard.sql | Quality analytics | DLT event log queries, pass/fail trends, observability |
| src/dashboards/quality_dashboard.lvdash.json | Quality dashboard | operational monitoring for the pipeline |

### Module 6 - Dashboards: Quality and Insights

| Asset | Dashboard | Key concepts |
|-------|-----------|--------------|
| src/sql/04_insights_dashboard.sql | Clickstream Insights | KPI storytelling, marketplace funnel analysis |
| src/dashboards/insights_dashboard.lvdash.json | Clickstream Insights | visual analytics over gold tables |
| src/dashboards/client_demand_intelligence.lvdash.json | Client Demand Intelligence | business-facing account review narrative |
| src/dashboards/quality_dashboard.lvdash.json | Data Quality Dashboard | operational quality review |

### Module 7 - Metric Views: Semantic Layer

| Asset | Purpose | Key concepts |
|-------|---------|--------------|
| src/sql/07_metric_views.sql | Reusable semantic views | governed metrics, consistent KPI definitions, reusable business aggregations |
| resources/jobs.yml | Metric refresh job | operationalizing the semantic layer |

### Module 8 - Genie: AI/BI Exploration

| Asset | Purpose | Key concepts |
|-------|---------|--------------|
| src/sql/08_genie_setup.sql | Genie setup flow | grants, validation queries, curated conversation surface |
| src/genie/genie_space_template.json | Tracked Genie template | repeatable space bootstrap |
| scripts/create_genie_space.sh | Space creation helper | template-based creation, idempotent setup |

### Module 9 - Databricks App: Business Delivery Layer

| Asset | Purpose | Key concepts |
|-------|---------|--------------|
| app/app.py | Business-facing shell | packaging dashboards and Genie into one experience |
| app/app.yaml | Runtime definition | app command and runtime behavior |
| resources/apps.yml | Bundle app resource | deployable presentation layer |

---

## Learning Objectives

- Understand how Databricks Asset Bundles define and deploy the complete workshop surface
- Set up Unity Catalog foundations: catalog, schema, volume, RBAC, masking, and lineage
- Understand medallion architecture (bronze -> silver -> gold)
- Write DLT pipeline notebooks in Python using @dlt.table decorators
- Build a star schema on top of curated data for downstream analytics and exploration
- Apply data quality expectations at three enforcement levels
- Add cross-metric business-rule expectations in gold
- Parse mixed-type columns safely in silver
- Build funnel metrics and position-bias curves in gold
- Create Databricks dashboards from SQL over Delta tables
- Build a semantic layer with reusable metric views
- Set up a Genie space for natural language data exploration
- Package analytics assets into a Databricks App for business delivery
