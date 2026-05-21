# Workshop Guide

A hands-on walkthrough of a production-style data engineering stack on Databricks — from raw CSV ingestion to governed, queryable analytics and a business-facing app.

Run modules in order. Later modules depend on objects created by earlier ones.

---

## Before You Start

```bash
export WAREHOUSE_ID=$(databricks warehouses list -o json \
  | jq -r '.[] | select(.name=="Serverless Starter Warehouse") | .id' | head -n1)
echo "$WAREHOUSE_ID"
```

---

## Module 1 — Databricks Asset Bundles

**Infrastructure-as-code for Databricks.** A bundle is a `databricks.yml` file that declares all resources — pipelines, dashboards, jobs, apps, storage — so the entire workshop environment can be deployed, destroyed, and reproduced from Git.

Key files: `databricks.yml`, `resources/`

```bash
databricks bundle validate --target dev --var="warehouse_id=$WAREHOUSE_ID"
databricks bundle deploy  --target dev --var="warehouse_id=$WAREHOUSE_ID"
databricks bundle summary --target dev --var="warehouse_id=$WAREHOUSE_ID"
```

---

## Module 2 — Unity Catalog

**A three-level governance hierarchy:** Account → Workspace → Metastore → Catalog → Schema → Tables / Volumes.

The bundle deploy created the `workspace.clickstream_workshop` schema and the `raw` volume. Upload the source CSVs before running the pipeline:

```bash
databricks fs cp --recursive ./data/ \
  dbfs:/Volumes/workspace/clickstream_workshop/raw/ --overwrite
```

**Datasets**

| File | Description | Grain |
|------|-------------|-------|
| `impression_events.csv` | A job listing was shown to a visitor | Per impression |
| `click_events.csv` | A visitor clicked a job listing | Per click |
| `job_openings.csv` | Job posting metadata | Per job |
| `clients.csv` | Client company profiles | Per client |
| `freelancers.csv` | Freelancer profiles | Per freelancer |

The event files form a funnel: search → impressions → clicks. The profile files enrich it.

---

## Module 3 — Pipelines and Medallion Architecture

**Three layers with clear contracts:**

| Layer | What it does |
|-------|-------------|
| Bronze | Raw ingestion — no transformation, schema-on-read, full lineage |
| Silver | Typed, deduplicated, quality-enforced — the trusted source of truth |
| Gold | Pre-aggregated metrics ready for dashboards |

```bash
databricks bundle run --target dev --var="warehouse_id=$WAREHOUSE_ID" clickstream_workshop
```

While the pipeline runs, explore the **Pipeline Settings** panel: pipeline mode (triggered vs continuous), source code, target catalog location, compute, configurations, and notification settings. The **Schedule** tab shows how pipelines can be triggered on a cron schedule.

The **Run** dropdown has four options: Full refresh, Incremental, Select tables for refresh, and Dry run.

---

## Module 4 — Data Quality and Expectations

**DLT expectations are declarative quality rules.** Three enforcement levels:

| Decorator | On failure |
|-----------|-----------|
| `@expect` | Warn — log the violation, keep the row |
| `@expect_or_drop` | Drop the bad row, continue the pipeline |
| `@expect_or_fail` | Halt the entire pipeline |

Open `src/dlt/02_silver.py` to see the expectations in context. In the pipeline UI, click a silver table → **Table Metrics** to see pass/fail/drop counts per rule.

Render and deploy the quality dashboard:

```bash
export PIPELINE_ID=$(databricks pipelines list-pipelines -o json \
  | jq -r '.[] | select(.name | test("clickstream-workshop")) | .pipeline_id' | head -n1)
./scripts/render_quality_dashboard.sh "$PIPELINE_ID"
databricks bundle deploy --target dev --var="warehouse_id=$WAREHOUSE_ID" \
  --resource dashboards.quality_dashboard
```

---

## Module 5 — Data Modeling and Star Schema

**Two output shapes from the same pipeline:**

| Shape | Tables | Best for |
|-------|--------|---------|
| Gold | `gold_job_performance`, `gold_position_ctr`, `gold_daily_metrics` | Fixed dashboard tiles — fast, no joins |
| Star schema | `fact_search_events` + `dim_job`, `dim_client`, `dim_freelancer`, `dim_category`, `dim_date` | Flexible analytics — any slice, any grouping |

After the pipeline finishes, browse both sets of tables in the Catalog Explorer.

---

## Module 6 — Metric Views: Semantic Layer

**A semantic layer decouples business KPIs from query patterns.** Each metric view declares measures and dimensions in YAML — the query engine generates SQL dynamically, so the same view answers `CTR by category` and `CTR by day` without writing two queries.

```bash
databricks bundle run --target dev --var="warehouse_id=$WAREHOUSE_ID" metric_views
```

To query a metric view, wrap measures in `Measure()`:

```sql
SELECT category, Measure(ctr)
FROM workspace.clickstream_workshop.mv_category_performance
GROUP BY category
```

Metric view definitions: `src/metric_views/`

---

## Module 7 — Dashboards

Three dashboards ship with the workshop:

| Dashboard | Audience | Data source |
|-----------|----------|-------------|
| Data Quality | Data engineers | DLT event log |
| Clickstream Insights | Analysts | Gold tables |
| Client Demand Intelligence | Sales / account managers | Star schema + report views |

```bash
databricks bundle summary --target dev --var="warehouse_id=$WAREHOUSE_ID" -o json \
  | jq -r '.resources.dashboards | to_entries[] | "\(.key): \(.value.url)"'
```

---

## Module 8 — Genie: Natural Language Exploration

**Genie turns natural language into SQL.** Users ask questions in plain English; Genie generates and runs the query against the semantic layer (star schema + metric views).

```bash
./scripts/create_genie_space.sh "Clickstream Analytics" "$WAREHOUSE_ID"
```

Try asking questions like:
- *Which categories have the highest CTR this month?*
- *Which clients have the most impressions but lowest click-through rate?*
- *How does position affect CTR across job categories?*

---

## Module 9 — Databricks Apps

**Package dashboards and Genie into a guided experience for non-technical users.** The app in this workshop is a sales call workspace: pick a client, see their performance benchmarks, and ask Genie a contextual question — all in one screen.

```bash
HOST=https://dbc-890d8196-85c6.cloud.databricks.com

export CLIENT_DASHBOARD_URL=$(databricks bundle summary --target dev \
  --var="warehouse_id=$WAREHOUSE_ID" -o json \
  | jq -r '.resources.dashboards.client_demand_intelligence.url')

export INSIGHTS_DASHBOARD_URL=$(databricks bundle summary --target dev \
  --var="warehouse_id=$WAREHOUSE_ID" -o json \
  | jq -r '.resources.dashboards.clickstream_insights.url')

export GENIE_SPACE_ID=$(databricks genie list-spaces -o json \
  | jq -r '.spaces[] | select(.title=="Clickstream Analytics") | .space_id' | head -n1)

export GENIE_SPACE_URL="$HOST/genie/rooms/$GENIE_SPACE_ID"

databricks bundle run --target dev \
  --var="warehouse_id=$WAREHOUSE_ID" \
  --var="client_dashboard_url=$CLIENT_DASHBOARD_URL" \
  --var="insights_dashboard_url=$INSIGHTS_DASHBOARD_URL" \
  --var="genie_space_url=$GENIE_SPACE_URL" \
  client_demand_shell
```

App source: `app/app.py`

---

## Module 10 — Unity Catalog Governance

**Lock down the surface you've built.** Unity Catalog provides column masking, row filters, RBAC grants, data tags, and end-to-end lineage — all managed at the catalog level.

Open `src/sql/governance.sql` in the Databricks SQL editor and work through the exercises.

---

## Teardown

```bash
databricks bundle destroy --target dev --var="warehouse_id=$WAREHOUSE_ID"
```

