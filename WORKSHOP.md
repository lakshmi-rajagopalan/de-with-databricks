# Workshop Guide

A hands-on walkthrough of a production-style data engineering stack on Databricks — from raw CSV ingestion to governed, queryable analytics and a business-facing app.

Run modules in order. Later modules depend on objects created by earlier ones.

---

## The Problem

You are a data engineer at FlexHire, a freelance job marketplace. Every time a visitor searches for work, the platform records three types of events:

| Event | What it captures | Grain |
|-------|-----------------|-------|
| `search_events.csv` | A visitor submitted a search query | Per search session |
| `impression_events.csv` | A job listing was shown to a visitor | Per impression |
| `click_events.csv` | A visitor clicked a job listing | Per click |

These events form a **search funnel**: search → impressions → clicks. Three dimension tables describe the entities the events reference:

| Dimension | What it captures | Grain |
|-----------|-----------------|-------|
| `job_openings.csv` | Job posting metadata | Per job |
| `clients.csv` | Client company profiles | Per client |
| `freelancers.csv` | Freelancer profiles | Per freelancer |

The business wants to answer questions like: *Which job categories get the most clicks? Do featured listings outperform organic results? Which search queries drive the most traffic?* Your job is to build the pipeline that makes those questions answerable — and govern, surface, and serve the results.

---

## Before You Start

```bash
export DATABRICKS_CONFIG_PROFILE=flexhire-clickstream

export BUNDLE_VAR_warehouse_id=$(databricks warehouses list -o json \
  | jq -r '.[] | select(.name=="Serverless Starter Warehouse") | .id' | head -n1)
echo "$BUNDLE_VAR_warehouse_id"

export BUNDLE_VAR_catalog_name=flexhire
```

---

## Session 1 — Build

---

## Module 1 — Databricks Asset Bundles


**Infrastructure-as-code for Databricks.** A bundle is a `databricks.yml` file that declares all resources — pipelines, dashboards, jobs, apps, storage — so the entire workshop environment can be deployed, destroyed, and reproduced from Git.

Key files: `databricks.yml`, `resources/`

```bash
databricks bundle validate
databricks bundle deploy
databricks bundle summary
```

The bundle deploy creates four schemas (`raw`, `bronze`, `silver`, `gold`) and a `landing` volume under `raw` ready to receive the source data.

---

## Module 2 — Pipelines and Medallion Architecture

**Upload the raw data into the volume created by the bundle:**

```bash
databricks fs cp --recursive ./data/ \
  dbfs:/Volumes/{catalog_name}/raw/landing/ --overwrite
```

Each dataset lives in its own subdirectory (`search_events/`, `impression_events/`, `click_events/`, `job_openings/`, `clients/`, `freelancers/`). Auto Loader watches each directory independently so new files can be dropped per-dataset without affecting others.

**Three layers with clear contracts:**

| Layer | What it does |
|-------|-------------|
| Bronze | Raw ingestion — no transformation, schema-on-read, full lineage |
| Silver | Typed, deduplicated, quality-enforced — the trusted source of truth |
| Gold | Pre-aggregated metrics ready for dashboards |

```bash
databricks bundle run clickstream
```

While the pipeline runs, explore the **Pipeline Settings** panel: pipeline mode (triggered vs continuous), source code, target catalog location, compute, configurations, and notification settings. The **Schedule** tab shows how pipelines can be triggered on a cron schedule.

The **Run** dropdown has four options: Full refresh, Incremental, Select tables for refresh, and Dry run.

---

## Module 3 — Data Quality and Expectations

**DLT expectations are declarative quality rules.** Three enforcement levels:

| Decorator | On failure |
|-----------|-----------|
| `@expect` | Warn — log the violation, keep the row |
| `@expect_or_drop` | Drop the bad row, continue the pipeline |
| `@expect_or_fail` | Halt the entire pipeline |

Open `src/dlt/silver.py` to see the expectations in context. In the pipeline UI, click a silver table → **Table Metrics** to see pass/fail/drop counts per rule.

**The quarantine pattern** is a fourth option not covered by the decorators above: instead of dropping bad rows, route them to a separate table so they can be investigated and reprocessed once the source issue is fixed. `@expect_or_drop` is appropriate when bad rows are meaningless noise; the quarantine pattern is appropriate when bad rows have diagnostic or analytical value.

Use case in this pipeline: `silver_click_events` drops bot traffic with `@expect_or_drop("no_bots", ...)`. Bot clicks aren't useful for CTR analysis, but they are useful for fraud monitoring and volume trending. `quarantine_click_events` reads the same bronze source and captures every row that silver would reject, tagging each with a `quarantine_reason` (`bot_traffic`, `null_event_id`, `invalid_position`). No data is lost — the two tables are complementary views of the same raw events.

Render and deploy the quality dashboard:

```bash
export PIPELINE_ID=$(databricks pipelines list-pipelines -o json \
  | jq -r '.[] | select(.name | test("clickstream")) | .pipeline_id' | head -n1)
./scripts/render_quality_dashboard.sh "$PIPELINE_ID"
databricks bundle deploy
```

---

## Session 2 — Model & Govern

---

## Module 4 — Data Modeling and Star Schema

**Two output shapes from the same pipeline:**

| Shape | Tables | Best for |
|-------|--------|---------|
| Gold | `gold_job_performance`, `gold_position_ctr`, `gold_daily_metrics` | Fixed dashboard tiles — fast, no joins |
| Star schema | `fact_search_events` + `dim_job`, `dim_client`, `dim_freelancer`, `dim_category`, `dim_date` | Flexible analytics — any slice, any grouping |

After the pipeline finishes, browse both sets of tables in the Catalog Explorer.

---

## Module 5 — Unity Catalog

**Organizational hierarchy:** Account → Workspace → Metastore

**Three-level namespace:** Catalog → Schema → Tables / Views / Volumes

Unity Catalog centralises access control, lineage, and data discovery across all workspaces in your account. Every object the pipeline wrote — tables, views, volumes — lives in this hierarchy and is governed from here.

**Lock down the data before building the analytics layer on top of it.** Unity Catalog provides column masking, row filters, RBAC grants, data tags, and end-to-end lineage — all managed at the catalog level. Govern the pipeline output now so every downstream consumer (metric views, dashboards, Genie, Apps) inherits the same access controls.

Open `src/sql/governance.sql` in the Databricks SQL editor and work through the exercises.

---

## Session 3 — Surface

---

## Module 6 — Metric Views: Semantic Layer

**A semantic layer decouples business KPIs from query patterns.** Each metric view declares measures and dimensions in YAML — the query engine generates SQL dynamically, so the same view answers `CTR by category` and `CTR by day` without writing two queries.

```bash
databricks bundle run metric_views
```

To query a metric view, wrap measures in `Measure()`:

```sql
SELECT category, Measure(ctr)
FROM {catalog_name}.gold.mv_category_performance
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
databricks bundle summary -o json \
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
databricks bundle run client_demand_app
```

The app runs under a dedicated service principal that needs explicit access to the data and the Genie space. After the app starts, find its SP ID and grant permissions:

```bash
# 1. Get the app service principal application UUID (needed for GRANT statements)
NUMERIC_ID=$(databricks apps get clickstream-client-demand-app --output json \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['service_principal_id'])")
SP_ID=$(databricks service-principals get $NUMERIC_ID --output json \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['applicationId'])")
echo $SP_ID
```

```sql
-- 2. Run in Databricks SQL editor — replace <SP_ID> with the value above
GRANT USE CATALOG ON CATALOG flexhire                      TO `<SP_ID>`;
GRANT USE SCHEMA  ON SCHEMA  {catalog_name}.gold TO `<SP_ID>`;
GRANT SELECT      ON SCHEMA  {catalog_name}.gold TO `<SP_ID>`;
```

**3. Grant Genie space access via API** — the Genie space lives in `/Shared`, so its ACL can be managed programmatically:

```bash
SPACE_ID=$(databricks genie list-spaces --output json \
  | python3 -c "import sys,json; [print(s['space_id']) for s in json.load(sys.stdin) if s.get('title')=='Clickstream Analytics']")

databricks api patch "/api/2.0/permissions/genie/$SPACE_ID" \
  --json "{\"access_control_list\": [{\"service_principal_name\": \"$SP_ID\", \"permission_level\": \"CAN_MANAGE\"}]}"
```

App source: `app/app.py`

---

## Teardown

```bash
databricks bundle destroy
```
