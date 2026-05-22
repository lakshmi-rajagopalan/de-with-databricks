# Upwork Clickstream - Data Engineering Workshop

A hands-on Databricks workshop built on Upwork job marketplace clickstream data — job impressions, click events, and job opening metadata.

Covers the full data engineering stack: ingestion, medallion pipelines, data quality, semantic layer, dashboards, Genie, governed access, and a business-facing app.

For step-by-step workshop instructions, see [WORKSHOP.md](WORKSHOP.md).

---

## Architecture

```
GitHub repo (data/*.csv + src/)
  → Unity Catalog volume  (workspace.clickstream_workshop.raw)
  → DLT pipeline          (bronze → silver → gold + star schema)
  → Semantic layer        (metric views + report views)
  → Dashboards + Genie + App
```

---

## Repo Layout

| Path | Purpose |
|------|---------|
| `data/` | Raw CSV inputs |
| `src/dlt/` | DLT Python pipeline (bronze, silver, gold, star schema) |
| `src/sql/` | SQL for governance, metric views, and Genie setup |
| `src/metric_views/` | Metric view YAML definitions |
| `src/dashboards/` | Lakeview dashboard JSON assets |
| `src/genie/` | Genie space template |
| `app/` | Databricks App (Flask) |
| `resources/` | Bundle resource definitions |
| `scripts/` | Helper scripts for setup and rendering |
| `docs/` | Supplementary documentation |

---

## Setup

**1 — Clone the repo**

```bash
git clone https://github.com/lakshmi-rajagopalan/de-with-databricks.git
cd de-with-databricks
```

**2 — Install Databricks CLI**

```bash
brew tap databricks/tap && brew install databricks
databricks --version
```

**3 — Authenticate**

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com
```

---

## Deploy

```bash
export WAREHOUSE_ID=$(databricks warehouses list -o json \
  | jq -r '.[] | select(.name=="Serverless Starter Warehouse") | .id' | head -n1)

databricks bundle deploy --target dev --var="warehouse_id=$WAREHOUSE_ID"
```

See [WORKSHOP.md](WORKSHOP.md) for per-module commands and what to run in what order.

---

## Learning Objectives

- Declare and deploy a full Databricks environment with Asset Bundles
- Set up Unity Catalog: catalog, schema, volume, RBAC, masking, and lineage
- Build a medallion pipeline (bronze → silver → gold) with DLT
- Apply data quality expectations at three enforcement levels
- Model data as a star schema for flexible analytics
- Define a semantic layer with reusable metric views
- Build AI/BI dashboards over Delta tables
- Set up a Genie space for natural language data exploration
- Package analytics assets into a Databricks App for business delivery

