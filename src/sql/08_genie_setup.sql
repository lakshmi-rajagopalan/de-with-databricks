-- Databricks notebook source

-- COMMAND ----------
-- MAGIC %md
-- MAGIC # Data Exploration — Genie (AI/BI) Setup
-- MAGIC
-- MAGIC **Step 4 of the workshop.** Genie is Databricks' natural language interface for
-- MAGIC data exploration. You ask questions in plain English; Genie generates and runs SQL,
-- MAGIC then explains the results.
-- MAGIC
-- MAGIC This notebook:
-- MAGIC 1. Grants the Genie service principal read access to the star schema tables
-- MAGIC 2. Walks you through creating a Genie Space in the UI
-- MAGIC 3. Provides sample questions to seed the space and test the AI
-- MAGIC
-- MAGIC **Prerequisites:**
-- MAGIC - The DLT pipeline has run and all tables exist in `workspace.de`
-- MAGIC - Step 3 (governance) has been run so roles are in place
-- MAGIC - Genie is enabled in your Databricks workspace (AI/BI feature flag)

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 1 · Grant table access for Genie
-- MAGIC
-- MAGIC Genie queries tables on behalf of the logged-in user, so the user's own grants
-- MAGIC (set up in step 3) already control what Genie can see.
-- MAGIC
-- MAGIC If you want Genie to run as a shared service principal, grant that principal
-- MAGIC SELECT access on the tables you want it to use.

-- COMMAND ----------

-- Grant the analyst group (and therefore Genie users in that group) SELECT on star schema
GRANT SELECT ON TABLE workspace.de.fact_search_events    TO `analyst`;
GRANT SELECT ON TABLE workspace.de.dim_job               TO `analyst`;
GRANT SELECT ON TABLE workspace.de.dim_client            TO `analyst`;
GRANT SELECT ON TABLE workspace.de.dim_category          TO `analyst`;
GRANT SELECT ON TABLE workspace.de.dim_date              TO `analyst`;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 2 · Verify tables are ready for Genie
-- MAGIC
-- MAGIC Run these cells to confirm the tables are populated and the schema looks correct
-- MAGIC before opening Genie.

-- COMMAND ----------

-- Quick row counts across all tables Genie will use
SELECT 'fact_search_events' AS table_name, COUNT(*) AS row_count FROM workspace.de.fact_search_events
UNION ALL
SELECT 'dim_job',      COUNT(*) FROM workspace.de.dim_job
UNION ALL
SELECT 'dim_client',   COUNT(*) FROM workspace.de.dim_client
UNION ALL
SELECT 'dim_category', COUNT(*) FROM workspace.de.dim_category
UNION ALL
SELECT 'dim_date',     COUNT(*) FROM workspace.de.dim_date
ORDER BY table_name;

-- COMMAND ----------

-- Preview the fact table — check was_clicked and time_to_click_secs look right
SELECT *
FROM workspace.de.fact_search_events
ORDER BY event_ts DESC
LIMIT 10;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 3 · Create a Genie Space (UI walkthrough)
-- MAGIC
-- MAGIC Genie Spaces are configured in the Databricks UI. Follow these steps:
-- MAGIC
-- MAGIC ### 3a — Open Genie
-- MAGIC 1. In the left sidebar, click **Genie** (the chat bubble icon)
-- MAGIC 2. Click **+ New Space**
-- MAGIC
-- MAGIC ### 3b — Name your space
-- MAGIC - **Display name:** `Clickstream Analytics`
-- MAGIC - **Description:** `Upwork job search event data — impressions, clicks, CTR and job performance`
-- MAGIC
-- MAGIC ### 3c — Add tables
-- MAGIC Click **Add tables** and select all of the following from `workspace.de`:
-- MAGIC
-- MAGIC | Table | Why include it |
-- MAGIC |-------|---------------|
-- MAGIC | `fact_search_events` | Core event grain — every impression and click |
-- MAGIC | `dim_job` | Job attributes — title, category, budget |
-- MAGIC | `dim_client` | Client posting history |
-- MAGIC | `dim_category` | Category aggregates |
-- MAGIC | `dim_date` | Calendar hierarchy for date filtering |
-- MAGIC | `gold_job_performance` | Pre-aggregated funnel — fastest for job-level questions |
-- MAGIC | `gold_position_ctr` | Position bias curve — fastest for ranking questions |
-- MAGIC | `gold_category_performance` | Category rollup |
-- MAGIC | `gold_daily_metrics` | Daily trend |
-- MAGIC
-- MAGIC ### 3d — Add context instructions
-- MAGIC In the **Instructions** box, paste the following to help Genie understand the domain:
-- MAGIC
-- MAGIC ```
-- MAGIC This workspace contains Upwork job marketplace clickstream data.
-- MAGIC
-- MAGIC Key concepts:
-- MAGIC - An "impression" means a job listing was shown to a visitor in search results or featured jobs.
-- MAGIC - A "click" means the visitor clicked on a job listing after seeing it.
-- MAGIC - CTR (click-through rate) = clicks / impressions × 100.
-- MAGIC - "Position" is the rank of a job in search results (1 = top). Higher positions get fewer clicks due to position bias.
-- MAGIC - "Sublocation" is either "search_results" or "featured_jobs".
-- MAGIC - fact_search_events is the grain table: one row per impression, with was_clicked=true if clicked.
-- MAGIC - For aggregated questions, prefer the gold_ tables as they are pre-computed.
-- MAGIC - visitor_id may be hashed for non-engineer users — use COUNT(DISTINCT visitor_id) for unique visitor metrics.
-- MAGIC ```
-- MAGIC
-- MAGIC ### 3e — Save and start chatting
-- MAGIC Click **Save** then **Start conversation**.

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 4 · Sample questions to test Genie
-- MAGIC
-- MAGIC Paste these questions into the Genie chat to verify it returns sensible results.
-- MAGIC
-- MAGIC **Funnel questions:**
-- MAGIC - "What is the overall click-through rate?"
-- MAGIC - "How many unique visitors have we seen?"
-- MAGIC - "What percentage of impressions resulted in a click?"
-- MAGIC
-- MAGIC **Job performance:**
-- MAGIC - "Which are the top 5 job categories by click-through rate?"
-- MAGIC - "Show me the jobs with the most impressions this week"
-- MAGIC - "Which job titles have the highest CTR?"
-- MAGIC
-- MAGIC **Position bias:**
-- MAGIC - "How does CTR change with position in search results?"
-- MAGIC - "Show me a position bias curve"
-- MAGIC - "What is the average time to click by position?"
-- MAGIC
-- MAGIC **Trend analysis:**
-- MAGIC - "What is the daily trend in impressions over the past week?"
-- MAGIC - "Which day had the highest CTR?"
-- MAGIC - "Show me impressions and clicks by day as a chart"
-- MAGIC
-- MAGIC **Featured vs search:**
-- MAGIC - "Compare featured jobs vs search results performance"
-- MAGIC - "Do featured job placements get a higher CTR than organic search results?"
-- MAGIC
-- MAGIC **Client and category:**
-- MAGIC - "Which clients have the most active job postings?"
-- MAGIC - "What is the average budget for jobs in the Engineering category?"

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 5 · Genie CLI (optional)
-- MAGIC
-- MAGIC Genie Spaces can also be created via the Databricks REST API or CLI when the
-- MAGIC feature is generally available in your workspace. Example:
-- MAGIC
-- MAGIC ```bash
-- MAGIC databricks api post /api/2.0/genie/spaces \
-- MAGIC   --json '{
-- MAGIC     "display_name": "Clickstream Analytics",
-- MAGIC     "description": "Upwork job search clickstream — impressions, clicks, CTR",
-- MAGIC     "table_identifiers": [
-- MAGIC       "workspace.de.fact_search_events",
-- MAGIC       "workspace.de.dim_job",
-- MAGIC       "workspace.de.dim_client",
-- MAGIC       "workspace.de.dim_category",
-- MAGIC       "workspace.de.dim_date",
-- MAGIC       "workspace.de.gold_job_performance",
-- MAGIC       "workspace.de.gold_position_ctr",
-- MAGIC       "workspace.de.gold_category_performance",
-- MAGIC       "workspace.de.gold_daily_metrics"
-- MAGIC     ]
-- MAGIC   }'
-- MAGIC ```
-- MAGIC
-- MAGIC > Check `databricks genie --help` for the exact command syntax in your CLI version.
