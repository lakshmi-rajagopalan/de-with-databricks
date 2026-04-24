-- Databricks notebook source

-- COMMAND ----------
-- MAGIC %md
-- MAGIC # Data Governance — Unity Catalog RBAC
-- MAGIC
-- MAGIC **Step 3 of the workshop.** Demonstrates Unity Catalog governance features applied
-- MAGIC to the clickstream tables built in earlier steps.
-- MAGIC
-- MAGIC | Feature | What it demonstrates |
-- MAGIC |---------|---------------------|
-- MAGIC | Data classification tags | Mark tables and columns with sensitivity metadata |
-- MAGIC | RBAC (grants) | Control which roles can read which tables |
-- MAGIC | Column masking | Hash PII for non-privileged users at query time |
-- MAGIC | Row-level security | Clients can only see their own job data |
-- MAGIC | Lineage | Audit who queried what via system tables |
-- MAGIC
-- MAGIC **Prerequisites:** the DLT pipeline must have run at least once so that the tables
-- MAGIC `workspace.de.silver_click_events`, `workspace.de.fact_search_events`, and
-- MAGIC `workspace.de.dim_job` exist.

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 1 · Data Classification — system tags
-- MAGIC
-- MAGIC Unity Catalog lets you attach key-value tags to catalogs, schemas, tables, and
-- MAGIC columns. Tags power data discovery (search by sensitivity) and can trigger
-- MAGIC downstream policy evaluation in tools like Collibra or Purview.
-- MAGIC
-- MAGIC > **Note:** `SET TAGS` requires `MODIFY` privilege on the table.

-- COMMAND ----------

-- Mark silver_click_events as containing PII (visitor_id is a user identifier)
ALTER TABLE workspace.de.silver_click_events
  SET TAGS ('pii' = 'true', 'sensitivity' = 'high', 'domain' = 'search');

-- COMMAND ----------

-- Mark fact_search_events as a gold/star-schema table for discoverability
ALTER TABLE workspace.de.fact_search_events
  SET TAGS ('pii' = 'true', 'sensitivity' = 'high', 'layer' = 'gold', 'domain' = 'search');

-- COMMAND ----------

-- Non-PII gold tables — low sensitivity, analytics-ready
ALTER TABLE workspace.de.gold_job_performance
  SET TAGS ('sensitivity' = 'low', 'layer' = 'gold');

ALTER TABLE workspace.de.dim_job
  SET TAGS ('sensitivity' = 'low', 'layer' = 'gold');

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 2 · RBAC — Roles and Grants
-- MAGIC
-- MAGIC Unity Catalog uses SQL `GRANT` / `REVOKE` statements against groups.
-- MAGIC Groups are managed in the Databricks account console or via SCIM.
-- MAGIC
-- MAGIC **Two example roles used here:**
-- MAGIC - `analyst` — read-only access to aggregated / masked gold tables
-- MAGIC - `data_engineer` — full access to all layers including PII silver tables
-- MAGIC
-- MAGIC > Replace `analyst` and `data_engineer` with the actual group names in your workspace.

-- COMMAND ----------

-- Analysts can use the schema and read all gold / star-schema tables
GRANT USE SCHEMA ON SCHEMA workspace.de TO `analyst`;

GRANT SELECT ON TABLE workspace.de.fact_search_events    TO `analyst`;
GRANT SELECT ON TABLE workspace.de.dim_job               TO `analyst`;
GRANT SELECT ON TABLE workspace.de.dim_client            TO `analyst`;
GRANT SELECT ON TABLE workspace.de.dim_category          TO `analyst`;
GRANT SELECT ON TABLE workspace.de.dim_date              TO `analyst`;
GRANT SELECT ON TABLE workspace.de.gold_job_performance  TO `analyst`;
GRANT SELECT ON TABLE workspace.de.gold_position_ctr     TO `analyst`;
GRANT SELECT ON TABLE workspace.de.gold_category_performance TO `analyst`;
GRANT SELECT ON TABLE workspace.de.gold_daily_metrics    TO `analyst`;

-- COMMAND ----------

-- Data engineers get full access to all layers (including raw PII in silver)
GRANT ALL PRIVILEGES ON SCHEMA workspace.de TO `data_engineer`;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 3 · Column Masking — protect visitor_id
-- MAGIC
-- MAGIC `visitor_id` is a PII identifier. A masking function hashes the value for
-- MAGIC anyone who is not a member of the `data_engineer` group. Analysts see a
-- MAGIC deterministic SHA-256 hash — useful for counting distinct visitors — but
-- MAGIC cannot reverse it to a real identity.
-- MAGIC
-- MAGIC Column masks are **transparent**: queries don't change; the function is applied
-- MAGIC at read time by the Unity Catalog query engine.

-- COMMAND ----------

-- Create the masking function
CREATE OR REPLACE FUNCTION workspace.de.mask_visitor_id(visitor_id STRING)
  RETURN IF(IS_MEMBER('data_engineer'), visitor_id, SHA2(visitor_id, 256));

-- COMMAND ----------

-- Apply the mask to silver_click_events
ALTER TABLE workspace.de.silver_click_events
  ALTER COLUMN visitor_id
  SET MASK workspace.de.mask_visitor_id;

-- COMMAND ----------

-- Apply the same mask to fact_search_events
ALTER TABLE workspace.de.fact_search_events
  ALTER COLUMN visitor_id
  SET MASK workspace.de.mask_visitor_id;

-- COMMAND ----------

-- Verify: run this as an analyst — you should see hashed values
-- Run this as a data_engineer — you should see the real visitor_id
SELECT visitor_id FROM workspace.de.silver_click_events LIMIT 5;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 4 · Row-Level Security — clients see only their own jobs
-- MAGIC
-- MAGIC The `dim_job` table contains job postings from many different clients.
-- MAGIC A row filter ensures each client (authenticated via `CURRENT_USER()`) can only
-- MAGIC query rows where `client_uid` matches their identity.
-- MAGIC
-- MAGIC Data engineers bypass the filter so they can see the full dataset.
-- MAGIC
-- MAGIC > **Important:** `CURRENT_USER()` returns the Databricks user email. For this
-- MAGIC > filter to work with real clients, `client_uid` values must match user emails
-- MAGIC > or the function must look up a mapping table.

-- COMMAND ----------

-- Create the row filter function
CREATE OR REPLACE FUNCTION workspace.de.client_row_filter(client_uid STRING)
  RETURN IS_MEMBER('data_engineer') OR client_uid = CURRENT_USER();

-- COMMAND ----------

-- Apply row filter to dim_job
ALTER TABLE workspace.de.dim_job
  SET ROW FILTER workspace.de.client_row_filter ON (client_uid);

-- COMMAND ----------

-- Verify row filter is in place — a non-engineer user should only see their own rows
DESCRIBE EXTENDED workspace.de.dim_job;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 5 · Lineage — audit system tables
-- MAGIC
-- MAGIC Unity Catalog records all `SELECT`, `CREATE TABLE`, and other access events in the
-- MAGIC `system.access.audit` table. Use this to answer:
-- MAGIC - Who has been querying the PII silver tables?
-- MAGIC - Which pipelines created which tables?
-- MAGIC - Are there any unexpected readers of the `fact_search_events` table?
-- MAGIC
-- MAGIC > `system.access.audit` requires the workspace to have system table access enabled.
-- MAGIC > Contact your workspace admin if this table returns an error.

-- COMMAND ----------

-- Recent access events across all workshop tables
SELECT
    event_time,
    user_identity.email        AS user_email,
    action_name,
    request_params.table_full_name AS table_name
FROM system.access.audit
WHERE action_name IN ('SELECT', 'CREATE TABLE', 'ALTER TABLE', 'GRANT')
  AND request_params.table_full_name LIKE 'workspace.de.%'
ORDER BY event_time DESC
LIMIT 50;

-- COMMAND ----------

-- Column-level lineage: which upstream columns feed into fact_search_events?
SELECT
    source_table_full_name,
    source_column_name,
    target_table_full_name,
    target_column_name
FROM system.lineage.column_lineage
WHERE target_table_full_name = 'workspace.de.fact_search_events'
ORDER BY target_column_name;
