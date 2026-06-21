-- Databricks notebook source

-- COMMAND ----------
-- MAGIC %md
-- MAGIC # Data Governance — Unity Catalog RBAC
-- MAGIC
-- MAGIC **Module 9 of the workshop.** Demonstrates Unity Catalog governance features applied
-- MAGIC to the full analytics surface — tables, semantic views, dashboards, and Genie space — built in Modules 1–8.
-- MAGIC
-- MAGIC | Feature | What it demonstrates |
-- MAGIC |---------|---------------------|
-- MAGIC | Data classification tags | Mark tables and columns with sensitivity metadata |
-- MAGIC | RBAC (grants) | Control which roles can read which tables |
-- MAGIC | Column masking | Hash PII for non-privileged users at query time |
-- MAGIC | Row-level security | Clients can only see their own job data |
-- MAGIC | Lineage | Audit who queried what via system tables |
-- MAGIC
-- MAGIC **Workshop use cases driving each exercise below:**
-- MAGIC
-- MAGIC 1. Govern the raw landing zone in `clickstream_dev.raw` and the populated silver/gold tables.
-- MAGIC 2. Separate engineer access to silver tables from analyst access to gold tables.
-- MAGIC 3. Mask behavioral identifiers such as `visitor_id` for broader audiences.
-- MAGIC 4. Apply row filters so client-facing consumers only see their own jobs or portfolios.
-- MAGIC 5. Tag sensitive clickstream assets and trace downstream lineage into dashboards and Genie.
-- MAGIC
-- MAGIC **Prerequisites:** the DLT pipeline must have run at least once so that the tables
-- MAGIC `clickstream_dev.silver.click_events`, `clickstream_dev.gold.fact_search_events`, and
-- MAGIC `clickstream_dev.gold.dim_job` exist.

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
ALTER TABLE clickstream_dev.silver.click_events
  SET TAGS ('pii' = 'true', 'sensitivity' = 'high', 'domain' = 'search');

-- COMMAND ----------

-- Mark fact_search_events as a gold/star-schema table for discoverability
ALTER TABLE clickstream_dev.gold.fact_search_events
  SET TAGS ('pii' = 'true', 'sensitivity' = 'high', 'layer' = 'gold', 'domain' = 'search');

-- COMMAND ----------

-- Non-PII gold tables — low sensitivity, analytics-ready
ALTER TABLE clickstream_dev.gold.job_performance
  SET TAGS ('sensitivity' = 'low', 'layer' = 'gold');

ALTER TABLE clickstream_dev.gold.dim_job
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
GRANT USE SCHEMA ON SCHEMA clickstream_dev.gold TO `analyst`;

GRANT SELECT ON TABLE clickstream_dev.gold.fact_search_events    TO `analyst`;
GRANT SELECT ON TABLE clickstream_dev.gold.dim_job               TO `analyst`;
GRANT SELECT ON TABLE clickstream_dev.gold.dim_client            TO `analyst`;
GRANT SELECT ON TABLE clickstream_dev.gold.dim_category          TO `analyst`;
GRANT SELECT ON TABLE clickstream_dev.gold.dim_date              TO `analyst`;
GRANT SELECT ON TABLE clickstream_dev.gold.job_performance  TO `analyst`;
GRANT SELECT ON TABLE clickstream_dev.gold.position_ctr     TO `analyst`;
GRANT SELECT ON TABLE clickstream_dev.gold.category_performance TO `analyst`;
GRANT SELECT ON TABLE clickstream_dev.gold.daily_metrics    TO `analyst`;

-- COMMAND ----------

-- Data engineers get full access to all layers (including raw PII in silver)
GRANT ALL PRIVILEGES ON SCHEMA clickstream_dev.gold TO `data_engineer`;

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
CREATE OR REPLACE FUNCTION clickstream_dev.gold.mask_visitor_id(visitor_id STRING)
  RETURN IF(IS_MEMBER('data_engineer'), visitor_id, SHA2(visitor_id, 256));

-- COMMAND ----------

-- Apply the mask to silver_click_events
ALTER TABLE clickstream_dev.silver.click_events
  ALTER COLUMN visitor_id
  SET MASK clickstream_dev.gold.mask_visitor_id;

-- COMMAND ----------

-- Apply the same mask to fact_search_events
ALTER TABLE clickstream_dev.gold.fact_search_events
  ALTER COLUMN visitor_id
  SET MASK clickstream_dev.gold.mask_visitor_id;

-- COMMAND ----------

-- Verify: run this as an analyst — you should see hashed values
-- Run this as a data_engineer — you should see the real visitor_id
SELECT visitor_id FROM clickstream_dev.silver.click_events LIMIT 5;

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
CREATE OR REPLACE FUNCTION clickstream_dev.gold.client_row_filter(client_uid STRING)
  RETURN IS_MEMBER('data_engineer') OR client_uid = CURRENT_USER();

-- COMMAND ----------

-- Apply row filter to dim_job
ALTER TABLE clickstream_dev.gold.dim_job
  SET ROW FILTER clickstream_dev.gold.client_row_filter ON (client_uid);

-- COMMAND ----------

-- Verify row filter is in place — a non-engineer user should only see their own rows
DESCRIBE EXTENDED clickstream_dev.gold.dim_job;

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
  AND request_params.table_full_name LIKE 'clickstream_dev.%'
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
WHERE target_table_full_name = 'clickstream_dev.gold.fact_search_events'
ORDER BY target_column_name;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 6 · App Service Principal — grant data access
-- MAGIC
-- MAGIC The Databricks App (Module 9) runs under a dedicated service principal that is
-- MAGIC created at deploy time. That SP is **not** a member of the `analyst` or
-- MAGIC `data_engineer` groups, so the grants above do not cover it automatically.
-- MAGIC
-- MAGIC **After every fresh `bundle deploy`, get the SP ID and grant access:**
-- MAGIC
-- MAGIC ```bash
-- MAGIC NUMERIC_ID=$(databricks apps get clickstream-client-demand-app --output json \
-- MAGIC   | python3 -c "import sys,json; print(json.load(sys.stdin)['service_principal_id'])")
-- MAGIC SP_ID=$(databricks service-principals get $NUMERIC_ID --output json \
-- MAGIC   | python3 -c "import sys,json; print(json.load(sys.stdin)['applicationId'])")
-- MAGIC echo $SP_ID
-- MAGIC ```
-- MAGIC
-- MAGIC Then run the three SQL cells below, substituting the SP ID.

-- COMMAND ----------

-- Replace <SP_ID> with the application UUID returned above
GRANT USE CATALOG ON CATALOG clickstream_dev                      TO `<SP_ID>`;
GRANT USE SCHEMA  ON SCHEMA  clickstream_dev.gold TO `<SP_ID>`;
GRANT SELECT      ON SCHEMA  clickstream_dev.gold TO `<SP_ID>`;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC **Grant Genie space access via API**
-- MAGIC
-- MAGIC The Genie space lives in `/Shared` so its ACL is manageable via the permissions API:
-- MAGIC
-- MAGIC ```bash
-- MAGIC SPACE_ID=$(databricks genie list-spaces --output json \
-- MAGIC   | python3 -c "import sys,json; [print(s['space_id']) for s in json.load(sys.stdin) if s.get('title')=='Clickstream Analytics']")
-- MAGIC
-- MAGIC databricks api patch "/api/2.0/permissions/genie/$SPACE_ID" \
-- MAGIC   --json "{\"access_control_list\": [{\"service_principal_name\": \"$SP_ID\", \"permission_level\": \"CAN_MANAGE\"}]}"
-- MAGIC ```
