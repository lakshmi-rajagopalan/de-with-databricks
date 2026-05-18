-- Databricks notebook source

-- COMMAND ----------
-- MAGIC %md
-- MAGIC # Star Schema — Dimensional Model
-- MAGIC
-- MAGIC **Purpose:** Restructure the silver layer into an explicit star schema suitable for
-- MAGIC self-service analytics, BI tools, and Genie (AI/BI natural language queries).
-- MAGIC
-- MAGIC ## Schema diagram
-- MAGIC
-- MAGIC ```
-- MAGIC  dim_freelancer           dim_date
-- MAGIC  visitor_id                  │
-- MAGIC       │                   event_date
-- MAGIC       │                      │
-- MAGIC  dim_client ─── dim_job ─── fact_search_events ─── dim_category
-- MAGIC  client_uid    opening_uid   (one row per impression)
-- MAGIC ```
-- MAGIC
-- MAGIC | Table | Grain | Role |
-- MAGIC |-------|-------|------|
-- MAGIC | `dim_job` | One row per job opening | Dimension |
-- MAGIC | `dim_client` | One row per client (enriched from clients.csv) | Dimension |
-- MAGIC | `dim_freelancer` | One row per freelancer (from freelancers.csv) | Dimension |
-- MAGIC | `dim_category` | One row per job category | Dimension |
-- MAGIC | `dim_date` | One row per calendar date | Date dimension |
-- MAGIC | `fact_search_events` | One row per impression event | Fact |
-- MAGIC
-- MAGIC **DLT concepts covered:**
-- MAGIC - `LIVE.` prefix to reference silver tables within the pipeline
-- MAGIC - Deriving a date spine from event data (no external seed needed)
-- MAGIC - LEFT JOIN to enrich the fact table with click outcome in a single pass
-- MAGIC - Enriching dimensions by joining multiple silver sources

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## dim_job
-- MAGIC
-- MAGIC One row per job opening. Carries all descriptive attributes about the job so
-- MAGIC downstream queries never need to join back to silver_job_openings.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_job
COMMENT "Job dimension — one row per opening with all descriptive attributes"
TBLPROPERTIES ("quality" = "gold")
AS SELECT
  opening_uid,
  title,
  category,
  budget_type,
  budget_amount,
  client_uid,
  posted_at,
  is_active
FROM LIVE.silver_job_openings;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## dim_client
-- MAGIC
-- MAGIC One row per client. Enriched by joining the client profile data (company name,
-- MAGIC country, industry, spend) from `silver_clients` onto the posting activity
-- MAGIC derived from `silver_job_openings`.
-- MAGIC
-- MAGIC Clients that appear in job postings but not in `clients.csv` still appear here
-- MAGIC via the LEFT JOIN — their profile attributes will be null.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_client
COMMENT "Client dimension — job posting activity enriched with company profile"
TBLPROPERTIES ("quality" = "gold")
AS
WITH activity AS (
  SELECT
    client_uid,
    COUNT(opening_uid)  AS jobs_posted,
    MIN(posted_at)      AS first_posted_at,
    MAX(posted_at)      AS latest_posted_at
  FROM LIVE.silver_job_openings
  GROUP BY client_uid
)
SELECT
  a.client_uid,
  a.jobs_posted,
  a.first_posted_at,
  a.latest_posted_at,
  p.company_name,
  p.country,
  p.industry,
  p.payment_verified,
  p.total_spend_usd,
  p.avg_rating
FROM activity a
LEFT JOIN LIVE.silver_clients p USING (client_uid);

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## dim_category
-- MAGIC
-- MAGIC One row per job category with summary statistics.
-- MAGIC Acts as a conformed dimension shared between the star schema and the gold
-- MAGIC aggregation tables.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_category
COMMENT "Category dimension — one row per category with job count and average budget"
TBLPROPERTIES ("quality" = "gold")
AS SELECT
  category,
  COUNT(opening_uid)           AS total_jobs,
  ROUND(AVG(budget_amount), 2) AS avg_budget_amount
FROM LIVE.silver_job_openings
GROUP BY category;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## dim_freelancer
-- MAGIC
-- MAGIC One row per freelancer, keyed on `visitor_id` which links to both
-- MAGIC `fact_search_events` (impressions) and `silver_click_events` (clicks).
-- MAGIC
-- MAGIC Enables segmentation questions like:
-- MAGIC - "Do top-rated freelancers click faster?"
-- MAGIC - "Which skill segments have the highest CTR?"
-- MAGIC - "How does hourly rate correlate with search behaviour?"

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_freelancer
COMMENT "Freelancer dimension — profile attributes linked to clickstream via visitor_id"
TBLPROPERTIES ("quality" = "gold")
AS SELECT
  visitor_id,
  name,
  country,
  primary_skill,
  hourly_rate,
  job_success_score,
  member_since,
  top_rated,
  is_verified
FROM LIVE.silver_freelancers;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## dim_date
-- MAGIC
-- MAGIC Date dimension derived from the distinct event dates in the impressions data.
-- MAGIC Contains calendar attributes that enable date-hierarchy drill-downs
-- MAGIC (year → quarter → month → day) without needing a pre-built date seed table.
-- MAGIC
-- MAGIC | Column | Example |
-- MAGIC |--------|---------|
-- MAGIC | `date_key` | `2024-03-15` |
-- MAGIC | `year` | `2024` |
-- MAGIC | `quarter` | `1` |
-- MAGIC | `month` | `3` |
-- MAGIC | `day_of_month` | `15` |
-- MAGIC | `day_of_week` | `6` (1=Sun, 7=Sat in Spark) |
-- MAGIC | `week_of_year` | `11` |
-- MAGIC | `is_weekend` | `true` |

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_date
COMMENT "Date dimension — one row per calendar date seen in the event data"
TBLPROPERTIES ("quality" = "gold")
AS
WITH distinct_dates AS (
  SELECT DISTINCT CAST(event_ts AS DATE) AS date_key
  FROM LIVE.silver_impression_events
)
SELECT
  date_key,
  YEAR(date_key)                     AS year,
  QUARTER(date_key)                  AS quarter,
  MONTH(date_key)                    AS month,
  DAYOFMONTH(date_key)               AS day_of_month,
  DAYOFWEEK(date_key)                AS day_of_week,
  WEEKOFYEAR(date_key)               AS week_of_year,
  DAYOFWEEK(date_key) IN (1, 7)      AS is_weekend
FROM distinct_dates
ORDER BY date_key;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## fact_search_events
-- MAGIC
-- MAGIC **Grain:** one row per impression event.
-- MAGIC
-- MAGIC The fact table is the single source of truth for the search funnel. It enriches
-- MAGIC every impression with the matching click outcome (if any) so a single scan
-- MAGIC gives you the full impression → click path.
-- MAGIC
-- MAGIC **Join logic:** LEFT JOIN `silver_click_events` on `(visitor_id, opening_uid, search_guid)`.
-- MAGIC If a matching click exists, `was_clicked = true` and `time_to_click_secs` is populated.
-- MAGIC
-- MAGIC **Foreign keys:**
-- MAGIC - `event_date` → `dim_date.date_key`
-- MAGIC - `opening_uid` → `dim_job.opening_uid`

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE fact_search_events
COMMENT "Search event fact table — one row per impression with click outcome attached"
TBLPROPERTIES ("quality" = "gold")
AS
WITH clicks_slim AS (
  SELECT
    visitor_id  AS clk_visitor_id,
    opening_uid AS clk_opening_uid,
    search_guid AS clk_search_guid,
    time_to_click_secs
  FROM LIVE.silver_click_events
  WHERE opening_uid IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY visitor_id, opening_uid, search_guid
    ORDER BY time_to_click_secs
  ) = 1
)
SELECT
  i.event_id,
  CAST(i.event_ts AS DATE)        AS event_date,
  i.opening_uid,
  i.visitor_id,
  i.search_guid,
  i.search_query,
  i.position,
  i.sublocation,
  i.event_ts,
  c.clk_visitor_id IS NOT NULL    AS was_clicked,
  c.time_to_click_secs
FROM LIVE.silver_impression_events i
LEFT JOIN clicks_slim c
  ON  i.visitor_id  = c.clk_visitor_id
  AND i.opening_uid = c.clk_opening_uid
  AND i.search_guid = c.clk_search_guid;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ---
-- MAGIC **Star schema complete.** Five tables are ready: four dimensions and one fact table.
-- MAGIC
-- MAGIC Next steps:
-- MAGIC - **Step 3 — Governance:** open `06_governance.sql` to apply RBAC, column masking and row-level security
-- MAGIC - **Step 4 — Exploration:** open `08_genie_setup.sql` to configure a Genie AI/BI space over these tables
-- MAGIC - **Step 5 — Insights:** open `08_insights_dashboard.sql` for the final analytics dashboard
