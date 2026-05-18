-- Databricks notebook source

-- COMMAND ----------
-- MAGIC %md
-- MAGIC # Insights Dashboard — Upwork Clickstream
-- MAGIC
-- MAGIC **Step 5 of the workshop.** Business metrics across the search funnel — impressions,
-- MAGIC clicks, CTR, position bias and audience behaviour. Each query maps to one dashboard tile.
-- MAGIC
-- MAGIC Run in a SQL editor after the DLT pipeline completes, then use
-- MAGIC **Save to dashboard** on each result to build the `Clickstream Insights` dashboard.
-- MAGIC
-- MAGIC Queries 1–12 use the pre-aggregated gold tables (fastest for dashboards).
-- MAGIC Query 13 uses the `fact_search_events` star schema table for session-level detail.

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 1 · Funnel Summary
-- MAGIC **Tile type:** Counter tiles — Total Impressions, Total Clicks, Overall CTR

-- COMMAND ----------

SELECT
    SUM(impressions)                                    AS total_impressions,
    SUM(clicks)                                         AS total_clicks,
    ROUND(SUM(clicks) / SUM(impressions) * 100, 2)     AS overall_ctr_pct,
    COUNT(DISTINCT opening_uid)                         AS jobs_in_results,
    MAX(posted_at)                                      AS latest_job_posted
FROM workspace.de.gold_job_performance;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 2 · Top 10 Jobs by Impressions
-- MAGIC **Tile type:** Table

-- COMMAND ----------

SELECT
    title,
    category,
    budget_type,
    budget_amount,
    impressions,
    clicks,
    ctr_pct,
    avg_impression_position,
    avg_time_to_click_secs
FROM workspace.de.gold_job_performance
WHERE impressions IS NOT NULL
ORDER BY impressions DESC
LIMIT 10;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 3 · Top 10 Jobs by CTR
-- MAGIC **Tile type:** Table

-- COMMAND ----------

SELECT
    title,
    category,
    budget_type,
    budget_amount,
    impressions,
    clicks,
    ctr_pct,
    avg_impression_position
FROM workspace.de.gold_job_performance
WHERE impressions >= 5
ORDER BY ctr_pct DESC
LIMIT 10;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 4 · Position Bias Curve
-- MAGIC **Tile type:** Line chart — X: position, Y: ctr_pct

-- COMMAND ----------

SELECT
    position,
    impressions,
    clicks,
    ctr_pct,
    avg_time_to_click_secs
FROM workspace.de.gold_position_ctr
WHERE position <= 20
ORDER BY position;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 5 · Category Performance
-- MAGIC **Tile type:** Horizontal bar chart — Category on Y, ctr_pct on X

-- COMMAND ----------

SELECT
    category,
    job_count,
    active_jobs,
    ROUND(avg_budget_amount, 0) AS avg_budget_amount,
    impressions,
    unique_visitors,
    clicks,
    ctr_pct
FROM workspace.de.gold_category_performance
WHERE category IS NOT NULL
ORDER BY impressions DESC;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 6 · Category CTR vs Average Budget
-- MAGIC **Tile type:** Scatter plot — X: avg_budget_amount, Y: ctr_pct, label: category

-- COMMAND ----------

SELECT
    category,
    ROUND(avg_budget_amount, 0) AS avg_budget_amount,
    ctr_pct,
    impressions
FROM workspace.de.gold_category_performance
WHERE category IS NOT NULL
  AND avg_budget_amount IS NOT NULL;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 7 · Search Query Performance
-- MAGIC **Tile type:** Table

-- COMMAND ----------

SELECT
    search_query,
    search_sessions,
    unique_visitors,
    unique_jobs_shown,
    impressions,
    clicks,
    ctr_pct
FROM workspace.de.gold_search_query_performance
WHERE search_query IS NOT NULL
ORDER BY impressions DESC
LIMIT 20;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 8 · Featured Jobs vs Search Results
-- MAGIC **Tile type:** Side-by-side bar chart

-- COMMAND ----------

SELECT
    sublocation,
    impressions,
    unique_visitors,
    unique_jobs,
    avg_position,
    clicks,
    ctr_pct,
    avg_time_to_click_secs
FROM workspace.de.gold_sublocation_performance
ORDER BY sublocation;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 9 · Daily Trend
-- MAGIC **Tile type:** Line chart — X: event_date, Y1: impressions, Y2: clicks

-- COMMAND ----------

SELECT
    event_date,
    impressions,
    unique_visitors,
    search_sessions,
    unique_jobs_shown,
    clicks,
    ctr_pct,
    avg_time_to_click_secs
FROM workspace.de.gold_daily_metrics
ORDER BY event_date;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 10 · Time-to-Click Distribution
-- MAGIC **Tile type:** Bar chart — bucket on X, clicks on Y

-- COMMAND ----------

SELECT
    CASE
        WHEN time_to_click_secs IS NULL  THEN 'unknown'
        WHEN time_to_click_secs < 3      THEN '< 3s (instant)'
        WHEN time_to_click_secs < 10     THEN '3-10s'
        WHEN time_to_click_secs < 30     THEN '10-30s'
        WHEN time_to_click_secs < 60     THEN '30-60s'
        ELSE '> 60s'
    END                               AS latency_bucket,
    COUNT(*)                          AS clicks,
    ROUND(AVG(time_to_click_secs), 1) AS avg_secs
FROM workspace.de.silver_click_events
GROUP BY latency_bucket
ORDER BY
    CASE latency_bucket
        WHEN '< 3s (instant)' THEN 1
        WHEN '3-10s'          THEN 2
        WHEN '10-30s'         THEN 3
        WHEN '30-60s'         THEN 4
        WHEN '> 60s'          THEN 5
        ELSE 6
    END;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 11 · Budget Type Distribution
-- MAGIC **Tile type:** Pie or donut chart

-- COMMAND ----------

SELECT
    budget_type,
    COUNT(*)                                         AS job_count,
    ROUND(AVG(budget_amount), 2)                    AS avg_budget_amount,
    SUM(impressions)                                AS total_impressions,
    ROUND(SUM(clicks) / SUM(impressions) * 100, 2)  AS ctr_pct
FROM workspace.de.gold_job_performance
WHERE budget_type IS NOT NULL
GROUP BY budget_type
ORDER BY job_count DESC;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 12 · Client Activity Summary
-- MAGIC **Tile type:** Table — top clients by impressions driven

-- COMMAND ----------

SELECT
    j.client_uid,
    COUNT(DISTINCT j.opening_uid)                        AS jobs_posted,
    SUM(gp.impressions)                                  AS total_impressions,
    SUM(gp.clicks)                                       AS total_clicks,
    ROUND(SUM(gp.clicks) / SUM(gp.impressions) * 100, 2) AS avg_ctr_pct,
    ROUND(AVG(j.budget_amount), 2)                       AS avg_budget_amount
FROM workspace.de.silver_job_openings j
LEFT JOIN workspace.de.gold_job_performance gp USING (opening_uid)
GROUP BY j.client_uid
ORDER BY total_impressions DESC
LIMIT 15;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 13 · Session-Level Funnel (Star Schema)
-- MAGIC
-- MAGIC Uses the `fact_search_events` table from the star schema (step 1).
-- MAGIC Shows, for each search session, how many impressions were served and whether
-- MAGIC any of them resulted in a click — the session-grain conversion funnel.
-- MAGIC
-- MAGIC **Tile type:** Bar chart — X: impressions_in_session bucket, Y: sessions

-- COMMAND ----------

SELECT
    CASE
        WHEN impressions_in_session = 1  THEN '1 impression'
        WHEN impressions_in_session <= 3 THEN '2-3 impressions'
        WHEN impressions_in_session <= 5 THEN '4-5 impressions'
        WHEN impressions_in_session <= 10 THEN '6-10 impressions'
        ELSE '> 10 impressions'
    END                                                      AS session_size,
    COUNT(DISTINCT search_guid)                              AS sessions,
    SUM(CASE WHEN any_clicked THEN 1 ELSE 0 END)            AS sessions_with_click,
    ROUND(
        SUM(CASE WHEN any_clicked THEN 1 ELSE 0 END) /
        COUNT(DISTINCT search_guid) * 100, 1
    )                                                        AS session_conversion_pct
FROM (
    SELECT
        search_guid,
        COUNT(*)                      AS impressions_in_session,
        MAX(CAST(was_clicked AS INT)) = 1 AS any_clicked
    FROM workspace.de.fact_search_events
    GROUP BY search_guid
) sessions
GROUP BY session_size
ORDER BY sessions DESC;
