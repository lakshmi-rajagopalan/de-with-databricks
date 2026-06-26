-- Gold Layer — Business Metrics
-- Six materialized views answering product analytics questions about the FlexHire marketplace.
-- Each view reads from silver.* tables and produces aggregated, dashboard-ready metrics.

-- =============================================================================
-- Job Performance CTR
-- Per-job funnel: impressions, clicks, avg position, time-to-click distribution
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.job_performance_ctr
AS
WITH JOB_IMPRESSIONS AS (
    SELECT
        opening_uid,
        count(event_id)                         AS impressions,
        approx_count_distinct(visitor_id)       AS unique_impression_visitors,
        avg(position)                           AS avg_impression_position
    FROM silver.impression_events
    GROUP BY opening_uid
), JOB_CLICKS AS (
    SELECT
        opening_uid,
        count(event_id)                                             AS clicks,
        approx_count_distinct(visitor_id)                           AS unique_click_visitors,
        avg(time_to_click_secs)                                     AS avg_click_time,
        percentile(time_to_click_secs, array(0.25, 0.5, 0.75))     AS click_time_quantiles
    FROM silver.click_events
    GROUP BY opening_uid
)
SELECT
    j.opening_uid,
    j.title,
    j.client_uid,
    ji.impressions,
    ji.unique_impression_visitors,
    ji.avg_impression_position,
    jc.clicks,
    jc.unique_click_visitors,
    jc.avg_click_time,
    jc.click_time_quantiles,
    round(jc.clicks / ji.impressions * 100, 2) AS ctr_pct
FROM silver.job_openings j
LEFT JOIN JOB_IMPRESSIONS ji ON j.opening_uid = ji.opening_uid
LEFT JOIN JOB_CLICKS jc      ON j.opening_uid = jc.opening_uid;


-- =============================================================================
-- Impression Position CTR
-- Position bias analysis: how CTR and click time vary by search result position
-- Reads from silver.fact_search_funnel — join already resolved upstream
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.impression_position_ctr
AS
SELECT
    position                                                    AS page_position,
    count(impression_id)                                        AS impressions,
    count(click_id)                                             AS clicks,
    round(count(click_id) / count(impression_id) * 100, 2)     AS ctr_pct,
    round(avg(time_to_click_secs), 1)                           AS avg_click_time
FROM silver.fact_search_funnel
WHERE position IS NOT NULL
GROUP BY position
ORDER BY position ASC;


-- =============================================================================
-- Search Query Performance
-- Per-query funnel: which search terms drive the most traffic and clicks
-- Reads from silver.fact_search_funnel — join already resolved upstream
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.search_query_performance
AS
SELECT
    search_query,
    count(impression_id)                        AS impressions,
    approx_count_distinct(visitor_id)           AS unique_visitors,
    approx_count_distinct(opening_uid)          AS unique_jobs_shown,
    approx_count_distinct(search_guid)          AS search_sessions,
    count(click_id)                             AS clicks,
    round(count(click_id) / count(impression_id) * 100, 2) AS ctr_pct
FROM silver.fact_search_funnel
WHERE search_query IS NOT NULL
GROUP BY search_query
ORDER BY impressions DESC;


-- =============================================================================
-- Category Performance
-- Per-category rollup: job count, impressions, clicks, CTR and avg budget
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.category_performance
AS
WITH JOB_AGG AS (
    SELECT
        category,
        count(opening_uid)                                          AS job_count,
        round(avg(numeric_budget_amount), 2)                        AS avg_budget,
        sum(CASE WHEN is_active THEN 1 ELSE 0 END)                 AS active_jobs
    FROM silver.job_openings
    GROUP BY category
), IMP_AGG AS (
    SELECT
        j.category,
        count(i.event_id)                       AS impressions,
        approx_count_distinct(i.visitor_id)     AS unique_visitors
    FROM silver.impression_events i
    LEFT JOIN silver.job_openings j ON i.opening_uid = j.opening_uid
    GROUP BY j.category
), CLK_AGG AS (
    SELECT
        j.category,
        coalesce(count(c.event_id), 0)   AS clicks
    FROM silver.click_events c
    LEFT JOIN silver.job_openings j ON c.opening_uid = j.opening_uid
    WHERE c.opening_uid IS NOT NULL
    GROUP BY j.category
)
SELECT
    ja.category,
    ja.job_count,
    ja.avg_budget,
    ja.active_jobs,
    ia.impressions,
    ia.unique_visitors,
    ca.clicks,
    round(ca.clicks / ia.impressions * 100, 2) AS ctr_pct
FROM JOB_AGG ja
LEFT JOIN IMP_AGG ia ON ja.category = ia.category
LEFT JOIN CLK_AGG ca ON ja.category = ca.category
ORDER BY impressions DESC;


-- =============================================================================
-- Sublocation Performance
-- Do featured job placements outperform organic search results?
-- Reads from silver.fact_search_funnel — join already resolved upstream
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.sublocation_performance
AS
SELECT
    sublocation,
    count(impression_id)                                        AS impressions,
    approx_count_distinct(visitor_id)                           AS unique_visitors,
    approx_count_distinct(opening_uid)                          AS unique_jobs,
    round(avg(position), 1)                                     AS avg_position,
    count(click_id)                                             AS clicks,
    round(avg(time_to_click_secs), 1)                           AS avg_click_time,
    round(count(click_id) / count(impression_id) * 100, 2)     AS ctr_pct
FROM silver.fact_search_funnel
WHERE sublocation IS NOT NULL
GROUP BY sublocation
ORDER BY sublocation;


-- =============================================================================
-- Daily Metrics
-- Day-grain search funnel trend for dashboards and anomaly detection
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.daily_metrics
AS
WITH IMP_DAILY AS (
    SELECT
        cast(date_trunc('day', i.event_ts) AS date)     AS event_date,
        count(i.event_id)                               AS impressions,
        approx_count_distinct(i.visitor_id)             AS unique_visitors,
        approx_count_distinct(i.search_guid)            AS search_sessions,
        approx_count_distinct(i.opening_uid)            AS unique_jobs_shown
    FROM silver.impression_events i
    GROUP BY event_date
), CLK_DAILY AS (
    SELECT
        cast(date_trunc('day', event_ts) AS date)   AS event_date,
        coalesce(count(event_id), 0)               AS clicks,
        round(avg(time_to_click_secs), 1)           AS avg_click_time
    FROM silver.click_events
    GROUP BY event_date
)
SELECT
    id.event_date,
    id.impressions,
    id.unique_visitors,
    id.search_sessions,
    id.unique_jobs_shown,
    cd.clicks,
    cd.avg_click_time,
    round(cd.clicks / id.impressions * 100, 2) AS ctr_pct
FROM IMP_DAILY id
LEFT JOIN CLK_DAILY cd ON id.event_date = cd.event_date
ORDER BY event_date;


-- =============================================================================
-- Visitor Engagement
-- Per-visitor activity: impressions seen, searches made, unique jobs explored
-- No joins — single table aggregation, guaranteed incremental with row tracking
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.visitor_engagement
AS
SELECT
    visitor_id,
    coalesce(count(event_id), 0)                     AS total_impressions,
    approx_count_distinct(search_guid)  AS total_searches,
    approx_count_distinct(opening_uid)  AS unique_jobs_seen
FROM silver.impression_events
GROUP BY visitor_id;


-- =============================================================================
-- Job Click Engagement
-- Per-job click depth: volume, unique clickers, and click speed distribution
-- No joins — single table aggregation, guaranteed incremental with row tracking
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.job_click_engagement
AS
SELECT
    opening_uid,
    coalesce(count(event_id), 0)                                         AS total_clicks,
    approx_count_distinct(visitor_id)                       AS unique_clickers,
    coalesce(round(avg(time_to_click_secs), 2), 0)                       AS avg_time_to_click,
    percentile(time_to_click_secs, array(0.25, 0.5, 0.75)) AS click_time_quantiles
FROM silver.click_events
GROUP BY opening_uid;


-- =============================================================================
-- Search to Click Conversion
-- Which search queries and sublocations drive actual clicks
-- INNER JOIN avoids retroactive-NULL problem of LEFT JOIN, safe for incremental
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.search_to_click_conversion
AS
SELECT
    s.search_query,
    s.sublocation,
    count(DISTINCT s.search_guid)       AS searches_with_clicks,
    count(c.event_id)                   AS total_clicks,
    round(avg(c.time_to_click_secs), 2) AS avg_time_to_click
FROM silver.search_events s
INNER JOIN silver.click_events c ON s.search_guid = c.search_guid
GROUP BY s.search_query, s.sublocation;


-- =============================================================================
-- Hourly Activity
-- Hour-of-day traffic pattern for capacity planning and anomaly detection
-- No joins — single table aggregation, guaranteed incremental with row tracking
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.hourly_activity
AS
SELECT
    hour(event_ts)                      AS hour_of_day,
    count(event_id)                     AS impressions,
    approx_count_distinct(visitor_id)   AS unique_visitors
FROM silver.impression_events
GROUP BY hour(event_ts)
ORDER BY hour_of_day;
