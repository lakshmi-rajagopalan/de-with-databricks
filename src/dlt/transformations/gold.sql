-- Gold Layer — Dimensions, Facts & Business Metrics
-- Star schema over the FlexHire marketplace clickstream.
-- Dimensions: dim_job_openings, dim_clients, dim_freelancers
-- Fact:       fact_search_funnel (impression grain)
-- Metrics:    pre-aggregated views for dashboard performance

-- =============================================================================
-- Dimension: Job Openings
-- Current state of every job posting — stable interface over silver.job_openings.
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.dim_job_openings
AS
SELECT
    opening_uid,
    client_uid,
    title,
    category,
    budget_type,
    numeric_budget_amount,
    is_active,
    posted_at
FROM silver.job_openings;


-- =============================================================================
-- Dimension: Clients
-- Current state of every client — stable interface over silver.clients.
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.dim_clients
AS
SELECT
    client_uid,
    company_name,
    industry,
    payment_verified,
    avg_rating,
    numeric_total_spend_usd
FROM silver.clients;


-- =============================================================================
-- Dimension: Freelancers
-- Current state of every freelancer — stable interface over silver.freelancers.
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.dim_freelancers
AS
SELECT
    visitor_id,
    name,
    country,
    primary_skill,
    member_since,
    top_rated,
    is_verified,
    numeric_hourly_rate,
    numeric_job_success_score
FROM silver.freelancers;


-- =============================================================================
-- Fact Search Funnel
-- Impression-grain fact table — one row per job shown in a search result.
-- Joins search context, impression, and click outcome into a single record.
-- This is the source for the metric view semantic layer and downstream aggregations.
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.fact_search_funnel
AS
SELECT
    s.search_guid,
    s.visitor_id,
    s.search_query,
    s.sublocation,
    s.ingest_ts                         AS search_ts,
    i.event_id                          AS impression_id,
    i.opening_uid,
    i.position,
    c.event_id                          AS click_id,
    c.time_to_click_secs
FROM silver.search_events s
LEFT JOIN silver.impression_events i ON s.search_guid = i.search_guid
LEFT JOIN silver.click_events c      ON i.search_guid = c.search_guid
                                     AND i.opening_uid = c.opening_uid;


-- =============================================================================
-- Job Performance CTR
-- Per-job funnel: impressions, clicks, avg position, time-to-click distribution
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.job_performance_ctr
AS
SELECT
    f.opening_uid,
    j.title,
    j.client_uid,
    count(f.impression_id)                                          AS impressions,
    approx_count_distinct(f.visitor_id)                             AS unique_impression_visitors,
    round(avg(f.position), 1)                                       AS avg_impression_position,
    count(f.click_id)                                               AS clicks,
    approx_count_distinct(CASE WHEN f.click_id IS NOT NULL THEN f.visitor_id END) AS unique_click_visitors,
    round(avg(f.time_to_click_secs), 1)                             AS avg_click_time,
    percentile(f.time_to_click_secs, array(0.25, 0.5, 0.75))       AS click_time_quantiles,
    round(count(f.click_id) / count(f.impression_id) * 100, 2)     AS ctr_pct
FROM gold.fact_search_funnel f
LEFT JOIN gold.dim_job_openings j ON f.opening_uid = j.opening_uid
GROUP BY f.opening_uid, j.title, j.client_uid;


-- =============================================================================
-- Impression Position CTR
-- Position bias analysis: how CTR and click time vary by search result position
-- Reads from gold.fact_search_funnel — join already resolved upstream
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.impression_position_ctr
AS
SELECT
    position                                                    AS page_position,
    count(impression_id)                                        AS impressions,
    count(click_id)                                             AS clicks,
    round(count(click_id) / count(impression_id) * 100, 2)     AS ctr_pct,
    round(avg(time_to_click_secs), 1)                           AS avg_click_time
FROM gold.fact_search_funnel
WHERE position IS NOT NULL
GROUP BY position
ORDER BY position ASC;


-- =============================================================================
-- Search Query Performance
-- Per-query funnel: which search terms drive the most traffic and clicks
-- Reads from gold.fact_search_funnel — join already resolved upstream
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
FROM gold.fact_search_funnel
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
        count(opening_uid)                          AS job_count,
        round(avg(numeric_budget_amount), 2)        AS avg_budget,
        sum(CASE WHEN is_active THEN 1 ELSE 0 END) AS active_jobs
    FROM gold.dim_job_openings
    GROUP BY category
), FUNNEL_AGG AS (
    SELECT
        j.category,
        count(f.impression_id)                      AS impressions,
        approx_count_distinct(f.visitor_id)         AS unique_visitors,
        count(f.click_id)                           AS clicks
    FROM gold.fact_search_funnel f
    LEFT JOIN gold.dim_job_openings j ON f.opening_uid = j.opening_uid
    GROUP BY j.category
)
SELECT
    ja.category,
    ja.job_count,
    ja.avg_budget,
    ja.active_jobs,
    fa.impressions,
    fa.unique_visitors,
    fa.clicks,
    round(fa.clicks / fa.impressions * 100, 2) AS ctr_pct
FROM JOB_AGG ja
LEFT JOIN FUNNEL_AGG fa ON ja.category = fa.category
ORDER BY impressions DESC;


-- =============================================================================
-- Sublocation Performance
-- Do featured job placements outperform organic search results?
-- Reads from gold.fact_search_funnel — join already resolved upstream
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
FROM gold.fact_search_funnel
WHERE sublocation IS NOT NULL
GROUP BY sublocation
ORDER BY sublocation;


-- =============================================================================
-- Daily Metrics
-- Day-grain search funnel trend for dashboards and anomaly detection
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.daily_metrics
AS
SELECT
    cast(date_trunc('day', search_ts) AS date)      AS event_date,
    count(impression_id)                            AS impressions,
    approx_count_distinct(visitor_id)               AS unique_visitors,
    approx_count_distinct(search_guid)              AS search_sessions,
    approx_count_distinct(opening_uid)              AS unique_jobs_shown,
    count(click_id)                                 AS clicks,
    round(avg(time_to_click_secs), 1)               AS avg_click_time,
    round(count(click_id) / count(impression_id) * 100, 2) AS ctr_pct
FROM gold.fact_search_funnel
GROUP BY event_date
ORDER BY event_date;


-- =============================================================================
-- Visitor Engagement
-- Per-visitor activity: impressions seen, searches made, unique jobs explored
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.visitor_engagement
AS
SELECT
    visitor_id,
    count(impression_id)                AS total_impressions,
    approx_count_distinct(search_guid)  AS total_searches,
    approx_count_distinct(opening_uid)  AS unique_jobs_seen
FROM gold.fact_search_funnel
GROUP BY visitor_id;


-- =============================================================================
-- Job Click Engagement
-- Per-job click depth: volume, unique clickers, and click speed distribution
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.job_click_engagement
AS
SELECT
    opening_uid,
    count(click_id)                                         AS total_clicks,
    approx_count_distinct(CASE WHEN click_id IS NOT NULL THEN visitor_id END) AS unique_clickers,
    round(avg(time_to_click_secs), 2)                       AS avg_time_to_click,
    percentile(time_to_click_secs, array(0.25, 0.5, 0.75)) AS click_time_quantiles
FROM gold.fact_search_funnel
WHERE click_id IS NOT NULL
GROUP BY opening_uid;


-- =============================================================================
-- Search to Click Conversion
-- Which search queries and sublocations drive actual clicks
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.search_to_click_conversion
AS
SELECT
    search_query,
    sublocation,
    count(DISTINCT search_guid)         AS searches_with_clicks,
    count(click_id)                     AS total_clicks,
    round(avg(time_to_click_secs), 2)   AS avg_time_to_click
FROM gold.fact_search_funnel
WHERE click_id IS NOT NULL
GROUP BY search_query, sublocation;


-- =============================================================================
-- Hourly Activity
-- Hour-of-day traffic pattern for capacity planning and anomaly detection
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.hourly_activity
AS
SELECT
    hour(search_ts)                     AS hour_of_day,
    count(impression_id)                AS impressions,
    approx_count_distinct(visitor_id)   AS unique_visitors
FROM gold.fact_search_funnel
GROUP BY hour(search_ts)
ORDER BY hour_of_day;
