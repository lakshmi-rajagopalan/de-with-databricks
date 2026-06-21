-- Gold Layer — Business Metrics
-- Six materialized views answering product analytics questions about the FlexHire marketplace.
-- Each view reads from silver.* tables and produces aggregated, dashboard-ready metrics.

-- =============================================================================
-- Job Performance CTR
-- Per-job funnel: impressions, clicks, avg position, time-to-click distribution
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.job_performance_ctr
AS
WITH JOBS AS (
    SELECT * FROM silver.job_openings
), JOB_IMPRESSIONS AS (
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
FROM JOBS j
LEFT JOIN JOB_IMPRESSIONS ji ON j.opening_uid = ji.opening_uid
LEFT JOIN JOB_CLICKS jc      ON j.opening_uid = jc.opening_uid;


-- =============================================================================
-- Impression Position CTR
-- Position bias analysis: how CTR and click time vary by search result position
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.impression_position_ctr
AS
WITH JOB_IMPRESSIONS AS (
    SELECT
        position                AS page_position,
        count(event_id)         AS impressions
    FROM silver.impression_events
    GROUP BY page_position
), JOB_CLICKS AS (
    SELECT
        position                        AS page_position,
        avg(time_to_click_secs)         AS avg_click_time
    FROM silver.click_events
    GROUP BY page_position
)
SELECT
    ji.page_position,
    ji.impressions,
    jc.avg_click_time
FROM JOB_IMPRESSIONS ji
LEFT JOIN JOB_CLICKS jc ON ji.page_position = jc.page_position
ORDER BY page_position ASC;


-- =============================================================================
-- Search Query Performance
-- Per-query funnel: which search terms drive the most traffic and clicks
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.search_query_performance
AS
WITH SEARCHES AS (
    SELECT search_guid, search_query FROM silver.search_events
), IMP_AGG AS (
    SELECT
        s.search_query,
        count(i.event_id)                       AS impressions,
        approx_count_distinct(i.visitor_id)     AS unique_visitors,
        approx_count_distinct(i.opening_uid)    AS unique_jobs_shown,
        approx_count_distinct(i.search_guid)    AS search_sessions
    FROM silver.impression_events i
    LEFT JOIN SEARCHES s ON i.search_guid = s.search_guid
    GROUP BY s.search_query
), CLK_AGG AS (
    SELECT
        s.search_query,
        count(c.event_id)   AS clicks
    FROM silver.click_events c
    LEFT JOIN SEARCHES s ON c.search_guid = s.search_guid
    WHERE c.opening_uid IS NOT NULL
    GROUP BY s.search_query
)
SELECT
    i.search_query,
    i.impressions,
    i.unique_visitors,
    i.unique_jobs_shown,
    i.search_sessions,
    c.clicks,
    round(c.clicks / i.impressions * 100, 2) AS ctr_pct
FROM IMP_AGG i
LEFT JOIN CLK_AGG c ON i.search_query = c.search_query
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
        count(c.event_id)   AS clicks
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
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.sublocation_performance
AS
WITH IMP_AGG AS (
    SELECT
        s.sublocation,
        count(i.event_id)                       AS impressions,
        approx_count_distinct(i.visitor_id)     AS unique_visitors,
        approx_count_distinct(i.opening_uid)    AS unique_jobs,
        round(avg(i.position), 1)               AS avg_position
    FROM silver.impression_events i
    LEFT JOIN silver.search_events s ON i.search_guid = s.search_guid
    GROUP BY s.sublocation
), CLK_AGG AS (
    SELECT
        s.sublocation,
        count(c.event_id)                       AS clicks,
        round(avg(c.time_to_click_secs), 1)     AS avg_click_time
    FROM silver.click_events c
    LEFT JOIN silver.search_events s ON c.search_guid = s.search_guid
    WHERE c.opening_uid IS NOT NULL
    GROUP BY s.sublocation
)
SELECT
    ia.sublocation,
    ia.impressions,
    ia.unique_visitors,
    ia.unique_jobs,
    ia.avg_position,
    ca.clicks,
    ca.avg_click_time,
    round(ca.clicks / ia.impressions * 100, 2) AS ctr_pct
FROM IMP_AGG ia
LEFT JOIN CLK_AGG ca ON ia.sublocation = ca.sublocation
ORDER BY sublocation;


-- =============================================================================
-- Daily Metrics
-- Day-grain search funnel trend for dashboards and anomaly detection
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW gold.daily_metrics
AS
WITH IMP_DAILY AS (
    SELECT
        cast(date_trunc('day', s.event_ts) AS date)     AS event_date,
        count(i.event_id)                               AS impressions,
        approx_count_distinct(i.visitor_id)             AS unique_visitors,
        approx_count_distinct(i.search_guid)            AS search_sessions,
        approx_count_distinct(i.opening_uid)            AS unique_jobs_shown
    FROM silver.impression_events i
    LEFT JOIN silver.search_events s ON i.search_guid = s.search_guid
    GROUP BY event_date
), CLK_DAILY AS (
    SELECT
        cast(date_trunc('day', event_ts) AS date)   AS event_date,
        count(event_id)                             AS clicks,
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
