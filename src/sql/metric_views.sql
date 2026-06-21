-- Databricks notebook source

-- COMMAND ----------
CREATE OR REPLACE VIEW clickstream_dev.gold.mv_search_funnel
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
comment: "Daily search funnel — impressions, clicks, CTR"
source: clickstream_dev.silver.impression_events
joins:
  - name: searches
    source: clickstream_dev.silver.search_events
    using: [search_guid]
  - name: clicks
    source: clickstream_dev.silver.click_events
    using: [visitor_id, opening_uid, search_guid]
dimensions:
  - name: event_date
    expr: TO_DATE(searches.event_ts)
measures:
  - name: impressions
    expr: COUNT(1)
  - name: clicks
    expr: COUNT(clicks.event_id)
  - name: ctr_pct
    expr: COUNT(clicks.event_id) * 100.0 / COUNT(1)
$$;

-- COMMAND ----------
CREATE OR REPLACE VIEW clickstream_dev.gold.mv_position_bias
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
comment: "Click-through rate by impression position — position bias curve"
source: clickstream_dev.silver.impression_events
joins:
  - name: clicks
    source: clickstream_dev.silver.click_events
    using: [visitor_id, opening_uid, search_guid]
dimensions:
  - name: position
    expr: position
measures:
  - name: impressions
    expr: COUNT(1)
  - name: clicks
    expr: COUNT(clicks.event_id)
  - name: ctr_pct
    expr: COUNT(clicks.event_id) * 100.0 / COUNT(1)
  - name: avg_time_to_click_secs
    expr: AVG(clicks.time_to_click_secs)
$$;

-- COMMAND ----------
CREATE OR REPLACE VIEW clickstream_dev.gold.mv_category_performance
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
comment: "Per-category funnel and average budget"
source: clickstream_dev.silver.impression_events
joins:
  - name: job
    source: clickstream_dev.silver.job_openings
    using: [opening_uid]
  - name: clicks
    source: clickstream_dev.silver.click_events
    using: [visitor_id, opening_uid, search_guid]
dimensions:
  - name: category
    expr: job.category
measures:
  - name: impressions
    expr: COUNT(1)
  - name: clicks
    expr: COUNT(clicks.event_id)
  - name: ctr_pct
    expr: COUNT(clicks.event_id) * 100.0 / COUNT(1)
  - name: avg_budget_amount
    expr: AVG(job.numeric_budget_amount)
$$;

-- COMMAND ----------
CREATE OR REPLACE VIEW clickstream_dev.gold.mv_client_portfolio
WITH METRICS LANGUAGE YAML AS $$
version: 1.1
comment: "Per-client portfolio summary — job counts, funnel metrics, budget"
source: clickstream_dev.silver.job_openings
joins:
  - name: clients
    source: clickstream_dev.silver.clients
    using: [client_uid]
  - name: impressions
    source: clickstream_dev.silver.impression_events
    using: [opening_uid]
  - name: clicks
    source: clickstream_dev.silver.click_events
    using: [opening_uid]
dimensions:
  - name: client_uid
    expr: client_uid
  - name: company_name
    expr: COALESCE(clients.company_name, client_uid)
measures:
  - name: clients_tracked
    expr: COUNT(DISTINCT client_uid)
  - name: jobs_posted
    expr: COUNT(DISTINCT opening_uid)
  - name: active_jobs
    expr: SUM(CASE WHEN is_active THEN 1 ELSE 0 END)
  - name: total_impressions
    expr: COUNT(DISTINCT impressions.event_id)
  - name: total_clicks
    expr: COUNT(DISTINCT clicks.event_id)
  - name: ctr_pct
    expr: COUNT(DISTINCT clicks.event_id) * 100.0 / NULLIF(COUNT(DISTINCT impressions.event_id), 0)
  - name: avg_budget_amount
    expr: AVG(numeric_budget_amount)
  - name: unique_categories
    expr: COUNT(DISTINCT category)
$$;
