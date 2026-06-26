-- Databricks notebook source

-- COMMAND ----------
-- Unified metric view over silver.fact_search_funnel.
-- Joins freelancers, job_openings, and clients so Genie can answer questions
-- across the full clickstream funnel without requiring the analyst to write joins.

-- COMMAND ----------
CREATE OR REPLACE VIEW clickstream_dev.gold.mv_clickstream_analytics
WITH METRICS LANGUAGE YAML AS $$
version: 1.1

source: clickstream_dev.silver.fact_search_funnel

joins:
  - name: freelancers
    source: clickstream_dev.silver.freelancers
    "on": source.visitor_id = freelancers.visitor_id
  - name: job_openings
    source: clickstream_dev.silver.job_openings
    "on": source.opening_uid = job_openings.opening_uid
    joins:
      - name: clients
        source: clickstream_dev.silver.clients
        "on": job_openings.client_uid = clients.client_uid

dimensions:
  - name: search_guid
    expr: source.search_guid
    display_name: Search Guid

  - name: opening_uid
    expr: source.opening_uid
    display_name: Opening Uid

  - name: visitor_id
    expr: source.visitor_id
    display_name: Visitor Id

  - name: search_query
    expr: source.search_query
    display_name: Search Query

  - name: sublocation
    expr: source.sublocation
    display_name: Sublocation

  - name: search_ts
    expr: source.search_ts
    display_name: Search Ts

  - name: impression_id
    expr: source.impression_id
    display_name: Impression Id

  - name: position
    expr: source.position
    display_name: Position

  - name: click_id
    expr: source.click_id
    display_name: Click Id

  - name: time_to_click_secs
    expr: source.time_to_click_secs
    display_name: Time To Click Secs

  - name: name
    expr: freelancers.name

  - name: country
    expr: freelancers.country

  - name: primary_skill
    expr: freelancers.primary_skill

  - name: member_since
    expr: freelancers.member_since

  - name: top_rated
    expr: freelancers.top_rated

  - name: is_verified
    expr: freelancers.is_verified

  - name: numeric_hourly_rate
    expr: freelancers.numeric_hourly_rate

  - name: numeric_job_success_score
    expr: freelancers.numeric_job_success_score

  - name: title
    expr: job_openings.title

  - name: category
    expr: job_openings.category

  - name: budget_type
    expr: job_openings.budget_type

  - name: client_uid
    expr: job_openings.client_uid

  - name: posted_at
    expr: job_openings.posted_at

  - name: is_active
    expr: job_openings.is_active

  - name: numeric_budget_amount
    expr: job_openings.numeric_budget_amount

  - name: company_name
    expr: job_openings.clients.company_name

  - name: industry
    expr: job_openings.clients.industry

  - name: payment_verified
    expr: job_openings.clients.payment_verified

  - name: avg_rating
    expr: job_openings.clients.avg_rating

  - name: numeric_total_spend_usd
    expr: job_openings.clients.numeric_total_spend_usd

measures:
  - name: total_impressions
    expr: COUNT(source.impression_id)
    comment: "Total number of impressions — one row per job shown in a search result."
    display_name: Total Impressions
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
      hide_group_separator: false
      abbreviation: none
    synonyms:
      - Total Impressions
      - Impressions

  - name: CTR
    expr: "try_divide(COUNT(source.click_id), COUNT(source.impression_id))"
    comment: Click-through rate — clicks divided by impressions. Use to measure how effectively a job listing drives user engagement.
    display_name: CTR
    format:
      type: number
      decimal_places:
        type: exact
        places: 2
      hide_group_separator: false
      abbreviation: none
    synonyms:
      - Click-Through Rate
      - Click Rate
      - Engagement Rate
      - Search Effectiveness
$$;
