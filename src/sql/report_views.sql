-- Databricks notebook source

-- COMMAND ----------
-- MAGIC %md
-- MAGIC # Module 4 — Report Views
-- MAGIC
-- MAGIC Plain SQL views with business-logic CASE expressions and nested aggregations
-- MAGIC that can't be expressed in the metric view semantic model.
-- MAGIC `client_portfolio_summary` has been promoted to `mv_client_portfolio` in `metric_views.sql`.

-- COMMAND ----------
CREATE OR REPLACE VIEW flexhire.clickstream_workshop.client_posting_performance AS
SELECT
	job_openings.client_uid,
	COALESCE(client_profiles.company_name, job_openings.client_uid) AS company_name,
	job_openings.opening_uid,
	job_openings.title,
	job_openings.category,
	job_openings.budget_type,
	ROUND(job_openings.budget_amount, 2) AS budget_amount,
	job_openings.is_active,
	COALESCE(job_performance.impressions, 0) AS impressions,
	COALESCE(job_performance.clicks, 0) AS clicks,
	ROUND(COALESCE(job_performance.ctr_pct, 0), 2) AS ctr_pct,
	ROUND(job_performance.avg_impression_position, 1) AS avg_impression_position,
	ROUND(job_performance.avg_time_to_click_secs, 1) AS avg_time_to_click_secs,
	CASE
		WHEN COALESCE(job_performance.impressions, 0) >= 20 AND COALESCE(job_performance.ctr_pct, 0) < 2 THEN 'High visibility, low engagement'
		WHEN COALESCE(job_performance.impressions, 0) < 10 AND COALESCE(job_performance.ctr_pct, 0) >= 5 THEN 'Low visibility, strong engagement'
		WHEN job_openings.is_active AND COALESCE(job_performance.clicks, 0) = 0 THEN 'Active posting needs attention'
		ELSE 'Steady performance'
	END AS opportunity_segment,
	CASE
		WHEN COALESCE(job_performance.impressions, 0) >= 20 AND COALESCE(job_performance.ctr_pct, 0) < 2 THEN 'Refresh title or posting copy'
		WHEN COALESCE(job_performance.impressions, 0) < 10 AND COALESCE(job_performance.ctr_pct, 0) >= 5 THEN 'Increase visibility or distribution'
		WHEN job_openings.is_active AND COALESCE(job_performance.clicks, 0) = 0 THEN 'Review budget and targeting'
		ELSE 'Maintain current strategy'
	END AS recommended_action
FROM flexhire.clickstream_workshop.silver_job_openings AS job_openings
LEFT JOIN flexhire.clickstream_workshop.gold_job_performance AS job_performance
	USING (opening_uid)
LEFT JOIN flexhire.clickstream_workshop.silver_clients AS client_profiles
	USING (client_uid);

-- COMMAND ----------
CREATE OR REPLACE VIEW flexhire.clickstream_workshop.client_category_benchmark AS
SELECT
	client_category.client_uid,
	COALESCE(client_profiles.company_name, client_category.client_uid) AS company_name,
	client_category.category,
	client_category.client_jobs,
	client_category.client_active_jobs,
	client_category.client_impressions,
	client_category.client_clicks,
	ROUND(client_category.client_ctr_pct, 2) AS client_ctr_pct,
	ROUND(client_category.client_avg_budget_amount, 2) AS client_avg_budget_amount,
	marketplace_category.impressions AS marketplace_impressions,
	marketplace_category.clicks AS marketplace_clicks,
	ROUND(marketplace_category.ctr_pct, 2) AS marketplace_ctr_pct,
	ROUND(marketplace_category.avg_budget_amount, 2) AS marketplace_avg_budget_amount,
	ROUND(client_category.client_ctr_pct - marketplace_category.ctr_pct, 2) AS ctr_gap_pct,
	ROUND(client_category.client_avg_budget_amount - marketplace_category.avg_budget_amount, 2) AS budget_gap_amount
FROM (
	SELECT
		job_openings.client_uid,
		job_openings.category,
		COUNT(DISTINCT job_openings.opening_uid) AS client_jobs,
		SUM(CASE WHEN job_openings.is_active THEN 1 ELSE 0 END) AS client_active_jobs,
		SUM(COALESCE(job_performance.impressions, 0)) AS client_impressions,
		SUM(COALESCE(job_performance.clicks, 0)) AS client_clicks,
		SUM(COALESCE(job_performance.clicks, 0)) / NULLIF(SUM(COALESCE(job_performance.impressions, 0)), 0) * 100 AS client_ctr_pct,
		AVG(job_openings.budget_amount) AS client_avg_budget_amount
	FROM flexhire.clickstream_workshop.silver_job_openings AS job_openings
	LEFT JOIN flexhire.clickstream_workshop.gold_job_performance AS job_performance
		USING (opening_uid)
	GROUP BY job_openings.client_uid, job_openings.category
) AS client_category
LEFT JOIN flexhire.clickstream_workshop.gold_category_performance AS marketplace_category
	ON client_category.category = marketplace_category.category
LEFT JOIN flexhire.clickstream_workshop.silver_clients AS client_profiles
	ON client_category.client_uid = client_profiles.client_uid;

-- COMMAND ----------
CREATE OR REPLACE VIEW flexhire.clickstream_workshop.client_opportunities AS
SELECT
	posting.client_uid,
	posting.company_name,
	posting.opening_uid,
	posting.title,
	posting.category,
	posting.is_active,
	posting.impressions,
	posting.clicks,
	posting.ctr_pct,
	posting.budget_amount,
	benchmark.marketplace_ctr_pct,
	benchmark.marketplace_avg_budget_amount,
	CASE
		WHEN posting.impressions >= 20 AND posting.ctr_pct < COALESCE(benchmark.marketplace_ctr_pct, posting.ctr_pct) THEN 'Improve conversion'
		WHEN posting.impressions < 10 AND posting.ctr_pct >= COALESCE(benchmark.marketplace_ctr_pct, posting.ctr_pct) THEN 'Increase visibility'
		WHEN posting.budget_amount < COALESCE(benchmark.marketplace_avg_budget_amount, posting.budget_amount) THEN 'Review budget competitiveness'
		ELSE 'Monitor'
	END AS opportunity_type,
	posting.recommended_action
FROM flexhire.clickstream_workshop.client_posting_performance AS posting
LEFT JOIN flexhire.clickstream_workshop.client_category_benchmark AS benchmark
	ON posting.client_uid = benchmark.client_uid
 AND posting.category = benchmark.category
WHERE posting.is_active
ORDER BY posting.impressions DESC, posting.ctr_pct ASC;
