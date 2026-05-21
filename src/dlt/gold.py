# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Layer — Business Metrics
# MAGIC
# MAGIC **Purpose:** Produce aggregated, denormalised tables optimised for dashboards and reporting.
# MAGIC Each gold table answers a specific product analytics question about the Upwork job marketplace.
# MAGIC
# MAGIC | Table | Business question |
# MAGIC |-------|------------------|
# MAGIC | `gold_job_performance` | Which jobs get the most impressions and clicks? |
# MAGIC | `gold_search_query_performance` | Which search queries drive the most traffic? |
# MAGIC | `gold_position_ctr` | How does click-through rate vary by position? (position bias) |
# MAGIC | `gold_category_performance` | Which job categories perform best? |
# MAGIC | `gold_sublocation_performance` | Do featured jobs outperform organic search results? |
# MAGIC | `gold_daily_metrics` | What does the day-by-day search funnel look like? |
# MAGIC
# MAGIC **DLT concepts covered:**
# MAGIC - `dp.read()` to join across silver tables in the same pipeline
# MAGIC - CTR funnel (impressions → clicks)
# MAGIC - Window functions for ranking within groups
# MAGIC - Derived KPIs: CTR, avg time-to-click, impression-to-click latency

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, count, countDistinct, sum as _sum, avg, round as _round,
    when, date_trunc, min as _min, max as _max, first,
    percentile_approx,
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## gold_job_performance
# MAGIC
# MAGIC One row per job opening. Joins impressions and clicks onto job metadata to
# MAGIC produce the full funnel: how many times each job was shown vs clicked.
# MAGIC
# MAGIC **Key metrics:** `impressions`, `clicks`, `ctr_pct`, `avg_position`, `avg_time_to_click_secs`

# COMMAND ----------

@dp.table(
    name="gold_job_performance",
    comment="Per-job funnel: impressions, clicks, CTR and average position",
    table_properties={"quality": "gold"},
)
@dp.expect("clicks_le_impressions", "clicks <= impressions OR clicks IS NULL")
def gold_job_performance():
    jobs        = dp.read("silver_job_openings")
    impressions = dp.read("silver_impression_events")
    clicks      = dp.read("silver_click_events")

    imp_agg = (
        impressions
        .groupBy("opening_uid")
        .agg(
            count("event_id").alias("impressions"),
            countDistinct("visitor_id").alias("unique_visitors_shown"),
            _round(avg("position"), 1).alias("avg_impression_position"),
        )
    )

    clk_agg = (
        clicks
        .filter(col("opening_uid").isNotNull())
        .groupBy("opening_uid")
        .agg(
            count("event_id").alias("clicks"),
            countDistinct("visitor_id").alias("unique_visitors_clicked"),
            _round(avg("time_to_click_secs"), 1).alias("avg_time_to_click_secs"),
            _round(percentile_approx("time_to_click_secs", 0.5), 1).alias("median_time_to_click_secs"),
        )
    )

    return (
        jobs
        .join(imp_agg, "opening_uid", "left")
        .join(clk_agg, "opening_uid", "left")
        .select(
            col("opening_uid"),
            col("title"),
            col("category"),
            col("budget_type"),
            col("budget_amount"),
            col("client_uid"),
            col("posted_at"),
            col("is_active"),
            col("impressions"),
            col("unique_visitors_shown"),
            col("avg_impression_position"),
            col("clicks"),
            col("unique_visitors_clicked"),
            _round(
                col("clicks") / col("impressions") * 100, 2
            ).alias("ctr_pct"),
            col("avg_time_to_click_secs"),
            col("median_time_to_click_secs"),
        )
        .orderBy(col("impressions").desc())
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## gold_search_query_performance
# MAGIC
# MAGIC One row per unique search query. Shows which queries drive the most traffic
# MAGIC and which convert best into clicks.

# COMMAND ----------

@dp.table(
    name="gold_search_query_performance",
    comment="Per-query funnel: impressions, clicks and CTR for each search term",
    table_properties={"quality": "gold"},
)
def gold_search_query_performance():
    impressions = dp.read("silver_impression_events")
    searches    = dp.read("silver_search_events")
    clicks      = dp.read("silver_click_events")

    # Enrich impressions with search_query from silver_search_events
    imp_with_query = (
        impressions
        .join(searches.select("search_guid", "search_query"), "search_guid", "left")
    )

    imp_agg = (
        imp_with_query
        .groupBy("search_query")
        .agg(
            count("event_id").alias("impressions"),
            countDistinct("visitor_id").alias("unique_visitors"),
            countDistinct("opening_uid").alias("unique_jobs_shown"),
            countDistinct("search_guid").alias("search_sessions"),
        )
    )

    clk_agg = (
        clicks
        .filter(col("opening_uid").isNotNull())
        .join(searches.select("search_guid", "search_query").distinct(), "search_guid", "left")
        .groupBy("search_query")
        .agg(count("event_id").alias("clicks"))
    )

    return (
        imp_agg
        .join(clk_agg, "search_query", "left")
        .withColumn("ctr_pct", _round(col("clicks") / col("impressions") * 100, 2))
        .orderBy(col("impressions").desc())
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## gold_position_ctr
# MAGIC
# MAGIC One row per search result position (1, 2, 3, …).
# MAGIC Classic **position bias** analysis: jobs shown higher on the page get more clicks
# MAGIC simply because of where they appear — this table quantifies that effect.
# MAGIC
# MAGIC Use a line chart with position on X and CTR on Y to visualise the decay curve.

# COMMAND ----------

@dp.table(
    name="gold_position_ctr",
    comment="CTR by search result position — used to analyse and correct for position bias",
    table_properties={"quality": "gold"},
)
def gold_position_ctr():
    impressions = dp.read("silver_impression_events")
    clicks      = dp.read("silver_click_events")

    imp_pos = (
        impressions
        .groupBy("position")
        .agg(count("event_id").alias("impressions"))
    )

    clk_pos = (
        clicks
        .filter(col("opening_uid").isNotNull())
        .groupBy("position")
        .agg(
            count("event_id").alias("clicks"),
            _round(avg("time_to_click_secs"), 1).alias("avg_time_to_click_secs"),
        )
    )

    return (
        imp_pos
        .join(clk_pos, "position", "left")
        .withColumn("ctr_pct", _round(col("clicks") / col("impressions") * 100, 2))
        .orderBy("position")
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## gold_category_performance
# MAGIC
# MAGIC One row per job category. Aggregates job count, impressions, clicks and
# MAGIC average budget to compare how different verticals perform on the platform.

# COMMAND ----------

@dp.table(
    name="gold_category_performance",
    comment="Job category rollup: job count, impressions, clicks, CTR and average budget",
    table_properties={"quality": "gold"},
)
def gold_category_performance():
    jobs        = dp.read("silver_job_openings")
    impressions = dp.read("silver_impression_events")
    clicks      = dp.read("silver_click_events")

    # Attach category to impressions via opening_uid
    imp_with_cat = impressions.join(jobs.select("opening_uid", "category"), "opening_uid", "left")
    clk_with_cat = (
        clicks
        .filter(col("opening_uid").isNotNull())
        .join(jobs.select("opening_uid", "category"), "opening_uid", "left")
    )

    job_agg = (
        jobs
        .groupBy("category")
        .agg(
            count("opening_uid").alias("job_count"),
            _round(avg("budget_amount"), 2).alias("avg_budget_amount"),
            _sum(when(col("is_active"), 1).otherwise(0)).alias("active_jobs"),
        )
    )

    imp_agg = (
        imp_with_cat
        .groupBy("category")
        .agg(
            count("event_id").alias("impressions"),
            countDistinct("visitor_id").alias("unique_visitors"),
        )
    )

    clk_agg = (
        clk_with_cat
        .groupBy("category")
        .agg(count("event_id").alias("clicks"))
    )

    return (
        job_agg
        .join(imp_agg, "category", "left")
        .join(clk_agg, "category", "left")
        .withColumn("ctr_pct", _round(col("clicks") / col("impressions") * 100, 2))
        .orderBy(col("impressions").desc())
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## gold_sublocation_performance
# MAGIC
# MAGIC One row per sublocation (`search_results` vs `featured_jobs`).
# MAGIC Answers: do featured job placements actually drive higher CTR than organic listings?

# COMMAND ----------

@dp.table(
    name="gold_sublocation_performance",
    comment="Sublocation comparison: search_results vs featured_jobs CTR and engagement",
    table_properties={"quality": "gold"},
)
def gold_sublocation_performance():
    impressions = dp.read("silver_impression_events")
    searches    = dp.read("silver_search_events")
    clicks      = dp.read("silver_click_events")

    # Enrich impressions with sublocation from silver_search_events
    imp_with_sub = (
        impressions
        .join(searches.select("search_guid", "sublocation"), "search_guid", "left")
    )

    # Annotate clicks with sublocation via search_guid
    clk_annotated = (
        clicks
        .filter(col("opening_uid").isNotNull())
        .join(
            searches.select("search_guid", "sublocation").distinct(),
            "search_guid",
            "left",
        )
    )

    imp_agg = (
        imp_with_sub
        .groupBy("sublocation")
        .agg(
            count("event_id").alias("impressions"),
            countDistinct("visitor_id").alias("unique_visitors"),
            countDistinct("opening_uid").alias("unique_jobs"),
            _round(avg("position"), 1).alias("avg_position"),
        )
    )

    clk_agg = (
        clk_annotated
        .groupBy("sublocation")
        .agg(
            count("event_id").alias("clicks"),
            _round(avg("time_to_click_secs"), 1).alias("avg_time_to_click_secs"),
        )
    )

    return (
        imp_agg
        .join(clk_agg, "sublocation", "left")
        .withColumn("ctr_pct", _round(col("clicks") / col("impressions") * 100, 2))
        .orderBy("sublocation")
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## gold_daily_metrics
# MAGIC
# MAGIC Day-grain time series of the full search funnel.
# MAGIC Used for trend charts and anomaly detection in the dashboard.

# COMMAND ----------

@dp.table(
    name="gold_daily_metrics",
    comment="Daily search funnel trend: impressions, clicks, CTR and unique visitor counts",
    table_properties={"quality": "gold"},
)
@dp.expect("positive_ctr", "ctr_pct IS NULL OR (ctr_pct >= 0 AND ctr_pct <= 100)")
def gold_daily_metrics():
    impressions = dp.read("silver_impression_events")
    searches    = dp.read("silver_search_events")
    clicks      = dp.read("silver_click_events")

    imp_daily = (
        impressions
        .join(searches.select("search_guid", "event_ts"), "search_guid", "left")
        .withColumn("event_date", date_trunc("day", col("event_ts")).cast("date"))
        .groupBy("event_date")
        .agg(
            count("event_id").alias("impressions"),
            countDistinct("visitor_id").alias("unique_visitors"),
            countDistinct("search_guid").alias("search_sessions"),
            countDistinct("opening_uid").alias("unique_jobs_shown"),
        )
    )

    clk_daily = (
        clicks
        .withColumn("event_date", date_trunc("day", col("event_ts")).cast("date"))
        .groupBy("event_date")
        .agg(
            count("event_id").alias("clicks"),
            _round(avg("time_to_click_secs"), 1).alias("avg_time_to_click_secs"),
        )
    )

    return (
        imp_daily
        .join(clk_daily, "event_date", "left")
        .withColumn("ctr_pct", _round(col("clicks") / col("impressions") * 100, 2))
        .orderBy("event_date")
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC **Gold complete.** Six business metric tables are ready.
# MAGIC Open `04_dashboard_queries.sql` to build the dashboard.
