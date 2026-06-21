# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Star Schema — Dimensional Model
# MAGIC
# MAGIC **Purpose:** Restructure the silver layer into an explicit star schema suitable for
# MAGIC self-service analytics, BI tools, and Genie (AI/BI natural language queries).
# MAGIC
# MAGIC ## Schema diagram
# MAGIC
# MAGIC ```
# MAGIC  dim_freelancer   dim_date   dim_search
# MAGIC  visitor_id          │       search_guid
# MAGIC       │           event_date      │
# MAGIC       │               │           │
# MAGIC  dim_client ─── dim_job ─── fact_search_events ─── dim_category
# MAGIC  client_uid    opening_uid   (one row per impression)
# MAGIC ```
# MAGIC
# MAGIC | Table | Grain | Role |
# MAGIC |-------|-------|------|
# MAGIC | `dim_job` | One row per job opening | Dimension |
# MAGIC | `dim_client` | One row per client (enriched from clients.csv) | Dimension |
# MAGIC | `dim_freelancer` | One row per freelancer (from freelancers.csv) | Dimension |
# MAGIC | `dim_category` | One row per job category | Dimension |
# MAGIC | `dim_date` | One row per calendar date | Date dimension |
# MAGIC | `dim_search` | One row per search session | Dimension |
# MAGIC | `fact_search_events` | One row per impression event | Fact |
# MAGIC
# MAGIC **DLT concepts covered:**
# MAGIC - `dp.read()` to reference silver tables within the pipeline
# MAGIC - Deriving a date spine from event data (no external seed needed)
# MAGIC - LEFT JOIN to enrich the fact table with click outcome in a single pass
# MAGIC - Enriching dimensions by joining multiple silver sources

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, count, countDistinct, avg, round as _round,
    min as _min, max as _max,
    year, month, dayofmonth, dayofweek, weekofyear, quarter,
    when, lit, to_date, date_trunc,
)
from pyspark.sql.types import BooleanType

# COMMAND ----------
# MAGIC %md
# MAGIC ## dim_job
# MAGIC
# MAGIC One row per job opening. Carries all descriptive attributes about the job so
# MAGIC downstream queries never need to join back to silver.job_openings.

# COMMAND ----------

@dp.table(
    name="dim_job",
    comment="Job dimension — one row per opening with all descriptive attributes",
    table_properties={"quality": "gold"},
)
def dim_job():
    return (
        dp.read("silver.job_openings")
        .select(
            col("opening_uid"),
            col("title"),
            col("category"),
            col("budget_type"),
            col("numeric_budget_amount"),
            col("client_uid"),
            col("posted_at"),
            col("is_active"),
        )
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## dim_client
# MAGIC
# MAGIC One row per client. Sourced from `silver.clients` (the authoritative client list)
# MAGIC and enriched with posting activity (job count, first/last post date) derived from
# MAGIC `silver.job_openings` via a LEFT JOIN.
# MAGIC
# MAGIC Clients with no postings still appear here; their activity columns will be null.

# COMMAND ----------

@dp.table(
    name="dim_client",
    comment="Client dimension — job posting activity enriched with company profile",
    table_properties={"quality": "gold"},
)
def dim_client():
    profile = dp.read("silver.clients").select(
        "client_uid", "company_name", "country", "industry",
        "payment_verified", "numeric_total_spend_usd", "avg_rating",
    )
    activity = (
        dp.read("silver.job_openings")
        .groupBy("client_uid")
        .agg(
            count("opening_uid").alias("jobs_posted"),
            _min("posted_at").alias("first_posted_at"),
            _max("posted_at").alias("latest_posted_at"),
        )
    )
    return profile.join(activity, "client_uid", "left")

# COMMAND ----------
# MAGIC %md
# MAGIC ## dim_category
# MAGIC
# MAGIC One row per job category with summary statistics.
# MAGIC Acts as a conformed dimension shared between the star schema and the gold
# MAGIC aggregation tables.

# COMMAND ----------

@dp.table(
    name="dim_category",
    comment="Category dimension — one row per category with job count and average budget",
    table_properties={"quality": "gold"},
)
def dim_category():
    return (
        dp.read("silver.job_openings")
        .groupBy("category")
        .agg(
            count("opening_uid").alias("total_jobs"),
            _round(avg("numeric_budget_amount"), 2).alias("avg_budget_amount"),
        )
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## dim_freelancer
# MAGIC
# MAGIC One row per freelancer, keyed on `visitor_id` which links to both
# MAGIC `fact_search_events` (impressions) and `silver.click_events` (clicks).
# MAGIC
# MAGIC Enables segmentation questions like:
# MAGIC - "Do top-rated freelancers click faster?"
# MAGIC - "Which skill segments have the highest CTR?"
# MAGIC - "How does hourly rate correlate with search behaviour?"

# COMMAND ----------

@dp.table(
    name="dim_freelancer",
    comment="Freelancer dimension — profile attributes linked to clickstream via visitor_id",
    table_properties={"quality": "gold"},
)
def dim_freelancer():
    return (
        dp.read("silver.freelancers")
        .select(
            col("visitor_id"),
            col("name"),
            col("country"),
            col("primary_skill"),
            col("numeric_hourly_rate"),
            col("numeric_job_success_score"),
            col("member_since"),
            col("top_rated"),
            col("is_verified"),
        )
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## dim_date
# MAGIC
# MAGIC Date dimension derived from the distinct event dates in the impressions data.
# MAGIC Contains calendar attributes that enable date-hierarchy drill-downs
# MAGIC (year → quarter → month → day) without needing a pre-built date seed table.
# MAGIC
# MAGIC | Column | Example |
# MAGIC |--------|---------|
# MAGIC | `date_key` | `2024-03-15` |
# MAGIC | `year` | `2024` |
# MAGIC | `quarter` | `1` |
# MAGIC | `month` | `3` |
# MAGIC | `day_of_month` | `15` |
# MAGIC | `day_of_week` | `6` (1=Sun, 7=Sat in Spark) |
# MAGIC | `week_of_year` | `11` |
# MAGIC | `is_weekend` | `true` |

# COMMAND ----------

@dp.table(
    name="dim_date",
    comment="Date dimension — one row per calendar date seen in the event data",
    table_properties={"quality": "gold"},
)
def dim_date():
    dates = (
        dp.read("silver.search_events")
        .select(to_date(col("event_ts")).alias("date_key"))
        .distinct()
    )
    return (
        dates
        .withColumn("year",         year(col("date_key")))
        .withColumn("quarter",      quarter(col("date_key")))
        .withColumn("month",        month(col("date_key")))
        .withColumn("day_of_month", dayofmonth(col("date_key")))
        .withColumn("day_of_week",  dayofweek(col("date_key")))
        .withColumn("week_of_year", weekofyear(col("date_key")))
        .withColumn("is_weekend",   dayofweek(col("date_key")).isin(1, 7))
        .orderBy("date_key")
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## dim_search
# MAGIC
# MAGIC One row per search session. Carries the full query context — the search term,
# MAGIC sublocation, sort order, and page — that drove the set of impressions with the
# MAGIC same `search_guid`.

# COMMAND ----------

@dp.table(
    name="dim_search",
    comment="Search session dimension — one row per search with query context",
    table_properties={"quality": "gold"},
)
def dim_search():
    return (
        dp.read("silver.search_events")
        .select(
            col("search_guid"),
            col("visitor_id"),
            col("search_query"),
            col("sublocation"),
            col("sort"),
            col("page"),
            col("event_ts"),
        )
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## fact_search_events
# MAGIC
# MAGIC **Grain:** one row per impression event.
# MAGIC
# MAGIC The fact table is the single source of truth for the search funnel. It enriches
# MAGIC every impression with the matching click outcome (if any) so a single scan
# MAGIC gives you the full impression → click path.
# MAGIC
# MAGIC **Join logic:** LEFT JOIN `silver.click_events` on `(visitor_id, opening_uid, search_guid)`.
# MAGIC If a matching click exists, `was_clicked = true` and `time_to_click_secs` is populated.
# MAGIC
# MAGIC **Foreign keys:**
# MAGIC - `event_date` → `dim_date.date_key`
# MAGIC - `opening_uid` → `dim_job.opening_uid`

# COMMAND ----------

@dp.table(
    name="fact_search_events",
    comment="Search event fact table — one row per impression with click outcome attached",
    table_properties={"quality": "gold"},
)
def fact_search_events():
    impressions = dp.read("silver.impression_events")
    clicks      = dp.read("silver.click_events")
    searches    = dp.read("silver.search_events")

    # Select only the click columns we need to avoid column-name collisions after join
    clicks_slim = (
        clicks
        .filter(col("opening_uid").isNotNull())
        .select(
            col("visitor_id").alias("clk_visitor_id"),
            col("opening_uid").alias("clk_opening_uid"),
            col("search_guid").alias("clk_search_guid"),
            col("time_to_click_secs"),
        )
        .dropDuplicates(["clk_visitor_id", "clk_opening_uid", "clk_search_guid"])
    )

    return (
        impressions
        .join(searches, "search_guid", "left")
        .join(
            clicks_slim,
            (impressions.visitor_id  == clicks_slim.clk_visitor_id)
            & (impressions.opening_uid == clicks_slim.clk_opening_uid)
            & (impressions.search_guid == clicks_slim.clk_search_guid),
            "left",
        )
        .select(
            impressions.event_id,
            to_date(searches.event_ts).alias("event_date"),
            impressions.opening_uid,
            impressions.visitor_id,
            impressions.search_guid,
            searches.search_query,
            impressions.position,
            searches.sublocation,
            searches.event_ts,
            impressions.collector_ts,
            when(col("clk_visitor_id").isNotNull(), True).otherwise(False).alias("was_clicked"),
            col("time_to_click_secs"),
        )
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC **Star schema complete.** Seven tables are ready: six dimensions and one fact table.
