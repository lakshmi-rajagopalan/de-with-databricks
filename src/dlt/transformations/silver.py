# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Layer — Cleansed & Validated
# MAGIC
# MAGIC **Purpose:** Promote bronze tables to typed, deduplicated, validated tables.
# MAGIC Rows that violate data quality rules are warned, dropped, or fail the pipeline
# MAGIC depending on the severity of the rule.
# MAGIC
# MAGIC **Layer contract:**
# MAGIC - Dimensions use `create_auto_cdc_flow` for SCD Type 1 upserts
# MAGIC - Mixed-type numeric columns parsed via cast-and-null pattern
# MAGIC - Fact tables pass through from bronze with expectations and minimal transforms
# MAGIC
# MAGIC **DLT concepts covered:**
# MAGIC - `@dp.expect`, `@dp.expect_or_drop`
# MAGIC - Quarantine pattern — routing rejects to a separate table
# MAGIC - `dp.create_auto_cdc_flow()` for SCD Type 1 upserts on dimension tables
# MAGIC - `spark.readStream.table()` to consume bronze tables as streaming sources

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md
# MAGIC ## Dimensions — SCD Type 1 via Auto CDC
# MAGIC
# MAGIC `job_openings`, `clients`, and `freelancers` are slowly changing dimensions.
# MAGIC Each uses a `_view` to parse mixed-type numeric columns, then
# MAGIC `create_auto_cdc_flow` to upsert into the silver table keyed on the entity ID.

# COMMAND ----------

def parse_numeric(col):
    return (
        F.when(col.cast("double").isNotNull(), col.cast("double"))
        .otherwise(None)
    )

def parse_integer(col):
    return (
        F.when(col.cast("integer").isNotNull(), col.cast("integer"))
        .otherwise(None)
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ### job_openings

# COMMAND ----------

@dp.view(
    name="job_openings_view"
)
def jobs():
    return (
        spark.readStream.table("bronze.job_openings")
        .withColumn("numeric_budget_amount", parse_numeric(F.col("budget_amount")))
        .drop("budget_amount")
    )

dp.create_streaming_table("silver.job_openings")

dp.create_auto_cdc_flow(
    source="job_openings_view",
    target="silver.job_openings",
    keys=["opening_uid"],
    sequence_by="posted_at",
    stored_as_scd_type=1,
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### clients

# COMMAND ----------

@dp.view(
    name="clients_view"
)
def clients():
    return (
        spark.readStream.table("bronze.clients")
        .withColumn("numeric_total_spend_usd", parse_numeric(F.col("total_spend_usd")))
        .drop("total_spend_usd")
    )

dp.create_streaming_table("silver.clients")

dp.create_auto_cdc_flow(
    source="clients_view",
    target="silver.clients",
    keys=["client_uid"],
    sequence_by="member_since",
    stored_as_scd_type=1,
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### freelancers

# COMMAND ----------

@dp.view(
    name="freelancers_view"
)
def freelancers():
    return (
        spark.readStream.table("bronze.freelancers")
        .withColumn("numeric_hourly_rate", parse_numeric(F.col("hourly_rate")))
        .withColumn("numeric_job_success_score", parse_numeric(F.col("job_success_score")))
        .drop("hourly_rate", "job_success_score")
    )

dp.create_streaming_table("silver.freelancers")

dp.create_auto_cdc_flow(
    source="freelancers_view",
    target="silver.freelancers",
    keys=["visitor_id"],
    sequence_by="member_since",
    stored_as_scd_type=1,
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Facts — Append-only event tables
# MAGIC
# MAGIC Event tables are immutable — rows are never updated, only appended.
# MAGIC Expectations enforce quality rules; minimal transforms fix ambiguous types.

# COMMAND ----------
# MAGIC %md
# MAGIC ### search_events

# COMMAND ----------

@dp.table(name="silver.search_events")
@dp.expect_or_drop("valid_search_guid", "search_guid IS NOT NULL")
@dp.expect_or_drop("valid_visitor_id", "visitor_id IS NOT NULL")
@dp.expect_or_drop("valid_search_query", "search_query IS NOT NULL")
@dp.expect("known_sublocation", "sublocation IN ('search_results', 'featured_jobs')")
@dp.expect("known_sort", "sort IN ('recency', 'relevance')")
def search_events():
    return spark.readStream.table("bronze.search_events")

# COMMAND ----------
# MAGIC %md
# MAGIC ### impression_events

# COMMAND ----------

@dp.table(name="silver.impression_events")
@dp.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dp.expect_or_drop("valid_visitor_id", "visitor_id IS NOT NULL")
@dp.expect_or_drop("valid_search_guid", "search_guid IS NOT NULL")
@dp.expect_or_drop("valid_opening_uid", "opening_uid IS NOT NULL")
@dp.expect_or_drop("valid_position", "position > 0")
@dp.expect_or_drop("valid_visitors", "is_bot = false")
def impression_events():
    return (
        spark.readStream.table("bronze.impression_events")
            .withColumn("is_bot", F.when(F.col("is_bot") == F.lit("True"), True).otherwise(False))
            .withColumn("position", parse_integer(F.col("position")))
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ### click_events

# COMMAND ----------

@dp.table(name="silver.click_events")
@dp.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dp.expect_or_drop("valid_visitor_id", "visitor_id IS NOT NULL")
@dp.expect_or_drop("valid_search_guid", "search_guid IS NOT NULL")
@dp.expect_or_drop("valid_opening_uid", "opening_uid IS NOT NULL")
@dp.expect_or_drop("valid_position", "position > 0")
@dp.expect_or_drop("valid_time_to_click_secs", "time_to_click_secs IS NOT NULL OR time_to_click_secs >= 0")
def click_events():
    return (
        spark.readStream.table("bronze.click_events")
            .withColumn("position", parse_integer(F.col("position")))
            .withColumn("time_to_click_secs", parse_numeric(F.col("time_to_click_secs")))
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC **Silver complete.** Six validated tables are ready for aggregation.
# MAGIC Open `gold.py` to build the business-facing metrics layer.
