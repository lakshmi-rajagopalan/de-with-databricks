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
# MAGIC - Bot traffic quarantined to a separate table rather than silently dropped
# MAGIC
# MAGIC **DLT concepts covered:**
# MAGIC - `@dp.expect`, `@dp.expect_or_drop`
# MAGIC - Quarantine pattern — routing bot traffic to a separate table
# MAGIC - `dp.create_auto_cdc_flow()` for SCD Type 1 upserts on dimension tables
# MAGIC - `spark.readStream.table()` to consume bronze tables as streaming sources

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# COMMAND ----------

def numeric(column_name):
    return F.col(column_name).cast("double")

def integer(column_name):
    return F.col(column_name).cast("int")

def is_bot(column_name):
    return F.when(F.col(column_name) == F.lit("True"), True).otherwise(False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Dimensions — SCD Type 1 via Auto CDC
# MAGIC
# MAGIC `job_openings`, `clients`, and `freelancers` are slowly changing dimensions.
# MAGIC Each uses a `_view` to parse mixed-type numeric columns, then
# MAGIC `create_auto_cdc_flow` to upsert into the silver table keyed on the entity ID.

# COMMAND ----------
# MAGIC %md
# MAGIC ### freelancers

# COMMAND ----------

@dp.view(name="freelancers_view")
def freelancers():
    return (
        spark.readStream
        .table("bronze.freelancers")
        .withColumn("numeric_hourly_rate", numeric("hourly_rate"))
        .withColumn("numeric_job_success_score", numeric("job_success_score"))
    )

dp.create_streaming_table("silver.freelancers")

dp.create_auto_cdc_flow(
    source="freelancers_view",
    target="silver.freelancers",
    keys=["visitor_id"],
    sequence_by="_ingested_at",
    stored_as_scd_type=1,
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### clients

# COMMAND ----------

@dp.view(name="clients_view")
def clients():
    return (
        spark.readStream
        .table("bronze.clients")
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("numeric_total_spend_usd", numeric("total_spend_usd"))
    )

dp.create_streaming_table("silver.clients")

dp.create_auto_cdc_flow(
    source="clients_view",
    target="silver.clients",
    keys=["client_uid"],
    sequence_by="_ingested_at",
    stored_as_scd_type=1,
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### job_openings

# COMMAND ----------

@dp.view(name="job_openings_view")
def job_openings():
    return (
        spark.readStream
        .table("bronze.job_openings")
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("numeric_budget_amount", numeric("budget_amount"))
        .drop("budget_amount")
    )

dp.create_streaming_table("silver.job_openings")

dp.create_auto_cdc_flow(
    source="job_openings_view",
    target="silver.job_openings",
    keys=["opening_uid"],
    sequence_by="_ingested_at",
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
@dp.expect_or_drop("valid_visitor_id", "visitor_id IS NOT NULL")
@dp.expect_or_drop("valid_search_guid", "search_guid IS NOT NULL")
@dp.expect_or_drop("valid_sublocation", "sublocation IN ('search_results', 'featured_jobs')")
def search_events():
    return (
        spark.readStream
        .table("bronze.search_events")
        .withColumnRenamed("collector_ts", "event_ts")
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ### impression_events

# COMMAND ----------

@dp.table(name="silver.impression_events")
@dp.expect_or_drop("valid_visitor_id", "visitor_id IS NOT NULL")
@dp.expect_or_drop("valid_search_guid", "search_guid IS NOT NULL")
@dp.expect_or_drop("valid_opening_uid", "opening_uid IS NOT NULL")
@dp.expect_or_drop("valid_position", "position IS NOT NULL")
@dp.expect_or_drop("valid_visitors", "is_bot = false")
def impression_events():
    return (
        spark.readStream
        .table("bronze.impression_events")
        .withColumnRenamed("collector_ts", "event_ts")
        .withColumn("position", integer("position"))
        .withColumn("is_bot", is_bot("is_bot"))
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ### quarantine_impression_events
# MAGIC
# MAGIC Captures bot traffic rejected by `silver.impression_events`.
# MAGIC Useful for fraud monitoring and volume trending — no data is lost.

# COMMAND ----------

@dp.table(name="silver.quarantine_impression_events")
def quarantine_impression_events():
    return (
        spark.readStream
        .table("bronze.impression_events")
        .withColumn("position", integer("position"))
        .withColumn("is_bot", is_bot("is_bot"))
        .filter(F.col("is_bot"))
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ### click_events

# COMMAND ----------

@dp.table(name="silver.click_events")
@dp.expect_or_drop("valid_visitor_id", "visitor_id IS NOT NULL")
@dp.expect_or_drop("valid_search_guid", "search_guid IS NOT NULL")
@dp.expect_or_drop("valid_opening_uid", "opening_uid IS NOT NULL")
@dp.expect_or_drop("valid_position", "position IS NOT NULL")
@dp.expect_or_drop("valid_time_to_click_secs", "time_to_click_secs IS NOT NULL")
@dp.expect_or_drop("valid_visitors", "is_bot = false")
def click_events():
    return (
        spark.readStream
        .table("bronze.click_events")
        .withColumnRenamed("collector_ts", "event_ts")
        .withColumn("position", integer("position"))
        .withColumn("time_to_click_secs", numeric("time_to_click_secs"))
        .withColumn("is_bot", is_bot("is_bot"))
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ### fact_search_funnel
# MAGIC
# MAGIC Impression-grain session fact table. Joins search context, impression, and click
# MAGIC outcome into a single row per impression — the canonical grain for CTR analysis.
# MAGIC Uses `spark.read` (batch snapshot) because click latency is unbounded; streaming
# MAGIC joins would require watermarks that would delay or drop valid late-arriving clicks.

# COMMAND ----------

@dp.table(
    name="silver.fact_search_funnel",
    comment="Impression-grain session table — search context joined with impressions and click outcomes",
    table_properties={"quality": "silver"},
)
def fact_search_funnel():
    searches = spark.read.table("silver.search_events").select(
        "search_guid",
        "visitor_id",
        "search_query",
        "sublocation",
        F.col("ingest_ts").alias("search_ts"),
    )
    impressions = spark.read.table("silver.impression_events").select(
        F.col("event_id").alias("impression_id"),
        "search_guid",
        "opening_uid",
        "position",
    )
    clicks = spark.read.table("silver.click_events").select(
        F.col("event_id").alias("click_id"),
        "search_guid",
        "opening_uid",
        "time_to_click_secs",
    )
    return (
        searches
        .join(impressions, on="search_guid", how="left")
        .join(clicks, on=["search_guid", "opening_uid"], how="left")
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC **Silver complete.** Seven validated tables are ready for aggregation.
# MAGIC Open `gold.sql` to build the business-facing metrics layer.
