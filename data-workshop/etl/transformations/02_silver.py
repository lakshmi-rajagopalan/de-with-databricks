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
# MAGIC - Correct Spark data types (timestamps, integers, doubles, booleans)
# MAGIC - Bot traffic removed (`is_bot = False`)
# MAGIC - Duplicate event IDs deduplicated
# MAGIC - Mixed-type columns (`time_to_click_secs`, `budget_amount`) parsed safely
# MAGIC - Null keys cause the row to be **dropped** (`expect_or_drop`)
# MAGIC - Business rule violations are **warned** (`expect`) — visible in DLT event log
# MAGIC - Structural violations **fail** the pipeline (`expect_or_fail`)
# MAGIC
# MAGIC **DLT concepts covered:**
# MAGIC - `@dlt.expect`, `@dlt.expect_or_drop`, `@dlt.expect_or_fail`
# MAGIC - `dlt.read()` to reference upstream tables in the same pipeline
# MAGIC - `regexp_extract` / conditional casting to parse mixed-type columns

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, to_timestamp, trim, when, regexp_extract, current_timestamp,
)
from pyspark.sql.types import IntegerType, DoubleType, BooleanType

# COMMAND ----------
# MAGIC %md
# MAGIC ## silver_impression_events
# MAGIC
# MAGIC Cast types, drop bots, and validate positions.
# MAGIC `opening_uid` is required — impressions without a linked job are meaningless.

# COMMAND ----------

@dlt.table(
    name="silver_impression_events",
    comment="Impression events — typed, bot-free, with valid positions and opening IDs",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("event_id_not_null", "event_id IS NOT NULL")
@dlt.expect_or_drop("opening_uid_not_null", "opening_uid IS NOT NULL")
@dlt.expect_or_drop("no_bots", "is_bot = false")
@dlt.expect_or_drop("valid_position", "position > 0")
@dlt.expect("known_sublocation", "sublocation IN ('search_results', 'featured_jobs')")
@dlt.expect("known_sort", "sort IN ('relevance', 'recency')")
@dlt.expect("reasonable_event_ts", "event_ts <= current_timestamp()")
def silver_impression_events():
    return (
        dlt.read("bronze_impression_events")
        .select(
            col("event_id"),
            col("visitor_id"),
            col("search_guid"),
            col("opening_uid"),
            col("position").cast(IntegerType()).alias("position"),
            trim(col("sublocation")).alias("sublocation"),
            trim(col("search_query")).alias("search_query"),
            to_timestamp(col("event_ts")).alias("event_ts"),
            to_timestamp(col("collector_ts")).alias("collector_ts"),
            when(col("is_bot") == "True", True).otherwise(False).alias("is_bot"),
            col("page").cast(IntegerType()).alias("page"),
            trim(col("sort")).alias("sort"),
        )
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## silver_click_events
# MAGIC
# MAGIC The trickiest table: `time_to_click_secs` contains mixed values.
# MAGIC
# MAGIC | Raw value | Treatment |
# MAGIC |-----------|-----------|
# MAGIC | `"8"`, `"33"` | Cast to integer normally |
# MAGIC | `"instant"`, `"fast"` | Non-numeric string → null |
# MAGIC | `"-5"` | Parsed as integer, then **warned** by expectation |
# MAGIC | `null` | Kept as null |
# MAGIC
# MAGIC `opening_uid` may be null for clicks that don't link back to a specific job
# MAGIC (e.g., browse navigation). These are kept but can be excluded in gold queries.
# MAGIC
# MAGIC Duplicate `event_id` rows are deduplicated — keep the first occurrence.

# COMMAND ----------

@dlt.table(
    name="silver_click_events",
    comment="Click events — typed, bot-free, deduplicated, with safe time_to_click parsing",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("event_id_not_null", "event_id IS NOT NULL")
@dlt.expect_or_drop("no_bots", "is_bot = false")
@dlt.expect_or_drop("valid_position", "position > 0")
@dlt.expect("reasonable_event_ts", "event_ts <= current_timestamp()")
@dlt.expect("non_negative_time_to_click", "time_to_click_secs IS NULL OR time_to_click_secs >= 0")
def silver_click_events():
    # Extract only strings that look like an integer (optional leading minus + digits).
    # Everything else (e.g. "instant", "fast") becomes null.
    numeric_ttc = (
        when(
            regexp_extract(col("time_to_click_secs"), r"^-?\d+$", 0) != "",
            col("time_to_click_secs").cast(IntegerType()),
        ).otherwise(None)
    )

    return (
        dlt.read("bronze_click_events")
        .select(
            col("event_id"),
            col("visitor_id"),
            col("search_guid"),
            col("opening_uid"),          # intentionally nullable
            col("position").cast(IntegerType()).alias("position"),
            to_timestamp(col("event_ts")).alias("event_ts"),
            to_timestamp(col("collector_ts")).alias("collector_ts"),
            when(col("is_bot") == "True", True).otherwise(False).alias("is_bot"),
            numeric_ttc.alias("time_to_click_secs"),
        )
        .dropDuplicates(["event_id"])    # deduplicate before expectations run
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## silver_job_openings
# MAGIC
# MAGIC `budget_amount` has two issues handled here:
# MAGIC
# MAGIC | Raw value | Treatment |
# MAGIC |-----------|-----------|
# MAGIC | `"95.67"`, `"50"` | Cast to double normally |
# MAGIC | `"negotiable"` | Non-numeric string → null |
# MAGIC | `"-500"` | Parsed to -500.0, then **warned** by expectation |
# MAGIC
# MAGIC Rows with a missing `title` are dropped — they can't be displayed to users.

# COMMAND ----------

@dlt.table(
    name="silver_job_openings",
    comment="Job openings — typed, title-validated, with safe budget parsing",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_fail("opening_uid_not_null", "opening_uid IS NOT NULL")
@dlt.expect_or_drop("title_not_null", "title IS NOT NULL AND title != ''")
@dlt.expect("positive_budget", "budget_amount IS NULL OR budget_amount > 0")
@dlt.expect("reasonable_posted_at", "posted_at <= current_timestamp()")
@dlt.expect("known_budget_type", "budget_type IN ('hourly', 'fixed', 'monthly', 'negotiable')")
def silver_job_openings():
    # Parse budget_amount: treat any non-numeric string as null
    numeric_budget = (
        when(
            regexp_extract(col("budget_amount"), r"^-?\d+(\.\d+)?$", 0) != "",
            col("budget_amount").cast(DoubleType()),
        ).otherwise(None)
    )

    return (
        dlt.read("bronze_job_openings")
        .select(
            col("opening_uid"),
            trim(col("title")).alias("title"),
            trim(col("category")).alias("category"),
            trim(col("budget_type")).alias("budget_type"),
            numeric_budget.alias("budget_amount"),
            col("client_uid"),
            to_timestamp(col("posted_at")).alias("posted_at"),
            when(col("is_active") == "True", True).otherwise(False).alias("is_active"),
        )
    )

# COMMAND ----------
# MAGIC %md
# COMMAND ----------
# MAGIC %md
# MAGIC ## silver_clients
# MAGIC
# MAGIC Client company profiles joined to job postings.
# MAGIC
# MAGIC `total_spend_usd` has three issues handled here:
# MAGIC
# MAGIC | Raw value | Treatment |
# MAGIC |-----------|-----------|
# MAGIC | `"12500.00"`, `"8750"` | Cast to double normally |
# MAGIC | `"negotiable"`, `"not disclosed"` | Non-numeric string → null |
# MAGIC | blank / null | Kept as null |
# MAGIC
# MAGIC `avg_rating` is blank for new accounts (kept as null).
# MAGIC `member_since` with a future date triggers a warning expectation.

# COMMAND ----------

@dlt.table(
    name="silver_clients",
    comment="Client profiles — typed, with safe total_spend parsing and rating validation",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_fail("client_uid_not_null", "client_uid IS NOT NULL")
@dlt.expect("payment_verified_known", "payment_verified IN (true, false)")
@dlt.expect("valid_rating", "avg_rating IS NULL OR (avg_rating >= 0 AND avg_rating <= 5)")
@dlt.expect("reasonable_member_since", "member_since <= current_date()")
def silver_clients():
    numeric_spend = (
        when(
            regexp_extract(col("total_spend_usd"), r"^-?\d+(\.\d+)?$", 0) != "",
            col("total_spend_usd").cast(DoubleType()),
        ).otherwise(None)
    )
    numeric_rating = (
        when(
            regexp_extract(col("avg_rating"), r"^\d+(\.\d+)?$", 0) != "",
            col("avg_rating").cast(DoubleType()),
        ).otherwise(None)
    )

    return (
        dlt.read("bronze_clients")
        .select(
            col("client_uid"),
            trim(col("company_name")).alias("company_name"),
            trim(col("country")).alias("country"),
            trim(col("industry")).alias("industry"),
            when(col("payment_verified") == "True", True).otherwise(False).alias("payment_verified"),
            numeric_spend.alias("total_spend_usd"),
            to_timestamp(col("member_since")).cast("date").alias("member_since"),
            numeric_rating.alias("avg_rating"),
        )
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## silver_freelancers
# MAGIC
# MAGIC Freelancer profiles linked to impression and click events via `visitor_id`.
# MAGIC
# MAGIC `hourly_rate` follows the same mixed-type pattern as `budget_amount`:
# MAGIC
# MAGIC | Raw value | Treatment |
# MAGIC |-----------|-----------|
# MAGIC | `"45.00"`, `"30"` | Cast to double normally |
# MAGIC | `"negotiable"` | Non-numeric string → null |
# MAGIC
# MAGIC `job_success_score` follows the same pattern as `time_to_click_secs`:
# MAGIC
# MAGIC | Raw value | Treatment |
# MAGIC |-----------|-----------|
# MAGIC | `"92"`, `"88"` | Cast to integer normally |
# MAGIC | `"N/A"` | Non-numeric string → null |
# MAGIC | `"-5"`, `"-3"` | Parsed as integer, then **warned** by expectation |

# COMMAND ----------

@dlt.table(
    name="silver_freelancers",
    comment="Freelancer profiles — typed, with safe hourly_rate and job_success_score parsing",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_fail("visitor_id_not_null", "visitor_id IS NOT NULL")
@dlt.expect("valid_job_success_score", "job_success_score IS NULL OR (job_success_score >= 0 AND job_success_score <= 100)")
@dlt.expect("reasonable_member_since", "member_since <= current_date()")
def silver_freelancers():
    numeric_rate = (
        when(
            regexp_extract(col("hourly_rate"), r"^-?\d+(\.\d+)?$", 0) != "",
            col("hourly_rate").cast(DoubleType()),
        ).otherwise(None)
    )
    numeric_score = (
        when(
            regexp_extract(col("job_success_score"), r"^-?\d+$", 0) != "",
            col("job_success_score").cast(IntegerType()),
        ).otherwise(None)
    )

    return (
        dlt.read("bronze_freelancers")
        .select(
            col("visitor_id"),
            trim(col("name")).alias("name"),
            trim(col("country")).alias("country"),
            trim(col("primary_skill")).alias("primary_skill"),
            numeric_rate.alias("hourly_rate"),
            numeric_score.alias("job_success_score"),
            to_timestamp(col("member_since")).cast("date").alias("member_since"),
            when(col("top_rated") == "True", True).otherwise(False).alias("top_rated"),
            when(col("is_verified") == "True", True).otherwise(False).alias("is_verified"),
        )
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC **Silver complete.** Five validated tables are ready for aggregation.
# MAGIC Open `03_gold.py` to build the business-facing metrics layer.
