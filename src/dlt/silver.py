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
# MAGIC - `@dp.expect`, `@dp.expect_or_drop`, `@dp.expect_or_fail`
# MAGIC - Quarantine pattern — routing rejects to a separate table instead of dropping them
# MAGIC - `dp.create_auto_cdc_flow()` for SCD Type 1 upserts (`silver_job_openings`)
# MAGIC - `dp.read_stream()` to consume bronze tables as streaming sources
# MAGIC - `regexp_extract` / conditional casting to parse mixed-type columns

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, to_timestamp, trim, when, regexp_extract, current_timestamp, lit,
)
from pyspark.sql.types import IntegerType, DoubleType, BooleanType

# COMMAND ----------
# MAGIC %md
# MAGIC ## silver_impression_events
# MAGIC
# MAGIC Cast types, drop bots, and validate positions.
# MAGIC `opening_uid` is required — impressions without a linked job are meaningless.

# COMMAND ----------

@dp.table(
    name="silver_impression_events",
    comment="Impression events — typed, bot-free, with valid positions and opening IDs",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("event_id_not_null", "event_id IS NOT NULL")
@dp.expect_or_drop("opening_uid_not_null", "opening_uid IS NOT NULL")
@dp.expect_or_drop("search_guid_not_null", "search_guid IS NOT NULL")
@dp.expect_or_drop("no_bots", "is_bot = false")
@dp.expect_or_drop("valid_position", "position > 0")
def silver_impression_events():
    return (
        dp.read_stream("bronze_impression_events")
        .select(
            col("event_id"),
            col("visitor_id"),
            col("search_guid"),
            col("opening_uid"),
            col("position").cast(IntegerType()).alias("position"),
            to_timestamp(col("collector_ts")).alias("collector_ts"),
            when(col("is_bot") == "True", True).otherwise(False).alias("is_bot"),
        )
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## silver_search_events
# MAGIC
# MAGIC One row per search session. `search_guid` is the PK linking all impressions
# MAGIC from the same query into a single session context.

# COMMAND ----------

@dp.table(
    name="silver_search_events",
    comment="Search session events — one row per search with query context and session attributes",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("search_guid_not_null", "search_guid IS NOT NULL")
@dp.expect_or_drop("visitor_id_not_null", "visitor_id IS NOT NULL")
@dp.expect("known_sublocation", "sublocation IN ('search_results', 'featured_jobs')")
@dp.expect("known_sort", "sort IN ('relevance', 'recency')")
@dp.expect("reasonable_event_ts", "event_ts <= current_timestamp()")
def silver_search_events():
    return (
        dp.read_stream("bronze_search_events")
        .select(
            col("search_guid"),
            col("visitor_id"),
            trim(col("search_query")).alias("search_query"),
            trim(col("sublocation")).alias("sublocation"),
            trim(col("sort")).alias("sort"),
            col("page").cast(IntegerType()).alias("page"),
            to_timestamp(col("event_ts")).alias("event_ts"),
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

@dp.table(
    name="silver_click_events",
    comment="Click events — typed, bot-free, deduplicated, with safe time_to_click parsing",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("event_id_not_null", "event_id IS NOT NULL")
@dp.expect_or_drop("no_bots", "is_bot = false")
@dp.expect_or_drop("valid_position", "position > 0")
@dp.expect("reasonable_event_ts", "event_ts <= current_timestamp()")
@dp.expect("non_negative_time_to_click", "time_to_click_secs IS NULL OR time_to_click_secs >= 0")
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
        dp.read_stream("bronze_click_events")
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
# MAGIC ## quarantine_click_events
# MAGIC
# MAGIC The quarantine pattern is an alternative to `@expect_or_drop`: instead of silently
# MAGIC discarding bad rows, route them to a separate table so they can be investigated
# MAGIC and reprocessed after the source issue is fixed.
# MAGIC
# MAGIC This table captures every click event that `silver_click_events` would reject,
# MAGIC reading directly from bronze so no data is lost. The most common reason is bot
# MAGIC traffic — which is analytically useful on its own for fraud monitoring.
# MAGIC
# MAGIC | Quarantine reason | Silver rule it mirrors |
# MAGIC |-------------------|----------------------|
# MAGIC | `bot_traffic` | `no_bots` (`@expect_or_drop`) |
# MAGIC | `null_event_id` | `event_id_not_null` (`@expect_or_drop`) |
# MAGIC | `invalid_position` | `valid_position` (`@expect_or_drop`) |

# COMMAND ----------

@dp.table(
    name="quarantine_click_events",
    comment="Click events rejected by silver quality rules — preserved for investigation and reprocessing",
    table_properties={"quality": "quarantine"},
)
def quarantine_click_events():
    position_int = col("position").cast("int")
    return (
        dp.read_stream("bronze_click_events")
        .where(
            col("event_id").isNull()
            | (col("is_bot") == "True")
            | position_int.isNull()
            | (position_int <= 0)
        )
        .withColumn(
            "quarantine_reason",
            when(col("event_id").isNull(), lit("null_event_id"))
            .when(col("is_bot") == "True", lit("bot_traffic"))
            .otherwise(lit("invalid_position")),
        )
        .withColumn("quarantined_at", current_timestamp())
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## silver_job_openings
# MAGIC
# MAGIC Uses `dp.apply_changes()` to maintain an SCD Type 1 table keyed on `opening_uid`.
# MAGIC When a job is updated (status change, budget edit), the row with the latest `posted_at`
# MAGIC wins — older versions are overwritten, not tracked.
# MAGIC
# MAGIC The source view `job_openings_cdc` handles type casting and budget parsing.
# MAGIC Quality rules on the source view handle type safety; `create_auto_cdc_flow` manages the upsert.
# MAGIC
# MAGIC `budget_amount` parsing:
# MAGIC
# MAGIC | Raw value | Treatment |
# MAGIC |-----------|-----------|
# MAGIC | `"95.67"`, `"50"` | Cast to double normally |
# MAGIC | `"negotiable"` | Non-numeric string → null |
# MAGIC | `"-500"` | Parsed to -500.0, then **warned** by expectation |
# MAGIC
# MAGIC Rows with a missing `title` are dropped — they can't be displayed to users.

# COMMAND ----------

@dp.view(name="job_openings_cdc")
def job_openings_cdc():
    numeric_budget = (
        when(
            regexp_extract(col("budget_amount"), r"^-?\d+(\.\d+)?$", 0) != "",
            col("budget_amount").cast(DoubleType()),
        ).otherwise(None)
    )

    return (
        dp.read_stream("bronze_job_openings")
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

# Create the target streaming table first (required for Auto CDC)
dp.create_streaming_table(
    name="silver_job_openings",
    comment="Job openings — typed, title-validated, with safe budget parsing",
    table_properties={"quality": "silver"},
)

# create_auto_cdc_flow maintains silver_job_openings as SCD Type 1 keyed on opening_uid.
# When a job is updated (status change, budget edit), the latest posted_at wins.
dp.create_auto_cdc_flow(
    target="silver_job_openings",
    source="job_openings_cdc",
    keys=["opening_uid"],
    sequence_by="posted_at",
    stored_as_scd_type=1,
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

@dp.table(
    name="silver_clients",
    comment="Client profiles — typed, with safe total_spend parsing and rating validation",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("client_uid_not_null", "client_uid IS NOT NULL")
@dp.expect("payment_verified_known", "payment_verified IN (true, false)")
@dp.expect("valid_rating", "avg_rating IS NULL OR (avg_rating >= 0 AND avg_rating <= 5)")
@dp.expect("reasonable_member_since", "member_since <= current_date()")
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
        dp.read_stream("bronze_clients")
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

@dp.table(
    name="silver_freelancers",
    comment="Freelancer profiles — typed, with safe hourly_rate and job_success_score parsing",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("visitor_id_not_null", "visitor_id IS NOT NULL")
@dp.expect("valid_job_success_score", "job_success_score IS NULL OR (job_success_score >= 0 AND job_success_score <= 100)")
@dp.expect("reasonable_member_since", "member_since <= current_date()")
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
        dp.read_stream("bronze_freelancers")
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
# MAGIC Open `gold.py` to build the business-facing metrics layer.
