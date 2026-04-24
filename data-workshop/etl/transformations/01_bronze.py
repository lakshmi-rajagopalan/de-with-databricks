# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Layer — Raw Ingestion
# MAGIC
# MAGIC **Purpose:** Land all three raw CSV files as Delta tables with zero transformation.
# MAGIC Bronze tables are the permanent record of what arrived — we never delete or modify them.
# MAGIC
# MAGIC **Layer contract:**
# MAGIC - All columns ingested as strings to preserve the original values exactly
# MAGIC - Two metadata columns added to every table: `_source_file` and `_ingested_at`
# MAGIC - No filtering, no type casting, no business logic
# MAGIC
# MAGIC **Source files:**
# MAGIC
# MAGIC | Table | File | Description |
# MAGIC |-------|------|-------------|
# MAGIC | `bronze_impression_events` | `impression_events.csv` | A job listing was shown to a visitor |
# MAGIC | `bronze_click_events` | `click_events.csv` | A visitor clicked on a job listing |
# MAGIC | `bronze_job_openings` | `job_openings.csv` | Job posting metadata |
# MAGIC | `bronze_clients` | `clients.csv` | Client company profiles |
# MAGIC | `bronze_freelancers` | `freelancers.csv` | Freelancer profiles |
# MAGIC
# MAGIC **DLT concepts covered:**
# MAGIC - `@dlt.table` decorator
# MAGIC - `spark.conf.get()` to read pipeline parameters set in the DLT UI
# MAGIC - Metadata columns for lineage tracking

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp, lit

# Pipeline parameter — set in the DLT pipeline configuration under raw_data_path.
raw_path = spark.conf.get("raw_data_path", "/Volumes/workspace/de/raw")


def _read_csv(filename: str):
    """Read a CSV from the raw volume. All columns land as strings (inferSchema=false)."""
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("nullValue", "")
        .load(f"{raw_path}/{filename}")
        .withColumn("_source_file", lit(filename))
        .withColumn("_ingested_at", current_timestamp())
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Impression Events
# MAGIC
# MAGIC One row per job listing shown to a visitor during a search session.
# MAGIC Columns: `event_id`, `visitor_id`, `search_guid`, `opening_uid`, `position`,
# MAGIC `sublocation`, `search_query`, `event_ts`, `collector_ts`, `is_bot`, `page`, `sort`.

# COMMAND ----------

@dlt.table(
    name="bronze_impression_events",
    comment="Raw impression events — every job listing shown to a visitor",
    table_properties={"quality": "bronze"},
)
def bronze_impression_events():
    return _read_csv("impression_events.csv")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Click Events
# MAGIC
# MAGIC One row per click on a job listing.
# MAGIC Columns: `event_id`, `visitor_id`, `search_guid`, `opening_uid`, `position`,
# MAGIC `event_ts`, `collector_ts`, `is_bot`, `time_to_click_secs`.
# MAGIC
# MAGIC > **Note:** `time_to_click_secs` contains mixed values — integers, `"instant"`,
# MAGIC > `"fast"` and negatives. Silver handles the parsing.

# COMMAND ----------

@dlt.table(
    name="bronze_click_events",
    comment="Raw click events — every visitor click on a job listing",
    table_properties={"quality": "bronze"},
)
def bronze_click_events():
    return _read_csv("click_events.csv")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Job Openings
# MAGIC
# MAGIC One row per job posted by a client.
# MAGIC Columns: `opening_uid`, `title`, `category`, `budget_type`, `budget_amount`,
# MAGIC `client_uid`, `posted_at`, `is_active`.
# MAGIC
# MAGIC > **Note:** `budget_amount` contains the string `"negotiable"` for some rows
# MAGIC > and negative values for others. Silver handles both cases.

# COMMAND ----------

@dlt.table(
    name="bronze_job_openings",
    comment="Raw job opening metadata posted by clients",
    table_properties={"quality": "bronze"},
)
def bronze_job_openings():
    return _read_csv("job_openings.csv")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Clients
# MAGIC
# MAGIC One row per client company that posts jobs on the platform.
# MAGIC Columns: `client_uid`, `company_name`, `country`, `industry`, `payment_verified`,
# MAGIC `total_spend_usd`, `member_since`, `avg_rating`.
# MAGIC
# MAGIC > **Note:** `total_spend_usd` contains `"negotiable"` and `"not disclosed"` for some
# MAGIC > clients, and `avg_rating` is blank for new accounts. Silver handles both cases.

# COMMAND ----------

@dlt.table(
    name="bronze_clients",
    comment="Raw client company profiles",
    table_properties={"quality": "bronze"},
)
def bronze_clients():
    return _read_csv("clients.csv")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Freelancers
# MAGIC
# MAGIC One row per freelancer visible in the clickstream data (keyed on `visitor_id`).
# MAGIC Columns: `visitor_id`, `name`, `country`, `primary_skill`, `hourly_rate`,
# MAGIC `job_success_score`, `member_since`, `top_rated`, `is_verified`.
# MAGIC
# MAGIC > **Note:** `hourly_rate` contains `"negotiable"` for some freelancers (like
# MAGIC > `budget_amount` in job openings). `job_success_score` is `"N/A"` for new accounts
# MAGIC > and negative for a handful of erroneous rows — silver handles both.

# COMMAND ----------

@dlt.table(
    name="bronze_freelancers",
    comment="Raw freelancer profiles linked to event data via visitor_id",
    table_properties={"quality": "bronze"},
)
def bronze_freelancers():
    return _read_csv("freelancers.csv")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC **Bronze complete.** Five tables land in the pipeline.
# MAGIC Open `02_silver.py` to see how we clean and validate them.
