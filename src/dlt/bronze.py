# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Layer — Raw Ingestion
# MAGIC
# MAGIC **Purpose:** Land all six raw CSV files as Delta tables with zero transformation.
# MAGIC Bronze tables are the permanent record of what arrived, we never delete or modify them.
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
# MAGIC | `bronze_search_events` | `search_events.csv` | One search session per visitor per query |
# MAGIC | `bronze_click_events` | `click_events.csv` | A visitor clicked on a job listing |
# MAGIC | `bronze_job_openings` | `job_openings.csv` | Job posting metadata |
# MAGIC | `bronze_clients` | `clients.csv` | Client company profiles |
# MAGIC | `bronze_freelancers` | `freelancers.csv` | Freelancer profiles |
# MAGIC
# MAGIC **DLT concepts covered:**
# MAGIC - `@dp.table` decorator
# MAGIC - `spark.conf.get()` to read pipeline parameters set in the DLT UI
# MAGIC - Metadata columns for lineage tracking

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, lit

# Pipeline parameter — set in the DLT pipeline configuration under raw_data_path.
raw_path = spark.conf.get("raw_data_path", "/Volumes/workspace/clickstream_workshop/raw")


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

@dp.table(
    name="bronze_search_events",
    comment="Raw search session events — one row per search with query context",
    table_properties={"quality": "bronze"},
)
def bronze_search_events():
    return _read_csv("search_events.csv")

# COMMAND ----------

@dp.table(
    name="bronze_impression_events",
    comment="Raw impression events — every job listing shown to a visitor",
    table_properties={"quality": "bronze"},
)
def bronze_impression_events():
    return _read_csv("impression_events.csv")

# COMMAND ----------

@dp.table(
    name="bronze_click_events",
    comment="Raw click events — every visitor click on a job listing",
    table_properties={"quality": "bronze"},
)
def bronze_click_events():
    return _read_csv("click_events.csv")

# COMMAND ----------

@dp.table(
    name="bronze_job_openings",
    comment="Raw job opening metadata posted by clients",
    table_properties={"quality": "bronze"},
)
def bronze_job_openings():
    return _read_csv("job_openings.csv")

# COMMAND ----------

@dp.table(
    name="bronze_clients",
    comment="Raw client company profiles",
    table_properties={"quality": "bronze"},
)
def bronze_clients():
    return _read_csv("clients.csv")

# COMMAND ----------

@dp.table(
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
# MAGIC Open `silver.py` to see how we clean and validate them.
