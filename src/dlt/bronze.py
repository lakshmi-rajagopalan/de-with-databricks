# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Layer — Raw Ingestion
# MAGIC
# MAGIC **Purpose:** Land all six raw CSV files as Delta tables with zero transformation.
# MAGIC Bronze tables are the permanent record of what arrived, we never delete or modify them.
# MAGIC
# MAGIC **Layer contract:**
# MAGIC - Columns ingested with inferred types; unexpected or mismatched columns rescued into `_rescued_data`
# MAGIC - Two metadata columns added to every table: `_source_file` and `_ingested_at`
# MAGIC - No filtering, no business logic
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
# MAGIC - Auto Loader (`cloudFiles`) for incremental file ingestion
# MAGIC - `spark.conf.get()` to read pipeline parameters set in the DLT UI
# MAGIC - Metadata columns for lineage tracking

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

# Pipeline parameter — set in the DLT pipeline configuration under raw_data_path.
raw_path = spark.conf.get("raw_data_path", "/Volumes/flexhire/clickstream_workshop/raw")


def _read_csv(name: str):
    """Stream new CSV files from the raw volume using Auto Loader. Each dataset lives in its own subdirectory."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("nullValue", "")
        .load(f"{raw_path}/{name}")
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_ingested_at", current_timestamp())
    )

# COMMAND ----------

@dp.table(
    name="bronze.search_events",
    comment="Raw search session events — one row per search with query context",
    table_properties={"quality": "bronze"},
)
def bronze_search_events():
    return _read_csv("search_events")

# COMMAND ----------

@dp.table(
    name="bronze.impression_events",
    comment="Raw impression events — every job listing shown to a visitor",
    table_properties={"quality": "bronze"},
)
def bronze_impression_events():
    return _read_csv("impression_events")

# COMMAND ----------

@dp.table(
    name="bronze.click_events",
    comment="Raw click events — every visitor click on a job listing",
    table_properties={"quality": "bronze"},
)
def bronze_click_events():
    return _read_csv("click_events")

# COMMAND ----------

@dp.table(
    name="bronze.job_openings",
    comment="Raw job opening metadata posted by clients",
    table_properties={"quality": "bronze"},
)
def bronze_job_openings():
    return _read_csv("job_openings")

# COMMAND ----------

@dp.table(
    name="bronze.clients",
    comment="Raw client company profiles",
    table_properties={"quality": "bronze"},
)
def bronze_clients():
    return _read_csv("clients")

# COMMAND ----------

@dp.table(
    name="bronze.freelancers",
    comment="Raw freelancer profiles linked to event data via visitor_id",
    table_properties={"quality": "bronze"},
)
def bronze_freelancers():
    return _read_csv("freelancers")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC **Bronze complete.** Six tables land in the pipeline.
# MAGIC Open `silver.py` to see how we clean and validate them.
