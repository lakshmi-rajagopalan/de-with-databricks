-- Databricks notebook source

-- COMMAND ----------
-- MAGIC %md
-- MAGIC # Data Quality Dashboard — DLT Expectations
-- MAGIC
-- MAGIC Monitors data quality across the bronze → silver pipeline using the DLT event log.
-- MAGIC Shows which expectations are failing, how many rows are being dropped, and trends
-- MAGIC across pipeline runs.
-- MAGIC
-- MAGIC **Before running:** set your pipeline ID in the cell below.
-- MAGIC Find it in the URL when viewing your pipeline: `.../#joblist/pipelines/<pipeline_id>`

-- COMMAND ----------

-- Pipeline ID widget — paste your pipeline ID into the box above the notebook
CREATE WIDGET TEXT pipeline_id DEFAULT '';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 1 · Rows Processed vs Dropped per Table
-- MAGIC **Tile type:** Grouped bar chart — table on X, passed vs dropped on Y

-- COMMAND ----------

SELECT
    origin.flow_name                                            AS table_name,
    SUM(details:flow_progress.metrics.num_output_rows)          AS rows_passed,
    SUM(details:flow_progress.data_quality.dropped_records)     AS rows_dropped,
    ROUND(
        SUM(details:flow_progress.data_quality.dropped_records) /
        NULLIF(
            SUM(details:flow_progress.metrics.num_output_rows) +
            SUM(details:flow_progress.data_quality.dropped_records), 0
        ) * 100, 2
    )                                                           AS drop_rate_pct
FROM event_log(:pipeline_id)
WHERE event_type = 'flow_progress'
  AND details:flow_progress.data_quality IS NOT NULL
GROUP BY table_name
ORDER BY rows_dropped DESC;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 2 · Per-Expectation Pass / Fail Breakdown
-- MAGIC **Tile type:** Table — apply conditional formatting: red where failed_records > 0

-- COMMAND ----------

SELECT
    e.dataset                                                   AS table_name,
    e.name                                                      AS expectation,
    SUM(e.passed_records)                                       AS passed_records,
    SUM(e.failed_records)                                       AS failed_records,
    SUM(e.passed_records) + SUM(e.failed_records)               AS total_records,
    ROUND(
        SUM(e.passed_records) /
        NULLIF(SUM(e.passed_records) + SUM(e.failed_records), 0) * 100, 2
    )                                                           AS pass_rate_pct
FROM event_log(:pipeline_id),
LATERAL EXPLODE(
    FROM_JSON(
        details:flow_progress.data_quality.expectations,
        'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>'
    )
) AS t(e)
WHERE event_type = 'flow_progress'
GROUP BY table_name, expectation
ORDER BY failed_records DESC;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 3 · Top Failing Expectations
-- MAGIC **Tile type:** Horizontal bar chart — expectation on Y, total_failures on X

-- COMMAND ----------

SELECT
    e.name                AS expectation,
    e.dataset             AS table_name,
    SUM(e.failed_records) AS total_failures
FROM event_log(:pipeline_id),
LATERAL EXPLODE(
    FROM_JSON(
        details:flow_progress.data_quality.expectations,
        'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>'
    )
) AS t(e)
WHERE event_type = 'flow_progress'
  AND e.failed_records > 0
GROUP BY expectation, table_name
ORDER BY total_failures DESC;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 4 · Drop Rate Trend Across Pipeline Runs
-- MAGIC **Tile type:** Line chart — X: run_hour, Y: rows_dropped, split by table_name

-- COMMAND ----------

SELECT
    DATE_TRUNC('hour', timestamp)                               AS run_hour,
    origin.flow_name                                            AS table_name,
    SUM(details:flow_progress.metrics.num_output_rows)          AS rows_passed,
    SUM(details:flow_progress.data_quality.dropped_records)     AS rows_dropped
FROM event_log(:pipeline_id)
WHERE event_type = 'flow_progress'
  AND details:flow_progress.data_quality IS NOT NULL
GROUP BY run_hour, table_name
ORDER BY run_hour, table_name;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 5 · Full Expectation Event Log
-- MAGIC **Tile type:** Table — raw detail for debugging individual failures

-- COMMAND ----------

SELECT
    timestamp,
    origin.flow_name  AS table_name,
    e.name            AS expectation,
    e.passed_records,
    e.failed_records
FROM event_log(:pipeline_id),
LATERAL EXPLODE(
    FROM_JSON(
        details:flow_progress.data_quality.expectations,
        'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>'
    )
) AS t(e)
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC, failed_records DESC
LIMIT 200;
