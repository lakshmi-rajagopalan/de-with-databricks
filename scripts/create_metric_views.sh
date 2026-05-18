#!/usr/bin/env bash
set -euo pipefail

CATALOG_NAME="${1:-workspace}"
SCHEMA_NAME="${2:-de}"
WAREHOUSE_ID="${3:-${WAREHOUSE_ID:-}}"

if [[ -z "$WAREHOUSE_ID" ]]; then
  WAREHOUSE_ID=$(databricks warehouses list -o json | jq -r '.[] | select(.name=="Serverless Starter Warehouse") | .id' | head -n1)
fi

if [[ -z "$WAREHOUSE_ID" || "$WAREHOUSE_ID" == "null" ]]; then
  echo "warehouse_id is required. Pass it as arg3 or set WAREHOUSE_ID."
  exit 2
fi

run_statement() {
  local statement="$1"

  local payload
  payload=$(jq -n \
    --arg warehouse_id "$WAREHOUSE_ID" \
    --arg statement "$statement" \
    '{warehouse_id: $warehouse_id, statement: $statement, wait_timeout: "50s"}')

  local response
  response=$(databricks api post /api/2.0/sql/statements --json "$payload" -o json)

  local state
  state=$(echo "$response" | jq -r '.status.state // "UNKNOWN"')

  if [[ "$state" != "SUCCEEDED" ]]; then
    echo "SQL statement failed with state=$state"
    echo "$response" | jq -r '.status.error // .status // .'
    exit 1
  fi
}

run_statement "CREATE SCHEMA IF NOT EXISTS ${CATALOG_NAME}.${SCHEMA_NAME}"
run_statement "CREATE OR REPLACE VIEW ${CATALOG_NAME}.${SCHEMA_NAME}.mv_search_funnel AS SELECT event_date, impressions, clicks, ctr_pct FROM ${CATALOG_NAME}.${SCHEMA_NAME}.gold_daily_metrics"
run_statement "CREATE OR REPLACE VIEW ${CATALOG_NAME}.${SCHEMA_NAME}.mv_position_bias AS SELECT position, impressions, clicks, ctr_pct, avg_time_to_click_secs FROM ${CATALOG_NAME}.${SCHEMA_NAME}.gold_position_ctr"
run_statement "CREATE OR REPLACE VIEW ${CATALOG_NAME}.${SCHEMA_NAME}.mv_category_performance AS SELECT category, impressions, clicks, ctr_pct, avg_budget_amount FROM ${CATALOG_NAME}.${SCHEMA_NAME}.gold_category_performance"

echo "Metric views created in ${CATALOG_NAME}.${SCHEMA_NAME}:"
echo "- mv_search_funnel"
echo "- mv_position_bias"
echo "- mv_category_performance"
