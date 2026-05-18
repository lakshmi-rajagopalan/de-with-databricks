#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_TEMPLATE_FILE="$SCRIPT_DIR/../src/genie/genie_space_template.json"

SPACE_NAME="${1:-Clickstream Analytics}"
WAREHOUSE_ID="${2:-${WAREHOUSE_ID:-}}"
SPACE_EXPORT_FILE="${3:-${GENIE_SPACE_EXPORT_FILE:-}}"

if [[ -z "$WAREHOUSE_ID" ]]; then
  WAREHOUSE_ID=$(databricks warehouses list -o json | jq -r '.[] | select(.name=="Serverless Starter Warehouse") | .id' | head -n1)
fi

if [[ -z "$WAREHOUSE_ID" || "$WAREHOUSE_ID" == "null" ]]; then
  echo "warehouse_id is required. Pass it as arg2 or set WAREHOUSE_ID."
  exit 2
fi

if [[ -z "$SPACE_EXPORT_FILE" ]]; then
  SPACE_EXPORT_FILE="$DEFAULT_TEMPLATE_FILE"
  echo "No serialized_space file provided. Using template: $SPACE_EXPORT_FILE"
fi

if [[ ! -f "$SPACE_EXPORT_FILE" ]]; then
  echo "serialized_space file not found: $SPACE_EXPORT_FILE"
  echo "Pass an exported Genie payload file or use the tracked template at:"
  echo "  $DEFAULT_TEMPLATE_FILE"
  exit 2
fi

if [[ ! -s "$SPACE_EXPORT_FILE" ]]; then
  echo "$SPACE_EXPORT_FILE is empty."
  echo "This usually happens when a failed export command redirects into the file."
  echo "Re-export into a temporary file, then move it into place only after success."
  exit 2
fi

if jq -e '.placeholder == true' "$SPACE_EXPORT_FILE" >/dev/null 2>&1; then
  echo "Replace $SPACE_EXPORT_FILE with a real Genie space export JSON from the Databricks UI."
  exit 2
fi

if ! jq empty "$SPACE_EXPORT_FILE" >/dev/null 2>&1; then
  echo "$SPACE_EXPORT_FILE is not valid JSON."
  exit 2
fi

SPACE_EXPORT_TYPE=$(jq -r 'type' "$SPACE_EXPORT_FILE")

if [[ "$SPACE_EXPORT_TYPE" == "string" ]]; then
  SERIALIZED_SPACE_JSON=$(jq -r '.' "$SPACE_EXPORT_FILE")
else
  SERIALIZED_SPACE_JSON=$(jq -c '
    if (.data_sources.tables? | type) == "array" then
      .data_sources.tables |= sort_by(.identifier)
    else
      .
    end
  ' "$SPACE_EXPORT_FILE")
fi

SPACES_JSON=$(databricks genie list-spaces -o json 2>/dev/null || echo "{}")

EXISTING_SPACE_ID=$(echo "$SPACES_JSON" | jq -r --arg name "$SPACE_NAME" '
  [
    .spaces[]?,
    .items[]?,
    (if type == "array" then .[] else empty end)
  ]
  | map(select((.title // .display_name // .name // "") == $name))
  | .[0]
  | (.space_id // .id // .genie_space_id // "")
')

if [[ -n "$EXISTING_SPACE_ID" && "$EXISTING_SPACE_ID" != "null" ]]; then
  echo "Genie space already exists: $SPACE_NAME ($EXISTING_SPACE_ID)"
  exit 0
fi

echo "Creating Genie space: $SPACE_NAME"
databricks genie create-space "$WAREHOUSE_ID" "$SERIALIZED_SPACE_JSON" \
  --title "$SPACE_NAME" \
  --description "Upwork job search clickstream - impressions, clicks, CTR"
