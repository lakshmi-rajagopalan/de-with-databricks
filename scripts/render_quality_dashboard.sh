#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: scripts/render_quality_dashboard.sh <pipeline-id>"
  exit 2
fi

PIPELINE_ID="$1"
SOURCE_FILE="src/dashboards/quality_dashboard.lvdash.json"
TARGET_FILE="build/quality_dashboard.lvdash.json"

mkdir -p "$(dirname "$TARGET_FILE")"
sed "s/__PIPELINE_ID__/${PIPELINE_ID}/g" "$SOURCE_FILE" > "$TARGET_FILE"

echo "Wrote $TARGET_FILE with pipeline ID $PIPELINE_ID"
