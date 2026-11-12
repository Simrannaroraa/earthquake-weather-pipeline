#!/usr/bin/env bash
# Helper script to upload local landing zone JSON files to Databricks Unity Catalog Volume.
set -euo pipefail

CATALOG="${1:-workspace}"
SCHEMA="${2:-bronze}"
VOLUME="${3:-landing}"
DEST_PATH="dbfs:/Volumes/${CATALOG}/${SCHEMA}/${VOLUME}/"

echo "=========================================================="
echo "Uploading landing_zone files to Databricks Volume"
echo "Destination: ${DEST_PATH}"
echo "=========================================================="

if command -v databricks &> /dev/null; then
    echo "Found Databricks CLI. Uploading *.json files..."
    for file in landing_zone/*.json; do
        if [ -f "$file" ]; then
            echo "Uploading $file -> ${DEST_PATH}"
            databricks fs cp "$file" "${DEST_PATH}"
        fi
    done
    echo "Upload completed successfully."
else
    echo "Notice: Databricks CLI is not installed or not in PATH."
    echo ""
    echo "Manual UI Upload Instructions:"
    echo "1. Open your Databricks workspace."
    echo "2. Navigate to Catalog > ${CATALOG} > ${SCHEMA} > Volumes > ${VOLUME}."
    echo "3. Click 'Upload to this volume' and select the files in:"
    echo "   $(pwd)/landing_zone/"
    echo "=========================================================="
fi

