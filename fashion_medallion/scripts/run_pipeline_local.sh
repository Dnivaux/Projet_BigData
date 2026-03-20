#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATASET_DIR="${DATASET_DIR:-$ROOT_DIR/../Dataset}"
DATA_LAKE_BASE="${DATA_LAKE_BASE:-$ROOT_DIR/data_lake}"
RAW_BASE="${RAW_BASE:-$DATA_LAKE_BASE/raw}"
SILVER_BASE="${SILVER_BASE:-$DATA_LAKE_BASE/silver}"
DATAMART_BASE="${DATAMART_BASE:-$DATA_LAKE_BASE/datamart}"
INGESTION_DATE="${INGESTION_DATE:-$(date +%F)}"

mkdir -p "$ROOT_DIR/logs" "$RAW_BASE" "$SILVER_BASE" "$DATAMART_BASE"

spark-submit --master local[*] --driver-memory 6g \
  "$ROOT_DIR/spark_jobs/feeder.py" \
  --articles-path "$DATASET_DIR/articles.csv" \
  --customers-path "$DATASET_DIR/customers.csv" \
  --transactions-path "$DATASET_DIR/transactions_train.csv" \
  --raw-base-path "$RAW_BASE" \
  --ingestion-date "$INGESTION_DATE" \
  --log-file "$ROOT_DIR/logs/feeder.txt"

spark-submit --master local[*] --driver-memory 8g \
  "$ROOT_DIR/spark_jobs/processor.py" \
  --raw-base-path "$RAW_BASE" \
  --silver-base-path "$SILVER_BASE" \
  --ingestion-date "$INGESTION_DATE" \
  --log-file "$ROOT_DIR/logs/processor.txt"

spark-submit --master local[*] --driver-memory 6g \
  "$ROOT_DIR/spark_jobs/datamart.py" \
  --silver-base-path "$SILVER_BASE" \
  --datamart-base-path "$DATAMART_BASE" \
  --ingestion-date "$INGESTION_DATE" \
  --log-file "$ROOT_DIR/logs/datamart.txt"

echo "Pipeline termine. Datamarts dans: $DATAMART_BASE"
