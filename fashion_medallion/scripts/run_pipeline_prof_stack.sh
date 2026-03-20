#!/usr/bin/env bash
set -euo pipefail

# Ce script suppose que le repo docker-hadoop-spark est lance
# et que ce projet est monte dans le conteneur spark-master.

SPARK_MASTER_CONTAINER="${SPARK_MASTER_CONTAINER:-spark-master}"
PROJECT_DIR_IN_CONTAINER="${PROJECT_DIR_IN_CONTAINER:-/opt/fashion_medallion}"
DATASET_DIR_IN_CONTAINER="${DATASET_DIR_IN_CONTAINER:-/opt/fashion_medallion/../Dataset}"
INGESTION_DATE="${INGESTION_DATE:-$(date +%F)}"

RAW_BASE="${RAW_BASE:-hdfs://namenode:9000/fashion/raw}"
SILVER_BASE="${SILVER_BASE:-hdfs://namenode:9000/fashion/silver}"
DATAMART_BASE="${DATAMART_BASE:-hdfs://namenode:9000/fashion/datamart}"

run_submit() {
  docker exec "$SPARK_MASTER_CONTAINER" spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    "$@"
}

run_submit \
  "$PROJECT_DIR_IN_CONTAINER/spark_jobs/feeder.py" \
  --articles-path "$DATASET_DIR_IN_CONTAINER/articles.csv" \
  --customers-path "$DATASET_DIR_IN_CONTAINER/customers.csv" \
  --transactions-path "$DATASET_DIR_IN_CONTAINER/transactions_train.csv" \
  --raw-base-path "$RAW_BASE" \
  --ingestion-date "$INGESTION_DATE" \
  --log-file "$PROJECT_DIR_IN_CONTAINER/logs/feeder.txt"

run_submit \
  "$PROJECT_DIR_IN_CONTAINER/spark_jobs/processor.py" \
  --raw-base-path "$RAW_BASE" \
  --silver-base-path "$SILVER_BASE" \
  --ingestion-date "$INGESTION_DATE" \
  --log-file "$PROJECT_DIR_IN_CONTAINER/logs/processor.txt"

run_submit \
  "$PROJECT_DIR_IN_CONTAINER/spark_jobs/datamart.py" \
  --silver-base-path "$SILVER_BASE" \
  --datamart-base-path "$DATAMART_BASE" \
  --ingestion-date "$INGESTION_DATE" \
  --log-file "$PROJECT_DIR_IN_CONTAINER/logs/datamart.txt"

echo "Pipeline execute sur stack docker-hadoop-spark"
