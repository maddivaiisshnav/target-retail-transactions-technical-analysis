#!/usr/bin/env bash
#
# Orchestrator script to execute the pipeline end-to-end.
# Runs ingestion, streaming consumption, data quality profiling, 
# analytical transformations, and Hive catalog loading.
#
# Usage:
#   ./run_pipeline.sh            # full execution with Kafka
#   ./run_pipeline.sh --no-kafka # skip the Kafka ingestion step (Spark + Hive only)

set -euo pipefail

PYTHON_EXEC=".venv/bin/python"
SPARK_SUBMIT_EXEC=".venv/bin/spark-submit"

# Resolve SPARK_HOME environment path pointing to local PySpark package directory
export SPARK_HOME="$($PYTHON_EXEC -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')"
export PYSPARK_PYTHON="$PWD/$PYTHON_EXEC"
export PYSPARK_DRIVER_PYTHON="$PWD/$PYTHON_EXEC"

KAFKA_CONNECT_PKG="org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
TOPIC="target_orders"
BROKER="localhost:9092"

RUN_KAFKA_STEP=1
[[ "${1:-}" == "--no-kafka" ]] && RUN_KAFKA_STEP=0

log_step() {
  echo -e "\n============================================================"
  echo -e ">>> $1"
  echo -e "============================================================\n"
}

# ---------------------------------------------------------------------------
if [[ $RUN_KAFKA_STEP -eq 1 ]]; then
  log_step "Verifying and Starting Kafka Services"
  if ! kafka-topics --bootstrap-server "$BROKER" --list >/dev/null 2>&1; then
    echo "Kafka broker is not running. Starting service..."
    brew services start kafka
    sleep 15
  fi
  echo "Kafka broker is active."

  # Initialize ingest topics
  kafka-topics --bootstrap-server "$BROKER" --create --topic "$TOPIC" \
    --partitions 3 --replication-factor 1 --if-not-exists 2>/dev/null || true
  kafka-topics --bootstrap-server "$BROKER" --describe --topic "$TOPIC"

  log_step "Ingestion Stage: Producing Events to Kafka"
  $PYTHON_EXEC src/ingestion/producer.py

  log_step "Ingestion Stage: Structured Streaming Consumption"
  $SPARK_SUBMIT_EXEC --packages "$KAFKA_CONNECT_PKG" src/ingestion/consumer.py
fi

# ---------------------------------------------------------------------------
log_step "Profiling Stage: Data Quality Profiling (EDA)"
$PYTHON_EXEC src/processing/eda.py

log_step "Transformation Stage: Compute Silver & Gold Aggregations"
$PYTHON_EXEC src/processing/transforms.py

log_step "Serving Stage: Register Catalog Tables in Hive Metastore"
$PYTHON_EXEC src/processing/hive_loader.py

log_step "PIPELINE COMPLETED SUCCESSFULLY"
echo "Hive warehouse path : output/warehouse/target_retail.db/"
echo "Gold Parquet path   : output/gold/"
echo "Interactive SQL CLI : $PYTHON_EXEC src/processing/query_hive.py"
