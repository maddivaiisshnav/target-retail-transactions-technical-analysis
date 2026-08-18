#!/usr/bin/env bash
#
# End-to-end pipeline runner.
#
# In production this orchestration would be an Airflow / Dagster DAG, not a
# shell script. A real orchestrator gives you: dependency graphs, automatic
# retries with backoff, backfills over historical date ranges, SLA alerting,
# and a UI showing which task failed and why. A bash script gives you none of
# that - it is here so the whole thing is runnable in one command while you
# are learning.
#
# Usage:
#   ./run_pipeline.sh            # full run including Kafka
#   ./run_pipeline.sh --no-kafka # skip the Kafka stage (Spark + Hive only)

set -euo pipefail

PY=".venv/bin/python"
SPARK_SUBMIT=".venv/bin/spark-submit"

# spark-submit is a shell script that must locate the Spark distribution (the
# JARs and the Scala/Java launcher). When PySpark is pip-installed, that
# distribution lives INSIDE the pyspark package, so we point SPARK_HOME there.
# Without this, spark-submit looks in the wrong place and dies with
# "Could not find valid SPARK_HOME". Reminder that PySpark is a thin Python
# wrapper over a JVM application - the JVM side has to be found on disk.
export SPARK_HOME="$($PY -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')"
export PYSPARK_PYTHON="$PWD/$PY"
export PYSPARK_DRIVER_PYTHON="$PWD/$PY"
KAFKA_PKG="org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
TOPIC="target_orders"
BROKER="localhost:9092"

RUN_KAFKA=1
[[ "${1:-}" == "--no-kafka" ]] && RUN_KAFKA=0

step() { echo; echo "############################################################"; \
         echo "# $1"; echo "############################################################"; }

# ---------------------------------------------------------------------------
if [[ $RUN_KAFKA -eq 1 ]]; then
  step "STAGE 0 - Kafka broker"
  if ! kafka-topics --bootstrap-server "$BROKER" --list >/dev/null 2>&1; then
    echo "Broker not reachable. Starting it..."
    brew services start kafka
    sleep 15
  fi
  echo "Broker is up."

  kafka-topics --bootstrap-server "$BROKER" --create --topic "$TOPIC" \
    --partitions 3 --replication-factor 1 --if-not-exists 2>/dev/null || true
  kafka-topics --bootstrap-server "$BROKER" --describe --topic "$TOPIC"

  step "STAGE 1 - INGESTION: produce the CSV into Kafka"
  $PY src/ingestion/producer.py

  step "STAGE 2 - INGESTION: consume from Kafka into Spark (batch, simple)"
  # A fresh group id each run so we always replay from the beginning.
  # Kafka RETAINS messages after they are read - that replayability is the
  # whole point, and is what a traditional queue cannot do.
  $PY src/ingestion/consumer.py --group "pipeline-$(date +%s)"

  step "STAGE 2b - INGESTION: Structured Streaming (the production pattern)"
  $SPARK_SUBMIT --packages "$KAFKA_PKG" src/ingestion/streaming_consumer.py
fi

# ---------------------------------------------------------------------------
step "STAGE 3 - EDA: profile the data before trusting it"
$PY src/processing/02_eda.py

step "STAGE 3b - EDA: null policy investigation"
$PY src/processing/02b_null_investigation.py

step "STAGE 4 - TRANSFORM: business questions -> gold Parquet tables"
$PY src/processing/03_analysis.py

step "STAGE 5 - SERVE: register gold tables in the Hive metastore"
$PY src/processing/04_save_to_hive.py

step "STAGE 6 - ANALYSIS: interrogate the headline result"
$PY src/processing/05_insight_deepdive.py

step "PIPELINE COMPLETE"
echo "Hive tables live in : output/warehouse/target_retail.db/"
echo "Gold parquet in     : output/gold/"
echo "Query them with     : $PY src/processing/query_hive.py"
