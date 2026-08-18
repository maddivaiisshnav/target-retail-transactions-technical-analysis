#!/usr/bin/env bash
# Entrypoint for the "pipeline" container in docker-compose.yml.
# Runs the same stages as run_pipeline.sh, but against the "kafka" service
# name instead of localhost, and waits for the broker to be reachable first
# since Compose starts containers in parallel, not in dependency-ready order.
set -euo pipefail

echo "Waiting for Kafka broker at ${KAFKA_BOOTSTRAP}..."
python3 - <<'PY'
import os
import socket
import sys
import time

host, port = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092").split(":")
for attempt in range(60):
    try:
        with socket.create_connection((host, int(port)), timeout=2):
            print("Kafka is reachable.")
            sys.exit(0)
    except OSError:
        time.sleep(2)
print("Kafka never became reachable.", file=sys.stderr)
sys.exit(1)
PY

echo
echo "### 1/5 - Kafka producer"
python3 src/ingestion/producer.py

echo
echo "### 2/5 - Kafka consumer -> Spark (bronze)"
python3 src/ingestion/consumer.py --group docker-run

echo
echo "### 3/5 - EDA"
python3 src/processing/02_eda.py
python3 src/processing/02b_null_investigation.py

echo
echo "### 4/5 - Business analysis (gold tables)"
python3 src/processing/03_analysis.py
python3 src/processing/05_insight_deepdive.py

echo
echo "### 5/5 - Register Hive tables"
python3 src/processing/04_save_to_hive.py

echo
echo "Pipeline complete. Hive tables are in output/warehouse/target_retail.db/"
echo "Query them with:"
echo "  docker compose exec pipeline python3 src/processing/query_hive.py"
