#!/usr/bin/env bash
# Entrypoint for the Docker container orchestration.
set -euo pipefail

echo "Checking Kafka broker connectivity at ${KAFKA_BOOTSTRAP}..."
python3 - <<'PY'
import os
import socket
import sys
import time

host, port = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092").split(":")
for attempt in range(60):
    try:
        with socket.create_connection((host, int(port)), timeout=2):
            print("Kafka connection established successfully.")
            sys.exit(0)
    except OSError:
        time.sleep(2)
print("Kafka broker is unreachable.", file=sys.stderr)
sys.exit(1)
PY

echo
echo "=== Step 1/5: Running Ingestion Event Producer ==="
python3 src/ingestion/producer.py

echo
echo "=== Step 2/5: Executing Streaming Consumer ==="
# Under Docker setup, we can submit utilizing the local spark-submit wrapper
# packages are loaded natively or pre-cached in environment setup
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 src/ingestion/consumer.py

echo
echo "=== Step 3/5: Running Data Profiling (EDA) ==="
python3 src/processing/eda.py

echo
echo "=== Step 4/5: Running Analytical Transformations ==="
python3 src/processing/transforms.py

echo
echo "=== Step 5/5: Registering Hive Catalog Tables ==="
python3 src/processing/hive_loader.py

echo
echo "Pipeline execution completed. Hive tables published under database 'target_retail'."
echo "To query the database interactive shell, run:"
echo "  docker compose exec pipeline python3 src/processing/query_hive.py"
