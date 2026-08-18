"""
Kafka event producer for sales transaction ingestion.

Simulates transaction events by streaming retail records into a partitioned 
Kafka topic. Ensures event sorting within geographic partitions by routing 
transactions utilizing keys.
"""

import argparse
import json
import os
import time

import pandas as pd
from kafka import KafkaProducer

TOPIC = "target_orders"
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
CSV_PATH = "data/target_orders.csv"

def build_producer() -> KafkaProducer:
    """Builds and configures a resilient KafkaProducer client."""
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        # Serialize payloads into UTF-8 JSON format
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        
        # Ensure all replication layers acknowledge event writes
        acks="all",
        
        # Resiliency configurations
        retries=3,
        
        # Performance configurations (batching)
        linger_ms=5,
    )

def main(rate: float, limit: int | None):
    # Load orders into pandas dataframe
    df = pd.read_csv(CSV_PATH)
    if limit:
        df = df.head(limit)

    # Convert pandas NaN values to Python None for compliant JSON serialization
    df = df.astype(object).where(pd.notna(df), None)

    producer = build_producer()
    print(f"Streaming {len(df)} orders to Kafka topic '{TOPIC}' at {BOOTSTRAP}...")

    sent_count = 0
    for record in df.to_dict(orient="records"):
        # Route events using customer state to guarantee ordering within partitions
        partition_key = record.get("customer_state", "UNKNOWN")

        producer.send(TOPIC, key=partition_key, value=record)
        sent_count += 1

        if sent_count % 100 == 0:
            print(f"  Pushed {sent_count}/{len(df)} records")
        
        if rate > 0:
            time.sleep(rate)

    # Flush all remaining buffered payloads prior to shutdown
    producer.flush()
    producer.close()
    print(f"Ingestion complete. {sent_count} records processed successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=0.0,
                        help="Cooldown sleep seconds between transactions")
    parser.add_argument("--limit", type=int, default=None,
                        help="Ingest constraint limit count")
    args = parser.parse_args()
    
    main(args.rate, args.limit)
