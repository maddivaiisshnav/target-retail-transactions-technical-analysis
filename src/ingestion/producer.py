"""
Kafka PRODUCER - streams the sales CSV into the topic, one order per message.

WHAT A PRODUCER IS
------------------
A producer writes messages to a Kafka TOPIC. It does not know or care who
reads them - zero, one, or fifty consumers may exist. That decoupling is the
entire point of Kafka. The store's point-of-sale system should not need to be
reconfigured because the analytics team added a fraud-detection consumer.

HONEST NOTE ON REALISM
----------------------
Reading a static CSV and pushing it to Kafka is a SIMULATION. Nobody does this
in production. In a real Target, this stream would come from:
  - the POS application emitting an event per transaction, or
  - Debezium / CDC tailing the operational database's write-ahead log, or
  - Kafka Connect pulling from an upstream system.
The mechanics you learn here are identical; only the source differs.
"""

import argparse
import json
import os
import time

import pandas as pd
from kafka import KafkaProducer

TOPIC = "target_orders"
# "kafka:9092" when run inside Docker Compose, "localhost:9092" on a bare-metal broker.
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
CSV = "data/target_orders.csv"


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,

        # Kafka moves BYTES. It has no idea what a Python dict is. A serializer
        # converts our object to bytes on the way out. We use JSON because it
        # is readable while learning.
        #
        # Production would use Avro or Protobuf with a SCHEMA REGISTRY instead:
        # they are far more compact (no repeated field names in every message)
        # and the registry REJECTS a producer trying to publish a message that
        # would break existing consumers. JSON gives you no such protection.
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),

        # acks='all' -> do not consider a write successful until every in-sync
        # replica has it. acks=0 is fire-and-forget (fast, loses data), acks=1
        # waits only for the leader (loses data if the leader dies before
        # replication). For financial/sales data, 'all' is the only defensible
        # choice. This is the classic durability-vs-throughput trade-off.
        acks="all",

        # Retry transient failures rather than dropping the message.
        retries=3,

        # Batch messages for up to 5ms before sending. Batching is why Kafka
        # is fast: one network round-trip carries many messages instead of one.
        linger_ms=5,
    )


def main(rate: float, limit: int | None):
    df = pd.read_csv(CSV)
    if limit:
        df = df.head(limit)

    # CRITICAL: pandas represents a missing value as NaN, which is a FLOAT,
    # not None. json.dumps(float('nan')) emits the bare token NaN - which is
    # not even legal JSON - and any consumer reading it back gets the string
    # "nan" instead of a null. Downstream, casting "nan" to TIMESTAMP throws
    # under Spark's ANSI mode.
    #
    # This is the classic "null semantics do not survive serialization" bug.
    # Fix it at the SOURCE: convert NaN to None so it serializes as JSON null.
    df = df.astype(object).where(pd.notna(df), None)

    producer = build_producer()
    print(f"Producing {len(df)} orders to topic '{TOPIC}' at {BOOTSTRAP}")

    sent = 0
    for record in df.to_dict(orient="records"):
        # THE KEY MATTERS. Kafka hashes the key to choose a partition, so all
        # messages with the same key land on the same partition, and ordering
        # is guaranteed WITHIN a partition (never across partitions).
        #
        # Keying by customer_state means every event for SP is ordered
        # relative to other SP events - which is what a stateful consumer
        # doing per-state aggregation needs.
        #
        # With no key, Kafka round-robins and you lose all ordering guarantees.
        key = record.get("customer_state", "UNKNOWN")

        producer.send(TOPIC, key=key, value=record)
        sent += 1

        if sent % 100 == 0:
            print(f"  sent {sent}/{len(df)}")
        if rate > 0:
            time.sleep(rate)

    # flush() blocks until every buffered message is actually acknowledged.
    # Skipping this is the #1 beginner bug: the script exits, the buffer is
    # discarded, and messages silently vanish.
    producer.flush()
    producer.close()
    print(f"DONE. {sent} messages produced and acknowledged.")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--rate", type=float, default=0.0,
                   help="seconds to sleep between messages (0 = as fast as possible)")
    p.add_argument("--limit", type=int, default=None,
                   help="only send the first N rows")
    a = p.parse_args()
    main(a.rate, a.limit)
