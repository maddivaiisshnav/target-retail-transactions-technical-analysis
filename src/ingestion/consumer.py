"""
Kafka CONSUMER - reads the topic and hands the batch to Spark.

This is the literal implementation of the brief:
  "Create a Spark DataFrame by appending all the data read from Kafka."

READ THE WARNING AT THE BOTTOM OF THIS FILE. This approach is correct for
learning and WRONG for production; streaming_consumer.py shows the real way.

KEY CONCEPTS
------------
offset          a bookmark. "I have read up to message #4812 on partition 2."
                Kafka stores it per consumer-group, so a crashed consumer
                resumes exactly where it stopped instead of replaying or
                skipping.

consumer group  a set of consumers cooperating on one topic. Kafka assigns
                each partition to exactly ONE member of the group, so each
                message is processed once by the group. Add a consumer and
                Kafka rebalances automatically - that is how you scale out.
                A topic with 3 partitions supports at most 3 useful consumers
                in a group; a 4th sits idle.

auto_offset_reset
                what to do when the group has NO stored offset (first run).
                'earliest' -> start from the beginning of the topic.
                'latest'   -> only messages arriving from now on.
                The brief says "start the consumer before the producer" purely
                because the default is 'latest'. We use 'earliest' instead,
                which removes the race entirely and lets us REPLAY at will.
                That replayability is Kafka's superpower over a normal queue:
                Kafka does not delete a message when it is read.
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "processing"))

from common import ORDERS_SCHEMA, clean, get_spark
from kafka import KafkaConsumer
from pyspark.sql import functions as F

TOPIC = "target_orders"
# "kafka:9092" when run inside Docker Compose, "localhost:9092" on a bare-metal broker.
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")


def consume(group: str, timeout_ms: int, from_beginning: bool):
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=group,
        auto_offset_reset="earliest" if from_beginning else "latest",

        # Mirror of the producer's serializer: bytes -> Python dict.
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,

        # Stop iterating after this long with no new messages, so this script
        # terminates instead of blocking forever. A real consumer runs forever.
        consumer_timeout_ms=timeout_ms,

        # False = WE decide when an offset is committed, after the data is
        # safely handled. With enable_auto_commit=True, Kafka commits on a
        # timer, so a crash between "offset committed" and "data written"
        # loses messages permanently. Manual commit gives at-least-once
        # delivery, which is what you want for sales data.
        enable_auto_commit=False,
    )

    print(f"Consumer '{group}' listening on '{TOPIC}' "
          f"({'earliest' if from_beginning else 'latest'})...")

    records = []
    per_partition = {}
    for msg in consumer:
        records.append(msg.value)
        per_partition[msg.partition] = per_partition.get(msg.partition, 0) + 1
        if len(records) % 200 == 0:
            print(f"  consumed {len(records)} messages")

    # Commit only after we have everything in hand.
    if records:
        consumer.commit()
    consumer.close()

    print(f"\nConsumed {len(records)} messages total.")
    print("Messages per partition (proof the key-based routing worked):")
    for part in sorted(per_partition):
        print(f"  partition {part}: {per_partition[part]}")
    return records


def to_spark(records):
    spark = get_spark("TargetPipeline-KafkaConsumer")
    if not records:
        print("No messages - is the producer running? Exiting.")
        spark.stop()
        return

    # Kafka gave us JSON, so every value arrived as a string/number with no
    # type guarantees. We create the DataFrame with all-string columns, then
    # CAST to the authoritative schema. This is exactly the "check the schema
    # and correct the data types" step from the brief - and it is why that
    # step exists: a transport layer carries bytes, never types.
    # Treat the null-ish tokens that survive a JSON round-trip as real nulls.
    # We fixed the root cause in the producer, but a consumer must never trust
    # that its upstream is well behaved - defence in depth.
    NULLISH = {"", "nan", "none", "null", "na", "n/a"}
    rows = [
        {k: (None if v is None or str(v).strip().lower() in NULLISH else str(v))
         for k, v in r.items()}
        for r in records
    ]
    raw = spark.createDataFrame(rows)

    print("\nSchema as it arrives from Kafka (everything is a string):")
    raw.printSchema()

    # try_cast, not cast. Spark 4 defaults to ANSI mode, where a bad cast
    # throws and kills the whole job. try_cast returns NULL instead, so one
    # malformed record cannot take down a batch of a million good ones.
    # The trade-off is that failures become silent - so we COUNT them below
    # rather than letting them disappear.
    typed = raw.select(*[
        F.expr(f"try_cast({f.name} AS {f.dataType.simpleString()})").alias(f.name)
        for f in ORDERS_SCHEMA.fields
    ])
    print("Schema after casting to the declared contract:")
    typed.printSchema()

    # Because try_cast fails silently, we must audit it. A row that had a
    # value before the cast and NULL after it is a cast failure, not real
    # missing data - and it needs to be visible.
    bad_id = typed.filter(F.col("Id").isNull()).count()
    if bad_id:
        print(f"WARNING: {bad_id} messages failed to cast their Id - investigate")

    df = clean(typed)
    print(f"Rows in the Spark DataFrame: {df.count()}")
    df.select("Id", "order_status", "customer_city", "customer_state",
              "order_total_value", "delivery_days").show(10, truncate=False)

    # Persist as the bronze/raw landing zone: the immutable record of what
    # arrived. Everything downstream is rebuilt from here.
    out = "output/bronze/orders_from_kafka"
    df.write.mode("overwrite").parquet(out)
    print(f"Written to {out}")
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--group", default="target-analytics")
    p.add_argument("--timeout-ms", type=int, default=10000)
    p.add_argument("--latest", action="store_true",
                   help="only read messages produced after startup")
    a = p.parse_args()
    to_spark(consume(a.group, a.timeout_ms, from_beginning=not a.latest))

# ---------------------------------------------------------------------------
# WHY THIS PATTERN DOES NOT SCALE
# ---------------------------------------------------------------------------
# We accumulated every message into a Python list on ONE machine, then handed
# that list to Spark. At 200 GB this fails immediately - it funnels all data
# through a single process's memory, which is precisely what Spark exists to
# avoid.
#
# It also has no fault tolerance: crash halfway and the in-memory list is gone,
# and because we had not committed offsets we would reprocess everything.
#
# The real answer is Spark Structured Streaming reading Kafka directly, so the
# Spark executors pull from the partitions in parallel and checkpoint their
# progress. See streaming_consumer.py.
# ---------------------------------------------------------------------------
