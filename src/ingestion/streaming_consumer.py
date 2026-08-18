"""
Spark STRUCTURED STREAMING consumer - how this is actually done in production.

DIFFERENCE FROM consumer.py
---------------------------
consumer.py            Spark executors read Kafka partitions IN PARALLEL.
  Kafka -> python      No single-machine bottleneck. Scales to any volume.
  list -> ONE machine
  -> Spark             Progress is CHECKPOINTED to disk, so a crash resumes
                       from the exact offset it left off - no data loss, no
  <- bottleneck        duplicates in the output sink.

THE CORE IDEA OF STRUCTURED STREAMING
-------------------------------------
Treat the stream as an unbounded TABLE that keeps growing. You write the same
DataFrame code you would write for a batch job; Spark works out how to run it
incrementally over each new micro-batch. There is no separate streaming API to
learn - readStream instead of read, writeStream instead of write. That is it.

RUN THIS WITH:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
        src/ingestion/streaming_consumer.py

The --packages flag pulls the Kafka connector JAR. It is not bundled with
PySpark because Spark ships a small core and loads connectors on demand.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "processing"))

from common import ORDERS_SCHEMA, clean, get_spark
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

TOPIC = "target_orders"
# "kafka:9092" when run inside Docker Compose, "localhost:9092" on a bare-metal broker.
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
CHECKPOINT = "output/checkpoints/orders_stream"
BRONZE = "output/bronze/orders_streaming"

spark = get_spark("TargetPipeline-StructuredStreaming")

# ---------------------------------------------------------------------------
# readStream: define the SOURCE. Nothing executes yet - still lazy.
# ---------------------------------------------------------------------------
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    # startingOffsets is the streaming equivalent of auto_offset_reset.
    # 'earliest' replays the whole topic; 'latest' takes only new messages.
    .option("startingOffsets", "earliest")
    # Cap per micro-batch so one huge backlog cannot produce a batch too big
    # for the cluster. Backpressure control.
    .option("maxOffsetsPerTrigger", 500)
    .load()
)

# Kafka's built-in schema is always the same 7 columns. The payload is in
# `value` as raw BYTES - Kafka itself is entirely schema-agnostic.
print("Kafka source schema (fixed, always these columns):")
raw.printSchema()

# ---------------------------------------------------------------------------
# Parse the JSON payload. Everything is read as STRING first, then cast, for
# the same reason as in the batch consumer: the transport carries no types.
# ---------------------------------------------------------------------------
json_as_strings = StructType([StructField(f.name, StringType(), True)
                              for f in ORDERS_SCHEMA.fields])

parsed = (
    raw
    .select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ingest_time"),
        F.from_json(F.col("value").cast("string"), json_as_strings).alias("d"),
    )
    .select("kafka_key", "kafka_partition", "kafka_offset",
            "kafka_ingest_time", "d.*")
)

typed = parsed.select(
    "kafka_key", "kafka_partition", "kafka_offset", "kafka_ingest_time",
    *[F.expr(f"try_cast({f.name} AS {f.dataType.simpleString()})").alias(f.name)
      for f in ORDERS_SCHEMA.fields],
)

enriched = clean(typed)

# ---------------------------------------------------------------------------
# SINK 1: append every record to the bronze layer as Parquet.
#
# checkpointLocation is the critical option. Spark writes the Kafka offsets it
# has processed there. Delete it and the stream restarts from scratch; keep it
# and a crashed job resumes at the exact record it stopped on. Without a
# checkpoint there is NO fault tolerance.
#
# trigger(availableNow=True) processes everything currently in the topic, then
# stops. It is how you run a "streaming" job on a schedule - increasingly the
# default in practice, because always-on clusters are expensive and most
# businesses do not truly need sub-second freshness.
# Swap for processingTime='30 seconds' for a continuously running job.
# ---------------------------------------------------------------------------
bronze_query = (
    enriched.writeStream
    .format("parquet")
    .option("path", BRONZE)
    .option("checkpointLocation", CHECKPOINT)
    .outputMode("append")
    .trigger(availableNow=True)
    .start()
)

# ---------------------------------------------------------------------------
# SINK 2: a live running aggregation, printed to the console.
#
# This is a STATEFUL operation: Spark keeps the running totals per state in
# memory across micro-batches and updates them as data arrives. That state is
# also checkpointed.
#
# outputMode('complete') reprints the full result table each batch. Only legal
# for aggregations, and only safe when the result set is small (26 states).
# For an unbounded key space you would use 'update' plus a watermark.
# ---------------------------------------------------------------------------
console_query = (
    enriched.groupBy("customer_state")
            .agg(
                F.count("*").alias("orders"),
                F.round(F.sum("order_total_value"), 2).alias("running_sales"),
            )
            .orderBy(F.desc("running_sales"))
            .writeStream
            .format("console")
            .outputMode("complete")
            .option("truncate", False)
            .option("numRows", 10)
            .trigger(availableNow=True)
            .start()
)

print("\nStreaming started. Processing available data...\n")
bronze_query.awaitTermination()
console_query.awaitTermination()

print("\nStream finished.")
print(f"  bronze parquet : {BRONZE}")
print(f"  checkpoint     : {CHECKPOINT}")
print("Re-run this script and it will process ZERO rows - the checkpoint")
print("remembers what was already consumed. That is exactly-once in action.")

spark.stop()
