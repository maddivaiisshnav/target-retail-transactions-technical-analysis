"""
Production-ready Spark Structured Streaming consumer.

This script executes parallelized partition ingestion from Kafka, casting columns
to the authoritative schema contract, cleaning grouping keys, and writing to the
bronze layer with fault-tolerant checkpointing.
"""

import os
import sys

# Ensure processing modules can be imported
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "processing"))

from common import ORDERS_SCHEMA, clean, get_spark
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

TOPIC = "target_orders"
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
CHECKPOINT = "output/checkpoints/orders_stream"
BRONZE = "output/bronze/orders_streaming"

def main():
    spark = get_spark("TargetPipeline-StructuredStreaming")

    # Connect to the Kafka stream source
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 500)
        .load()
    )

    # Convert the value binary payload into schema fields as strings
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

    # Cast to schema datatypes defensively
    typed = parsed.select(
        "kafka_key", "kafka_partition", "kafka_offset", "kafka_ingest_time",
        *[F.expr(f"try_cast({f.name} AS {f.dataType.simpleString()})").alias(f.name)
          for f in ORDERS_SCHEMA.fields],
    )

    # Run clean and enrichment steps
    enriched = clean(typed)

    # Sink 1: Append micro-batches to the bronze parquet store
    bronze_query = (
        enriched.writeStream
        .format("parquet")
        .option("path", BRONZE)
        .option("checkpointLocation", CHECKPOINT)
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )

    # Sink 2: Print aggregates to console for verification
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

    print("\nStreaming pipeline started. Consuming messages...\n")
    bronze_query.awaitTermination()
    console_query.awaitTermination()

    print("\nStream consumption complete.")
    spark.stop()

if __name__ == "__main__":
    main()
