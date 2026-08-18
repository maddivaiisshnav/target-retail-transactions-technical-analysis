"""
Data profiling and exploratory data analysis.

Performs automated profiling on the raw transactions dataset to validate schemas,
check null value percentages, inspect order status distributions, and analyze
geographical metrics.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common import clean, get_spark, load_raw
from pyspark.sql import functions as F

def main():
    spark = get_spark("TargetPipeline-EDA")
    
    raw = load_raw(spark)
    df = clean(raw)
    df.cache()

    total_rows = df.count()
    print(f"\nTotal rows loaded: {total_rows}")

    # Check for schema violations or unparseable records
    corrupt_count = df.filter(F.col("_corrupt_record").isNotNull()).count()
    print(f"Malformed records (failed schema validation): {corrupt_count}")

    # Profile null distributions across columns
    print("\nNull values distribution:")
    null_counts = df.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c)
        for c in df.columns if c != "_corrupt_record"
    ]).collect()[0].asDict()

    print(f"{'Column':<34}{'Null Count':>12}{'Percentage':>12}")
    print("-" * 58)
    for col, count in null_counts.items():
        if count:
            print(f"{col:<34}{count:>12}{100 * count / total_rows:>11.1f}%")

    # Order status distribution
    print("\nOrder Status Distribution:")
    (
        df.groupBy("order_status")
          .agg(F.count("*").alias("order_count"))
          .withColumn("percentage", F.round(100 * F.col("order_count") / total_rows, 2))
          .orderBy(F.desc("order_count"))
          .show(truncate=False)
    )

    # Unique states count and distribution
    unique_states = df.select("customer_state").distinct().count()
    print(f"Unique customer states: {unique_states}\n")
    (
        df.groupBy("customer_state")
          .agg(F.count("*").alias("order_count"))
          .orderBy(F.desc("order_count"))
          .show(10, truncate=False)
    )

    # Top cities by order count
    print("\nTop 5 Cities by Order Volume:")
    (
        df.groupBy("customer_city")
          .agg(F.count("*").alias("order_count"))
          .orderBy(F.desc("order_count"))
          .show(5, truncate=False)
    )

    df.unpersist()
    spark.stop()

if __name__ == "__main__":
    main()
