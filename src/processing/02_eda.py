"""
Step 2: Exploratory Data Analysis.

EDA comes BEFORE transformation for one reason: you cannot write a correct
aggregation over data whose shape you have not checked. Every question here
is really "what am I allowed to assume?"

Answers the six EDA questions in the brief.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common import clean, get_spark, load_raw
from pyspark.sql import functions as F

spark = get_spark("TargetPipeline-EDA")

raw = load_raw(spark)
df = clean(raw)

# cache(): this DataFrame is used by ~8 separate actions below. Without a
# cache, Spark re-reads and re-cleans the CSV from scratch for EVERY action,
# because transformations are lazy and nothing is stored by default.
# cache() says "after you compute this once, keep it in memory."
df.cache()

BAR = "=" * 72
def header(n, q):
    print(f"\n{BAR}\nEDA {n}: {q}\n{BAR}")

total = df.count()
print(f"\nTotal rows loaded: {total}")

# ---------------------------------------------------------------------------
# Sanity check FIRST: did any row fail to parse against our schema?
# ---------------------------------------------------------------------------
corrupt = df.filter(F.col("_corrupt_record").isNotNull()).count()
print(f"Rows that failed schema parsing: {corrupt}")

# ---------------------------------------------------------------------------
header(4, "Are there any missing values? (asked 4th, but must be answered 1st)")
# ---------------------------------------------------------------------------
# Built as a single aggregation so Spark makes ONE pass over the data instead
# of one pass per column. Column-at-a-time would be 12 full scans.
null_counts = df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df.columns if c != "_corrupt_record"
]).collect()[0].asDict()

print(f"{'column':<34}{'nulls':>8}{'pct':>9}")
print("-" * 51)
for col, n in null_counts.items():
    if n:
        print(f"{col:<34}{n:>8}{100*n/total:>8.1f}%")
print("\n(columns not listed have zero nulls)")

# ---------------------------------------------------------------------------
header(1, "Mean of order_products_value and order_freight_value")
# ---------------------------------------------------------------------------
# avg() ignores nulls by design - it divides by the count of NON-null values.
# That is what we want, and it is why a blanket dropna() is unnecessary here.
df.select(
    F.round(F.avg("order_products_value"), 2).alias("mean_products_value"),
    F.round(F.avg("order_freight_value"), 2).alias("mean_freight_value"),
    F.round(F.avg("order_total_value"), 2).alias("mean_total_value"),
).show()

# ---------------------------------------------------------------------------
header(2, "Distribution of order_status")
# ---------------------------------------------------------------------------
header(6, "...which is the same question as 'percent delivered / cancelled'")
(
    df.groupBy("order_status")
      .agg(F.count("*").alias("order_count"))
      # a window-free way to get percentages: divide by the total we already have
      .withColumn("pct_of_orders", F.round(100 * F.col("order_count") / total, 2))
      .orderBy(F.desc("order_count"))
      .show(truncate=False)
)

# ---------------------------------------------------------------------------
header(3, "How many unique states?")
# ---------------------------------------------------------------------------
n_states = df.select("customer_state").distinct().count()
print(f"Unique states: {n_states}\n")
(
    df.groupBy("customer_state")
      .agg(F.count("*").alias("orders"))
      .orderBy(F.desc("orders"))
      .show(10, truncate=False)
)

# ---------------------------------------------------------------------------
header(5, "Top 5 cities by number of orders")
# ---------------------------------------------------------------------------
print(">>> On the CLEANED city column (casing normalised):")
(
    df.groupBy("customer_city")
      .agg(F.count("*").alias("orders"))
      .orderBy(F.desc("orders"))
      .show(5, truncate=False)
)

print(">>> On the RAW city column, to show what the bug would have cost:")
(
    raw.groupBy("customer_city")
       .agg(F.count("*").alias("orders"))
       .orderBy(F.desc("orders"))
       .show(5, truncate=False)
)

df.unpersist()
spark.stop()
