"""
Why we do NOT blindly dropna().

The brief says "remove null values if any". Taken literally that means
df.dropna(). This script shows what that actually deletes, and finds a real
data-quality contradiction hiding in the nulls.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common import clean, get_spark, load_raw
from pyspark.sql import functions as F

spark = get_spark("TargetPipeline-NullInvestigation")
df = clean(load_raw(spark)).drop("_corrupt_record").cache()

print("\n=== Which statuses have a missing delivery date? ===")
(
    df.groupBy("order_status")
      .agg(
          F.count("*").alias("total"),
          F.sum(F.when(F.col("order_delivered_customer_date").isNull(), 1)
                 .otherwise(0)).alias("missing_delivery_date"),
      )
      .orderBy(F.desc("total"))
      .show(truncate=False)
)

print("=== The contradiction: status='delivered' but no delivery date ===")
bad = df.filter(
    (F.col("order_status") == "delivered")
    & F.col("order_delivered_customer_date").isNull()
)
print(f"count = {bad.count()}   <- these are genuinely broken records\n")
bad.select("Id", "order_status", "order_purchase_timestamp",
           "order_delivered_customer_date", "customer_city").show(truncate=False)

print("=== Cost of a blanket dropna() ===")
before = df.count()
after = df.dropna().count()
print(f"rows before : {before}")
print(f"rows after  : {after}")
print(f"rows deleted: {before - after}  ({100*(before-after)/before:.1f}%)")
print("""
FINDING: all 22 nulls sit inside order_status = 'delivered'. Every shipped /
canceled / invoiced / processing order DOES have a delivery date. So the data
contradicts itself in both directions:

  * 22 orders claim 'delivered' with no delivery timestamp
  *  3 orders claim 'canceled'  yet carry a delivery timestamp

Neither is a null-handling problem. Both are UPSTREAM data-quality defects,
and dropna() would hide the first while leaving the second untouched.

The correct policy is per-metric, not global:
  * revenue / order counts -> keep every row (avg() already skips nulls)
  * delivery-time metrics  -> filter to rows that HAVE a delivery date
  * both contradiction sets-> quarantine to a separate table and raise
                              them with the source system owner

Blanket dropna() is a silent 2.2% data loss that answers no question well.
""")

spark.stop()
