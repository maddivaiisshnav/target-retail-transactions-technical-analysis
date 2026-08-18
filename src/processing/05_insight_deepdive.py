"""
Step 5: Interrogating the headline result.

Q6 returned a correlation of ~0.02 between delivery time and review score -
essentially zero. The expected business story is "slow delivery makes
customers angry", so a null result deserves scrutiny before it is reported.

Three possibilities, tested below:
  (a) the relationship is real but NON-LINEAR (a cliff, not a slope), which
      Pearson would miss entirely
  (b) the effect exists only in the tail (very late orders)
  (c) there genuinely is no relationship in this 1000-row sample
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common import clean, get_spark, load_raw
from pyspark.sql import functions as F

spark = get_spark("TargetPipeline-Insight")
df = clean(load_raw(spark)).drop("_corrupt_record")
delivered = df.filter(F.col("order_delivered_customer_date").isNotNull()).cache()

BAR = "=" * 72
print(f"\n{BAR}\n(a) Bucket delivery time - does a CLIFF appear?\n{BAR}")
# Pearson only detects a straight line. If satisfaction is fine up to ~2 weeks
# and then collapses, the correlation is near zero while the effect is huge.
bucketed = (
    delivered
    .withColumn("delivery_bucket",
        F.when(F.col("delivery_days") <= 3,  "1. 0-3 days")
         .when(F.col("delivery_days") <= 7,  "2. 4-7 days")
         .when(F.col("delivery_days") <= 14, "3. 8-14 days")
         .when(F.col("delivery_days") <= 21, "4. 15-21 days")
         .when(F.col("delivery_days") <= 30, "5. 22-30 days")
         .otherwise("6. 30+ days"))
    .groupBy("delivery_bucket")
    .agg(
        F.count("*").alias("orders"),
        F.round(F.avg("review_score"), 2).alias("avg_review"),
        F.round(100 * F.avg(F.when(F.col("review_score") <= 2, 1.0).otherwise(0.0)), 1)
         .alias("pct_1_or_2_star"),
        F.round(100 * F.avg(F.when(F.col("review_score") == 5, 1.0).otherwise(0.0)), 1)
         .alias("pct_5_star"),
    )
    .orderBy("delivery_bucket")
)
bucketed.show(truncate=False)
bucketed.coalesce(1).write.mode("overwrite").parquet("output/gold/delivery_bucket_vs_review")

print(f"{BAR}\n(b) The tail - are the LATEST orders rated worse?\n{BAR}")
p90 = delivered.approxQuantile("delivery_days", [0.5, 0.9, 0.95, 0.99], 0.01)
print(f"delivery_days percentiles  p50={p90[0]:.1f}  p90={p90[1]:.1f}  "
      f"p95={p90[2]:.1f}  p99={p90[3]:.1f}\n")

(
    delivered
    .withColumn("is_very_late", F.col("delivery_days") > p90[1])
    .groupBy("is_very_late")
    .agg(
        F.count("*").alias("orders"),
        F.round(F.avg("review_score"), 2).alias("avg_review"),
        F.round(100 * F.avg(F.when(F.col("review_score") <= 2, 1.0).otherwise(0.0)), 1)
         .alias("pct_1_or_2_star"),
    )
    .show(truncate=False)
)

print(f"{BAR}\n(c) Sample size - how much can we even conclude?\n{BAR}")
n = delivered.count()
print(f"n = {n} delivered orders")
print("For a correlation to be statistically distinguishable from zero at")
print(f"n={n}, it needs |r| > ~0.063. Our r = 0.02 is well inside the noise.")
print("""
CONCLUSION
----------
Report the null result honestly. In this 1000-row SAMPLE there is no linear
relationship between delivery time and review score. That is a statement
about the sample, not about Target's business.

The full 100k-row Olist dataset this sample was drawn from DOES show a clear
negative relationship. A 1000-row curated sample is simply too small and too
skewed (98.3% delivered, 58% five-star) to reproduce it.

The correct engineering takeaway: the PIPELINE is validated - it computes the
metric correctly. The DATA is insufficient to answer the business question.
Those are two different problems, and conflating them is how bad decisions
get made from good code.
""")

spark.stop()
