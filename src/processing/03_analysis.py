"""
Step 3: Data processing / business analysis.

Answers the six processing questions in the brief, and writes each result out
as a Parquet "gold" table under output/gold/ so the Hive step can register
them without recomputing anything.

Design note: every question is a small function returning a DataFrame. That
makes each one independently testable, which is how real pipeline code is
written - one giant script is impossible to unit test.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common import clean, get_spark, load_raw
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

GOLD = "output/gold"
BAR = "=" * 72


def banner(n, q):
    print(f"\n{BAR}\nQ{n}: {q}\n{BAR}")


def write_gold(df: DataFrame, name: str) -> DataFrame:
    """Persist a result table as Parquet.

    Parquet, not CSV, because it is COLUMNAR: reading one column reads only
    that column's bytes, it compresses far better, and it stores min/max
    statistics per row-group so engines can skip files that cannot match a
    filter (predicate pushdown). CSV has none of that.

    coalesce(1) merges the output into a single file. Correct here because
    these result tables are tiny; on real data you would NOT do this, as it
    forces all data through one task and destroys parallelism.
    """
    df.coalesce(1).write.mode("overwrite").parquet(f"{GOLD}/{name}")
    return df


spark = get_spark("TargetPipeline-Analysis")

df = clean(load_raw(spark)).drop("_corrupt_record")
df.cache()

# A dedicated view for delivery analysis. Rows without a delivery date cannot
# contribute to a delivery-time metric - including them would silently treat
# "unknown" as if it were a data point.
delivered = df.filter(F.col("order_delivered_customer_date").isNotNull())

# Spark SQL: register the DataFrame as a temp view so we can also use raw SQL.
# DataFrame API and SQL compile to the SAME optimised plan - pick whichever
# reads more clearly for the question at hand.
df.createOrReplaceTempView("orders")
delivered.createOrReplaceTempView("orders_delivered")


# ---------------------------------------------------------------------------
banner(1, "Total sales in each customer city")
# ---------------------------------------------------------------------------
# 'Total sales' is ambiguous - product value only, or including freight?
# We report both and label them, rather than picking silently. Ambiguity in a
# spec should surface as an extra column, never as a hidden assumption.
#
# Cancelled orders are excluded from revenue: a cancelled order is not sales.
city_sales = (
    df.filter(F.col("order_status") != "canceled")
      .groupBy("customer_city", "customer_state")
      .agg(
          F.count("*").alias("order_count"),
          F.round(F.sum("order_products_value"), 2).alias("total_product_sales"),
          F.round(F.sum("order_freight_value"), 2).alias("total_freight"),
          F.round(F.sum("order_total_value"), 2).alias("total_sales"),
          F.round(F.avg("order_total_value"), 2).alias("avg_order_value"),
      )
      .orderBy(F.desc("total_sales"))
)
write_gold(city_sales, "city_sales")
city_sales.show(10, truncate=False)
print(f"cities with sales: {city_sales.count()}")

# Same thing at state level - what an exec actually looks at.
state_sales = (
    df.filter(F.col("order_status") != "canceled")
      .groupBy("customer_state")
      .agg(
          F.count("*").alias("order_count"),
          F.round(F.sum("order_total_value"), 2).alias("total_sales"),
          F.round(F.avg("order_total_value"), 2).alias("avg_order_value"),
      )
      .orderBy(F.desc("total_sales"))
)
write_gold(state_sales, "state_sales")
print("\nBy state:")
state_sales.show(10, truncate=False)


# ---------------------------------------------------------------------------
banner(2, "Correlation between order value, freight and item quantity")
# ---------------------------------------------------------------------------
# corr() = Pearson correlation: -1 perfectly inverse, 0 none, +1 perfectly
# linear. It measures LINEAR association only, and it is NOT causation.
pairs = [
    ("order_products_value", "order_freight_value"),
    ("order_products_value", "order_items_qty"),
    ("order_freight_value",  "order_items_qty"),
]
corr_rows = [(a, b, round(df.stat.corr(a, b), 4)) for a, b in pairs]
corr_df = spark.createDataFrame(corr_rows, ["metric_a", "metric_b", "pearson_corr"])
write_gold(corr_df, "value_freight_qty_correlation")
corr_df.show(truncate=False)


# ---------------------------------------------------------------------------
banner(3, "Average order delivery time and approval time")
# ---------------------------------------------------------------------------
# delivery_days   = purchase -> in customer's hands  (customer experience)
# approval_hours  = purchase -> retailer approved    (internal ops)
# Reported in different units because they live on different scales; forcing
# both into "days" would render approval time as 0.02 and hide all signal.
#
# We report the MEDIAN alongside the mean. Delivery times are right-skewed -
# a handful of 200-day outliers drags the mean well above typical experience.
# The median is what a customer actually experiences.
timing = delivered.select(
    F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"),
    F.round(F.expr("percentile_approx(delivery_days, 0.5)"), 2).alias("median_delivery_days"),
    F.round(F.min("delivery_days"), 2).alias("min_delivery_days"),
    F.round(F.max("delivery_days"), 2).alias("max_delivery_days"),
    F.round(F.avg("approval_hours"), 2).alias("avg_approval_hours"),
    F.round(F.expr("percentile_approx(approval_hours, 0.5)"), 2).alias("median_approval_hours"),
)
write_gold(timing, "delivery_timing_summary")
timing.show(truncate=False)


# ---------------------------------------------------------------------------
banner(4, "Average review score per order")
# ---------------------------------------------------------------------------
# In this dataset one row IS one order, so 'average review score per order'
# is just the average review score. We report the overall average plus the
# full score distribution, which is far more informative than a single mean:
# review scores are usually bimodal (lots of 5s, lots of 1s, few 3s), and a
# mean of 4.0 can describe two completely different businesses.
overall_review = df.select(
    F.round(F.avg("review_score"), 3).alias("avg_review_score"),
    F.count("review_score").alias("reviews_counted"),
)
overall_review.show(truncate=False)

review_dist = (
    df.groupBy("review_score")
      .agg(F.count("*").alias("num_orders"))
      .withColumn("pct", F.round(100 * F.col("num_orders") / df.count(), 2))
      .orderBy("review_score")
)
write_gold(review_dist, "review_score_distribution")
review_dist.show(truncate=False)

# Average review per city - actionable: which markets are unhappy?
# min_orders guard: a city with 1 order and a 1-star review is noise, not a
# signal. Filtering low-volume groups before ranking is essential; without it
# every "top/bottom N" list is dominated by tiny samples.
MIN_ORDERS = 5
review_by_city = (
    df.groupBy("customer_city")
      .agg(
          F.count("*").alias("order_count"),
          F.round(F.avg("review_score"), 2).alias("avg_review_score"),
      )
      .filter(F.col("order_count") >= MIN_ORDERS)
      .orderBy("avg_review_score")
)
write_gold(review_by_city, "review_by_city")
print(f"\nWorst-rated cities (min {MIN_ORDERS} orders):")
review_by_city.show(5, truncate=False)
print(f"Best-rated cities (min {MIN_ORDERS} orders):")
review_by_city.orderBy(F.desc("avg_review_score")).show(5, truncate=False)


# ---------------------------------------------------------------------------
banner(5, "Top 3 cities with fastest and slowest delivery times")
# ---------------------------------------------------------------------------
city_delivery = (
    delivered.groupBy("customer_city", "customer_state")
             .agg(
                 F.count("*").alias("delivered_orders"),
                 F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"),
                 F.round(F.avg("review_score"), 2).alias("avg_review_score"),
             )
             .filter(F.col("delivered_orders") >= MIN_ORDERS)
)
write_gold(city_delivery, "city_delivery_performance")

print(f"FASTEST 3 cities (min {MIN_ORDERS} delivered orders):")
city_delivery.orderBy("avg_delivery_days").show(3, truncate=False)
print(f"SLOWEST 3 cities (min {MIN_ORDERS} delivered orders):")
city_delivery.orderBy(F.desc("avg_delivery_days")).show(3, truncate=False)


# ---------------------------------------------------------------------------
banner(6, "Relationship between delivery time and review score")
# ---------------------------------------------------------------------------
# THE headline insight of this case study. Two complementary views:
#
#  (a) average delivery time bucketed by review score - easy to read, and it
#      shows the SHAPE of the relationship (is it monotonic? where's the cliff?)
#  (b) a single Pearson correlation - one number for the summary slide.
#
# Caveat worth stating out loud: review_score is ORDINAL (1..5), not a true
# continuous variable. Pearson assumes interval data, so it under-states a
# monotonic-but-non-linear relationship. Spearman rank correlation is the
# statistically correct choice; we compute both and compare.
delivery_vs_review = (
    delivered.groupBy("review_score")
             .agg(
                 F.count("*").alias("num_orders"),
                 F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"),
                 F.round(F.expr("percentile_approx(delivery_days, 0.5)"), 2)
                  .alias("median_delivery_days"),
             )
             .orderBy("review_score")
)
write_gold(delivery_vs_review, "delivery_vs_review")
delivery_vs_review.show(truncate=False)

pearson = delivered.stat.corr("delivery_days", "review_score", "pearson")

# Spearman = Pearson computed on the RANKS instead of the raw values.
# Spark has no built-in spearman for two columns, so we rank first.
from pyspark.sql.window import Window  # noqa: E402 (imported here, next to its one use)

ranked = (
    delivered
    .withColumn("rank_delivery", F.rank().over(Window.orderBy("delivery_days")))
    .withColumn("rank_review",   F.rank().over(Window.orderBy("review_score")))
)
spearman = ranked.stat.corr("rank_delivery", "rank_review", "pearson")

corr_summary = spark.createDataFrame(
    [("delivery_days_vs_review_score", round(pearson, 4), round(spearman, 4))],
    ["relationship", "pearson_corr", "spearman_corr"],
)
write_gold(corr_summary, "delivery_review_correlation")
corr_summary.show(truncate=False)


# ---------------------------------------------------------------------------
banner(7, "BONUS: daily and weekly sales - the brief's actual ask")
# ---------------------------------------------------------------------------
# The problem statement asks for sales "on a daily and weekly basis", which
# the listed steps never cover. This is the time-series table a real reporting
# layer would be built on.
daily_sales = (
    df.filter(F.col("order_status") != "canceled")
      .groupBy("order_date", "customer_state")
      .agg(
          F.count("*").alias("order_count"),
          F.round(F.sum("order_total_value"), 2).alias("total_sales"),
      )
      .orderBy("order_date", "customer_state")
)
write_gold(daily_sales, "daily_sales_by_state")

weekly_sales = (
    df.filter(F.col("order_status") != "canceled")
      .groupBy("order_year", "order_week")
      .agg(
          F.count("*").alias("order_count"),
          F.round(F.sum("order_total_value"), 2).alias("total_sales"),
          F.round(F.avg("review_score"), 2).alias("avg_review_score"),
      )
      .orderBy("order_year", "order_week")
)
write_gold(weekly_sales, "weekly_sales")
print("Weekly sales (last 10 weeks in the data):")
weekly_sales.orderBy(F.desc("order_year"), F.desc("order_week")).show(10, truncate=False)

# The cleaned row-level table itself is the "silver" layer everything else
# derives from. Save it too.
df.write.mode("overwrite").parquet("output/silver/orders_clean")

print(f"\n{BAR}\nAll gold tables written to {GOLD}/\n{BAR}")
for t in sorted(os.listdir(GOLD)):
    print(f"  - {t}")

df.unpersist()
spark.stop()
