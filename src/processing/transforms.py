"""
Analytical transformations and gold layer aggregates.

Calculates key retail business metrics including geographic sales distribution,
delivery/approval SLAs, correlations between price, freight, and volume, review
score distributions, and transaction time-series (daily/weekly).
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common import clean, get_spark, load_raw
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

GOLD_DIR = "output/gold"
SILVER_DIR = "output/silver"

def write_gold(df: DataFrame, name: str) -> DataFrame:
    """Helper to persist analytical outputs as optimized single-partition Parquet tables."""
    df.coalesce(1).write.mode("overwrite").parquet(f"{GOLD_DIR}/{name}")
    return df

def main():
    spark = get_spark("TargetPipeline-Transforms")
    
    # Load and prepare clean data
    raw_df = load_raw(spark)
    df = clean(raw_df).drop("_corrupt_record")
    df.cache()

    # Filtered view for delivery metrics (completed deliveries only)
    delivered_df = df.filter(F.col("order_delivered_customer_date").isNotNull())

    # 1. Geographic Sales Distribution (City Level)
    print("Computing city-level sales aggregates...")
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

    # 2. Geographic Sales Distribution (State Level)
    print("Computing state-level sales aggregates...")
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

    # 3. Product, Freight, and Quantity Correlations
    print("Calculating metrics correlations...")
    pairs = [
        ("order_products_value", "order_freight_value"),
        ("order_products_value", "order_items_qty"),
        ("order_freight_value",  "order_items_qty"),
    ]
    corr_rows = [(a, b, round(df.stat.corr(a, b), 4)) for a, b in pairs]
    corr_df = spark.createDataFrame(corr_rows, ["metric_a", "metric_b", "pearson_corr"])
    write_gold(corr_df, "value_freight_qty_correlation")

    # 4. Delivery & Approval Durations SLA Analysis
    print("Analyzing delivery and approval timelines...")
    timing = delivered_df.select(
        F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"),
        F.round(F.expr("percentile_approx(delivery_days, 0.5)"), 2).alias("median_delivery_days"),
        F.round(F.min("delivery_days"), 2).alias("min_delivery_days"),
        F.round(F.max("delivery_days"), 2).alias("max_delivery_days"),
        F.round(F.avg("approval_hours"), 2).alias("avg_approval_hours"),
        F.round(F.expr("percentile_approx(approval_hours, 0.5)"), 2).alias("median_approval_hours"),
    )
    write_gold(timing, "delivery_timing_summary")

    # 5. Customer Review Distributions
    print("Computing customer satisfaction metrics...")
    review_dist = (
        df.groupBy("review_score")
          .agg(F.count("*").alias("num_orders"))
          .withColumn("pct", F.round(100 * F.col("num_orders") / df.count(), 2))
          .orderBy("review_score")
    )
    write_gold(review_dist, "review_score_distribution")

    # City-level review averages (min 5 orders)
    review_by_city = (
        df.groupBy("customer_city")
          .agg(
              F.count("*").alias("order_count"),
              F.round(F.avg("review_score"), 2).alias("avg_review_score"),
          )
          .filter(F.col("order_count") >= 5)
          .orderBy("avg_review_score")
    )
    write_gold(review_by_city, "review_by_city")

    # 6. Delivery Performance by City
    print("Evaluating city delivery durations...")
    city_delivery = (
        delivered_df.groupBy("customer_city", "customer_state")
                 .agg(
                     F.count("*").alias("delivered_orders"),
                     F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"),
                     F.round(F.avg("review_score"), 2).alias("avg_review_score"),
                 )
                 .filter(F.col("delivered_orders") >= 5)
    )
    write_gold(city_delivery, "city_delivery_performance")

    # 7. Delivery Duration vs. Customer Satisfaction Correlation
    print("Computing delivery time vs. review score correlation...")
    delivery_vs_review = (
        delivered_df.groupBy("review_score")
                 .agg(
                     F.count("*").alias("num_orders"),
                     F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"),
                     F.round(F.expr("percentile_approx(delivery_days, 0.5)"), 2)
                      .alias("median_delivery_days"),
                 )
                 .orderBy("review_score")
    )
    write_gold(delivery_vs_review, "delivery_vs_review")

    # Calculate Pearson Correlation
    pearson = delivered_df.stat.corr("delivery_days", "review_score", "pearson")

    # Calculate Spearman (Rank) Correlation
    ranked = (
        delivered_df
        .withColumn("rank_delivery", F.rank().over(Window.orderBy("delivery_days")))
        .withColumn("rank_review",   F.rank().over(Window.orderBy("review_score")))
    )
    spearman = ranked.stat.corr("rank_delivery", "rank_review", "pearson")

    corr_summary = spark.createDataFrame(
        [("delivery_days_vs_review_score", round(pearson, 4), round(spearman, 4))],
        ["relationship", "pearson_corr", "spearman_corr"],
    )
    write_gold(corr_summary, "delivery_review_correlation")

    # 8. Time-Series Aggregations (Daily and Weekly)
    print("Aggregating sales time-series (daily/weekly)...")
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

    # Save cleaned Silver layer
    df.write.mode("overwrite").parquet(f"{SILVER_DIR}/orders_clean")

    print(f"\nAll transformations completed. Outputs saved to {GOLD_DIR}/ and {SILVER_DIR}/.")
    
    df.unpersist()
    spark.stop()

if __name__ == "__main__":
    main()
