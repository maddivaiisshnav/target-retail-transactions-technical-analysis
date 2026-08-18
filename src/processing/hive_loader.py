"""
Hive Metastore catalog publisher.

Publishes cleaned silver layers and gold aggregates into the Hive Metastore
database namespace (`target_retail`), enabling SQL queries across standard BI engines.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common import get_spark

DB = "target_retail"
GOLD_DIR = "output/gold"
SILVER_DIR = "output/silver"

def register_table(spark, path: str, table_name: str, partition_by=None):
    """Reads Parquet data and publishes it as a managed Hive catalog table."""
    df = spark.read.parquet(path)
    writer = df.write.mode("overwrite").format("parquet")

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.saveAsTable(table_name)
    row_count = spark.table(table_name).count()
    partition_info = f"  (partitioned by {partition_by})" if partition_by else ""
    print(f"  Published {DB}.{table_name:<30} | count: {row_count:<6}{partition_info}")

def main():
    # Initialize Spark Session with Hive support enabled
    spark = get_spark("TargetPipeline-Hive", with_hive=True)

    # Establish target database schema catalog
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
    spark.sql(f"USE {DB}")
    print(f"Using database catalog schema: {DB}\n")

    print("Registering gold and silver layers in Hive metastore:")
    
    # Gold layer tables
    register_table(spark, f"{GOLD_DIR}/city_sales",                    "city_sales")
    register_table(spark, f"{GOLD_DIR}/state_sales",                   "state_sales")
    register_table(spark, f"{GOLD_DIR}/value_freight_qty_correlation", "value_freight_qty_correlation")
    register_table(spark, f"{GOLD_DIR}/delivery_timing_summary",       "delivery_timing_summary")
    register_table(spark, f"{GOLD_DIR}/review_score_distribution",     "review_score_distribution")
    register_table(spark, f"{GOLD_DIR}/review_by_city",                "review_by_city")
    register_table(spark, f"{GOLD_DIR}/city_delivery_performance",     "city_delivery_performance")
    register_table(spark, f"{GOLD_DIR}/delivery_vs_review",            "delivery_vs_review")
    register_table(spark, f"{GOLD_DIR}/delivery_review_correlation",   "delivery_review_correlation")
    register_table(spark, f"{GOLD_DIR}/weekly_sales",                  "weekly_sales")
    
    # Partitioned Gold table
    register_table(spark, f"{GOLD_DIR}/daily_sales_by_state",          "daily_sales_by_state",
                   partition_by=["customer_state"])
    
    # Partitioned Silver table (row-level cleaned transactions)
    register_table(spark, f"{SILVER_DIR}/orders_clean",                "orders_clean",
                   partition_by=["customer_state"])

    # Output metastore table index for confirmation
    print("\nMetastore Tables Index:")
    spark.sql(f"SHOW TABLES IN {DB}").show(truncate=False)

    print("Sample validation queries:")
    
    print("\n1. Top 5 customer cities by total sales volume:")
    spark.sql("""
        SELECT customer_city, customer_state, order_count, total_sales, avg_order_value
        FROM city_sales
        ORDER BY total_sales DESC
        LIMIT 5
    """).show(truncate=False)

    print("\n2. Monthly Sales Ingest Aggregations:")
    spark.sql("""
        SELECT DATE_FORMAT(order_purchase_timestamp, 'yyyy-MM') AS month,
               COUNT(*)                         AS orders,
               ROUND(SUM(order_total_value), 2) AS revenue
        FROM orders_clean
        WHERE order_status <> 'canceled'
        GROUP BY 1
        ORDER BY 1
    """).show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
