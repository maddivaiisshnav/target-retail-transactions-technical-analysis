"""
Step 4: Publish the results as Hive tables.

WHAT HIVE ACTUALLY IS (2026 edition)
------------------------------------
Hive began as a system that compiled SQL into MapReduce jobs. That execution
engine is dead - Spark replaced it. What survived, and what every lakehouse
still runs, is the HIVE METASTORE: a small relational database holding
METADATA about tables:

    table 'city_sales'  ->  files at output/warehouse/city_sales/
                        ->  schema: customer_city STRING, total_sales DOUBLE
                        ->  format: Parquet
                        ->  partitions: order_date=2026-08-07/ ...

The data itself is just Parquet files sitting in a filesystem or object store.
The metastore is the CATALOG that says "those files are a table called X".

Why that matters: it separates STORAGE from COMPUTE. Any engine - Spark,
Trino, Presto, Flink, DuckDB - can read the same metastore and query the same
files. Nobody owns the data. That is the opposite of Postgres, where data
lives in a proprietary format only Postgres can read.

Locally, Spark's enableHiveSupport() spins up an embedded Apache Derby
database as the metastore (you will see a metastore_db/ folder appear). In
production this would be MySQL/Postgres, or a managed catalog: AWS Glue,
Databricks Unity Catalog.

saveAsTable vs save:
    df.write.save(path)          -> writes files. Nothing knows they exist.
    df.write.saveAsTable(name)   -> writes files AND registers them in the
                                    metastore, so SELECT * FROM name works.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common import get_spark

DB = "target_retail"
GOLD = "output/gold"
BAR = "=" * 72

# with_hive=True -> enableHiveSupport(). This is what makes saveAsTable
# register into a persistent catalog instead of a session-scoped temp view.
spark = get_spark("TargetPipeline-Hive", with_hive=True)

# ---------------------------------------------------------------------------
# A DATABASE (aka schema) is just a namespace for tables. Real warehouses use
# them to separate layers and teams: raw / staging / analytics, or per-domain.
# ---------------------------------------------------------------------------
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
spark.sql(f"USE {DB}")
print(f"Using database: {DB}\n")


def register(parquet_path: str, table: str, partition_by=None):
    """Read a gold Parquet result and register it as a managed Hive table.

    mode('overwrite') makes this job IDEMPOTENT: re-running it produces the
    same end state instead of duplicating rows. Idempotency is non-negotiable
    in pipelines, because retries and backfills are normal, not exceptional.
    """
    df = spark.read.parquet(parquet_path)
    writer = df.write.mode("overwrite").format("parquet")

    if partition_by:
        # PARTITIONING writes data into subdirectories, e.g.
        #   .../daily_sales_by_state/customer_state=SP/part-0000.parquet
        # A query filtering on customer_state then reads ONLY that directory
        # and skips everything else. This is "partition pruning" and it is the
        # single biggest performance lever in a data lake.
        #
        # Choose a partition column that queries actually filter on, and that
        # has moderate cardinality. Partitioning by a high-cardinality column
        # (like order Id) creates millions of tiny files and is far SLOWER -
        # the classic "small files problem".
        writer = writer.partitionBy(*partition_by)

    writer.saveAsTable(table)
    n = spark.table(table).count()
    part = f"  partitioned by {partition_by}" if partition_by else ""
    print(f"  registered {DB}.{table:<32} rows={n:<6}{part}")


print("Registering gold tables into the Hive metastore:")
register(f"{GOLD}/city_sales",                    "city_sales")
register(f"{GOLD}/state_sales",                   "state_sales")
register(f"{GOLD}/value_freight_qty_correlation", "value_freight_qty_correlation")
register(f"{GOLD}/delivery_timing_summary",       "delivery_timing_summary")
register(f"{GOLD}/review_score_distribution",     "review_score_distribution")
register(f"{GOLD}/review_by_city",                "review_by_city")
register(f"{GOLD}/city_delivery_performance",     "city_delivery_performance")
register(f"{GOLD}/delivery_vs_review",            "delivery_vs_review")
register(f"{GOLD}/delivery_review_correlation",   "delivery_review_correlation")
register(f"{GOLD}/weekly_sales",                  "weekly_sales")
# Partitioned by state: this table is always sliced by geography.
register(f"{GOLD}/daily_sales_by_state",          "daily_sales_by_state",
         partition_by=["customer_state"])
# The row-level silver table, partitioned by state as well.
register("output/silver/orders_clean",            "orders_clean",
         partition_by=["customer_state"])

# ---------------------------------------------------------------------------
print(f"\n{BAR}\nWhat the metastore now knows\n{BAR}")
# ---------------------------------------------------------------------------
spark.sql(f"SHOW TABLES IN {DB}").show(truncate=False)

print("Schema + storage details Hive recorded for one table:")
spark.sql("DESCRIBE FORMATTED city_sales").show(60, truncate=False)

# ---------------------------------------------------------------------------
print(f"\n{BAR}\nProof it works: querying with plain SQL, no Spark API at all\n{BAR}")
# ---------------------------------------------------------------------------
# This is the payoff of the entire pipeline. An analyst who has never seen
# PySpark can now answer business questions in SQL.

print("Top 5 cities by revenue:")
spark.sql("""
    SELECT customer_city, customer_state, order_count, total_sales, avg_order_value
    FROM city_sales
    ORDER BY total_sales DESC
    LIMIT 5
""").show(truncate=False)

print("Delivery speed vs customer satisfaction, by state:")
spark.sql("""
    SELECT customer_state,
           COUNT(*)                          AS delivered_orders,
           ROUND(AVG(delivery_days), 2)      AS avg_delivery_days,
           ROUND(AVG(review_score), 2)       AS avg_review_score
    FROM orders_clean
    WHERE order_delivered_customer_date IS NOT NULL
    GROUP BY customer_state
    HAVING COUNT(*) >= 10
    ORDER BY avg_delivery_days DESC
""").show(truncate=False)

print("Monthly revenue trend:")
spark.sql("""
    SELECT DATE_FORMAT(order_purchase_timestamp, 'yyyy-MM') AS month,
           COUNT(*)                         AS orders,
           ROUND(SUM(order_total_value), 2) AS revenue
    FROM orders_clean
    WHERE order_status <> 'canceled'
    GROUP BY 1
    ORDER BY 1
""").show(30, truncate=False)

# ---------------------------------------------------------------------------
print(f"\n{BAR}\nPartition pruning: proof Spark reads only what it needs\n{BAR}")
# ---------------------------------------------------------------------------
# EXPLAIN shows the physical plan. Look for "PartitionFilters" - that is Spark
# telling us it will skip every directory except customer_state=SP.
spark.sql("SELECT * FROM orders_clean WHERE customer_state = 'SP'") \
     .explain(mode="formatted")

print(f"\n{BAR}\nHive layer complete.\n{BAR}")
spark.stop()
