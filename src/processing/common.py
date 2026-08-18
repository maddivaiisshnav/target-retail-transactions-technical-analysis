"""
Shared building blocks for the Target sales pipeline.

Everything that more than one script needs lives here:
  - how to build a SparkSession
  - the ONE authoritative schema definition
  - the cleaning / enrichment logic

Why a shared module? If each script declares its own schema, they drift apart
the moment someone edits one of them. One definition, imported everywhere.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

RAW_CSV = "data/target_orders.csv"
WAREHOUSE_DIR = "output/warehouse"


# ---------------------------------------------------------------------------
# 1. THE EXPLICIT SCHEMA  (replaces inferSchema)
# ---------------------------------------------------------------------------
# This is a CONTRACT. If upstream data stops matching it, we want to know.
#
# Type choices worth defending:
#   order_products_value -> DoubleType, it is money we do arithmetic on.
#   customer_zip_code_prefix -> StringType, NOT Integer. A zip code is an
#       identifier, never a quantity. Integer silently eats leading zeros
#       ("01040" -> 1040). Rule of thumb: if you would never add two of them
#       together, it is a string.
#   review_score -> IntegerType, it is an ordinal 1..5 we will average.
#   the timestamps -> TimestampType so we can do real date arithmetic.
#
# NOTE the misspelling "order_aproved_at". It is wrong in the source file, so
# it must be wrong here. Never silently "fix" a source column name at read
# time; rename it explicitly later so the mapping is visible.
# ---------------------------------------------------------------------------
ORDERS_SCHEMA = StructType([
    StructField("Id",                            IntegerType(),   True),
    StructField("order_status",                  StringType(),    True),
    StructField("order_products_value",          DoubleType(),    True),
    StructField("order_freight_value",           DoubleType(),    True),
    StructField("order_items_qty",               IntegerType(),   True),
    StructField("order_purchase_timestamp",      TimestampType(), True),
    StructField("order_aproved_at",              TimestampType(), True),
    StructField("order_delivered_customer_date", TimestampType(), True),
    StructField("customer_city",                 StringType(),    True),
    StructField("customer_state",                StringType(),    True),
    StructField("customer_zip_code_prefix",      StringType(),    True),
    StructField("review_score",                  IntegerType(),   True),
])


def get_spark(app_name: str, with_hive: bool = False) -> SparkSession:
    """Build (or reuse) a SparkSession.

    local[*]  -> run on this machine using every CPU core.
                 Swap for "yarn" / k8s on a cluster; NO other code changes.
    """
    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        # 200 shuffle partitions is the Spark default, tuned for terabytes.
        # On 1000 rows it creates 200 near-empty tasks and is pure overhead.
        # Tuning this is one of the most common real Spark optimisations.
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
    )
    if with_hive:
        builder = builder.enableHiveSupport()

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_raw(spark: SparkSession, path: str = RAW_CSV) -> DataFrame:
    """Read the CSV using our explicit schema.

    mode=PERMISSIVE (the default) turns unparseable values into null rather
    than failing. We pair it with a _corrupt_record column so bad rows are
    VISIBLE instead of silently becoming nulls we later mistake for real
    missing data. Silent data loss is the enemy.
    """
    schema_with_corrupt = StructType(
        ORDERS_SCHEMA.fields + [StructField("_corrupt_record", StringType(), True)]
    )
    return (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .schema(schema_with_corrupt)
        .csv(path)
    )


def clean(df: DataFrame) -> DataFrame:
    """Clean and enrich. This is the 'Silver' layer of a medallion architecture.

    Deliberately NOT doing a blanket dropna(). See the note on nulls below.
    """
    return (
        df
        # --- normalise the grouping keys -----------------------------------
        # "SAO PAULO" and "Sao Paulo" are different strings to a groupBy.
        # trim() first (stray whitespace is invisible and just as damaging),
        # then lower() to collapse casing, then initcap() for presentation.
        .withColumn("customer_city", F.initcap(F.lower(F.trim(F.col("customer_city")))))
        .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
        .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))

        # --- derived business metrics --------------------------------------
        # Delivery time: purchase -> in the customer's hands. This is what the
        # customer actually experiences, so it is what correlates with reviews.
        # Null stays null for undelivered orders, which is correct.
        .withColumn(
            "delivery_days",
            (F.col("order_delivered_customer_date").cast("long")
             - F.col("order_purchase_timestamp").cast("long")) / 86400.0,
        )
        # Approval time: purchase -> retailer approved. Usually minutes/hours,
        # so hours is the sensible unit. This is an internal ops metric.
        .withColumn(
            "approval_hours",
            (F.col("order_aproved_at").cast("long")
             - F.col("order_purchase_timestamp").cast("long")) / 3600.0,
        )
        # Total order value = goods + shipping. What the customer actually paid.
        .withColumn(
            "order_total_value",
            F.col("order_products_value") + F.col("order_freight_value"),
        )

        # --- calendar keys for the daily / weekly reporting in the brief ----
        .withColumn("order_date", F.to_date(F.col("order_purchase_timestamp")))
        .withColumn("order_year", F.year(F.col("order_purchase_timestamp")))
        .withColumn("order_week", F.weekofyear(F.col("order_purchase_timestamp")))
    )
