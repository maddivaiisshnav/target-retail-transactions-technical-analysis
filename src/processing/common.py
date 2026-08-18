"""
Shared processing utilities, schemas, and cleaning helpers.
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

# Authoritative schema definition to enforce strict data contracts
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
    """Configures and retrieves the SparkSession."""
    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
    )
    if with_hive:
        builder = builder.enableHiveSupport()

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def load_raw(spark: SparkSession, path: str = RAW_CSV) -> DataFrame:
    """Reads raw sales CSV files enforcing schema mapping and record validation."""
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
    """Normalizes string casing/spacing, calculates derived KPIs, and registers calendar keys."""
    return (
        df
        # Normalize text attributes
        .withColumn("customer_city", F.initcap(F.lower(F.trim(F.col("customer_city")))))
        .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
        .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))

        # Derivations & KPIs
        .withColumn(
            "delivery_days",
            (F.col("order_delivered_customer_date").cast("long")
             - F.col("order_purchase_timestamp").cast("long")) / 86400.0,
        )
        .withColumn(
            "approval_hours",
            (F.col("order_aproved_at").cast("long")
             - F.col("order_purchase_timestamp").cast("long")) / 3600.0,
        )
        .withColumn(
            "order_total_value",
            F.col("order_products_value") + F.col("order_freight_value"),
        )

        # Date partitioning / indexing attributes
        .withColumn("order_date", F.to_date(F.col("order_purchase_timestamp")))
        .withColumn("order_year", F.year(F.col("order_purchase_timestamp")))
        .withColumn("order_week", F.weekofyear(F.col("order_purchase_timestamp")))
    )
