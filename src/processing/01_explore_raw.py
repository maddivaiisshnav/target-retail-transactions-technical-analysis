"""
Step 1: Get Spark running and understand what it does to our schema.

Goal of this script is NOT to produce results. It is to SEE the problem
that the 'cast your columns' step in the case study is there to solve.
"""

from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# The SparkSession is the entry point to everything.
# Creating it boots a JVM in the background and starts the Spark "driver".
#
#   .master("local[*]")  -> run Spark on THIS machine, using [*] = all CPU cores.
#                           On a cluster this would be "yarn" or a k8s URL.
#                           The rest of our code does not change - that is the
#                           whole point of Spark: same code, 1 laptop or 500 nodes.
#   .appName(...)        -> label shown in the Spark UI / cluster logs.
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("TargetPipeline-ExploreRaw")
    .getOrCreate()
)

# Spark is extremely chatty at INFO level. Turn it down so we can read output.
spark.sparkContext.setLogLevel("WARN")

CSV = "data/target_orders.csv"

print("\n" + "=" * 70)
print("READ #1: header=True only  (Spark's DEFAULT behaviour)")
print("=" * 70)

df_default = spark.read.option("header", True).csv(CSV)
df_default.printSchema()

print("\n" + "=" * 70)
print("READ #2: header=True, inferSchema=True  (let Spark guess types)")
print("=" * 70)

df_inferred = spark.read.option("header", True).option("inferSchema", True).csv(CSV)
df_inferred.printSchema()

print("\n" + "=" * 70)
print("Sample rows (from the inferred read)")
print("=" * 70)
df_inferred.show(5, truncate=False)

print(f"\nRow count: {df_inferred.count()}")

spark.stop()
