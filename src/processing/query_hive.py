"""
Interactive SQL shell over the Hive tables.

This is the deliverable an ANALYST touches. They never see PySpark, Kafka, or
Parquet - they see tables and write SQL. That is the entire point of the
pipeline: turn a raw event stream into something a non-engineer can question.

    .venv/bin/python src/processing/query_hive.py             # interactive
    .venv/bin/python src/processing/query_hive.py "SELECT ..." # one-shot
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import get_spark

spark = get_spark("TargetPipeline-Query", with_hive=True)
spark.sql("USE target_retail")

if len(sys.argv) > 1:
    spark.sql(sys.argv[1]).show(100, truncate=False)
    spark.stop()
    sys.exit(0)

print("\nTables available in target_retail:")
spark.sql("SHOW TABLES").show(truncate=False)
print("Type SQL and press Enter. 'tables' to list, 'quit' to exit.\n")

while True:
    try:
        q = input("hive> ").strip()
    except (EOFError, KeyboardInterrupt):
        break
    if not q:
        continue
    if q.lower() in {"quit", "exit", "\\q"}:
        break
    if q.lower() == "tables":
        spark.sql("SHOW TABLES").show(truncate=False)
        continue
    try:
        spark.sql(q.rstrip(";")).show(100, truncate=False)
    except Exception as e:
        print(f"ERROR: {e}\n")

spark.stop()
