"""
CLI client query interface for local Hive database catalog.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import get_spark

def main():
    spark = get_spark("TargetPipeline-Query", with_hive=True)
    spark.sql("USE target_retail")

    # One-shot command line query execution
    if len(sys.argv) > 1:
        spark.sql(sys.argv[1]).show(100, truncate=False)
        spark.stop()
        sys.exit(0)

    # Interactive SQL session loop
    print("\nTables available in schema target_retail:")
    spark.sql("SHOW TABLES").show(truncate=False)
    print("Execute SQL commands. Type 'tables' to list tables, 'exit' or 'quit' to exit.\n")

    while True:
        try:
            query = input("hive> ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        
        if not query:
            continue
        if query.lower() in {"quit", "exit", "\\q"}:
            break
        if query.lower() == "tables":
            spark.sql("SHOW TABLES").show(truncate=False)
            continue
            
        try:
            spark.sql(query.rstrip(";")).show(100, truncate=False)
        except Exception as e:
            print(f"Error executing statement: {e}\n")

    spark.stop()

if __name__ == "__main__":
    main()
