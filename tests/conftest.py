import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "processing"))

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """One Spark session for the whole test run - starting the JVM is slow,
    so we pay that cost once rather than per test."""
    s = (
        SparkSession.builder
        .master("local[1]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
