"""
Unit tests for the shared cleaning/schema logic in src/processing/common.py.

These are deliberately narrow: each test asserts ONE behavior that a future
edit could silently break. They exist because two of these behaviors were
real bugs caught during development (city casing, the NaN/null handling) -
a regression here is not hypothetical, it already happened once.
"""

from common import ORDERS_SCHEMA, clean
from pyspark.sql.types import StructType

ORDER_COLS = [
    "customer_city", "customer_state", "order_status",
    "order_products_value", "order_freight_value", "order_items_qty",
    "order_purchase_timestamp", "order_aproved_at",
    "order_delivered_customer_date", "review_score",
]

# Rows in these tests carry all-None columns (e.g. timestamps we don't care
# about for a given assertion), which Spark's type inference cannot handle -
# it needs a concrete schema to fall back on. Reuse the real ORDERS_SCHEMA
# field types so the test data matches production types exactly.
_FIELDS_BY_NAME = {f.name: f for f in ORDERS_SCHEMA.fields}
ORDER_ROWS_SCHEMA = StructType([_FIELDS_BY_NAME[c] for c in ORDER_COLS])


def make_orders_df(spark, rows):
    return spark.createDataFrame(rows, schema=ORDER_ROWS_SCHEMA)


def test_city_casing_is_normalized(spark):
    """SAO PAULO and Sao Paulo must collapse to one group, or every city
    aggregation undercounts its top city (this happened - see README)."""
    df = make_orders_df(spark, [
        ("SAO PAULO", "sp", "DELIVERED", None, None, None, None, None, None, None),
        ("Sao Paulo", "SP", "delivered", None, None, None, None, None, None, None),
        (" sao paulo ", "Sp", "Delivered", None, None, None, None, None, None, None),
    ])
    cleaned = clean(df)
    cities = [r["customer_city"] for r in cleaned.select("customer_city").collect()]
    assert cities == ["Sao Paulo", "Sao Paulo", "Sao Paulo"]


def test_state_is_uppercased(spark):
    df = make_orders_df(spark, [
        ("Sao Paulo", "sp", "delivered", None, None, None, None, None, None, None),
    ])
    result = clean(df).select("customer_state").first()
    assert result["customer_state"] == "SP"


def test_order_total_value_is_sum_of_products_and_freight(spark):
    df = make_orders_df(spark, [
        ("Sao Paulo", "SP", "delivered", 100.0, 25.5, 2, None, None, None, 5),
    ])
    row = clean(df).select("order_total_value").first()
    assert row["order_total_value"] == 125.5


def test_undelivered_orders_keep_null_delivery_days(spark):
    """An order with no delivery timestamp must stay null, not become a
    fabricated 0 or get silently dropped - see the null-policy discussion
    in README.md."""
    df = make_orders_df(spark, [
        ("Sao Paulo", "SP", "shipped", 10.0, 1.0, 1, None, None, None, None),
    ])
    row = clean(df).select("delivery_days").first()
    assert row["delivery_days"] is None


def test_schema_has_no_duplicate_columns():
    names = [f.name for f in ORDERS_SCHEMA.fields]
    assert len(names) == len(set(names))


def test_zip_code_is_declared_as_string_not_integer():
    """A zip code is an identifier, never a quantity - storing it as an
    integer silently eats leading zeros (01040 -> 1040)."""
    field = next(f for f in ORDERS_SCHEMA.fields if f.name == "customer_zip_code_prefix")
    assert field.dataType.simpleString() == "string"
