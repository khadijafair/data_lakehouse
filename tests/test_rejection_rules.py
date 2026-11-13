"""
Tests des motifs de rejet Silver, sur une SparkSession locale minimale
(aucun jar Iceberg ni MinIO nécessaire).
"""

import math
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

sys.path.append(str(Path(__file__).resolve().parent.parent / "spark" / "common"))
from pyspark.sql import SparkSession  # noqa: E402
from rejection_rules import order_rules, product_rules, split_by_rules  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-rejection-rules")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


ORDER_COLS = "order_id string, customer_id string, order_amount double, quantity int, " \
             "status string, payment_method string, order_date date"


def _orders(spark, rows):
    return spark.createDataFrame(rows, ORDER_COLS)


def test_valid_order_is_kept(spark):
    import datetime as dt
    df = _orders(spark, [("o1", "c1", 10.0, 2, "success", "card", dt.date(2024, 1, 1))])
    valid, rejected = split_by_rules(df, order_rules())
    assert valid.count() == 1
    assert rejected.count() == 0
    assert "_rejection_reason" not in valid.columns


def test_each_invalid_field_is_named(spark):
    df = _orders(spark, [("o2", "c1", math.nan, 0, "success", None, None)])
    valid, rejected = split_by_rules(df, order_rules())
    assert valid.count() == 0
    reason = rejected.collect()[0]["_rejection_reason"]
    assert reason == "order_amount, quantity, payment_method, order_date"


def test_negative_price_rejected(spark):
    df = spark.createDataFrame(
        [("p1", -5.0, "home"), ("p2", 3.0, "home")],
        "product_id string, price double, category string",
    )
    valid, rejected = split_by_rules(df, product_rules())
    assert [r.product_id for r in valid.collect()] == ["p2"]
    assert rejected.collect()[0]["_rejection_reason"] == "price"
