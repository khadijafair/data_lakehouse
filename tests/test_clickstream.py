"""Tests du flux clickstream : message Kafka du producteur, règles Silver, DAG."""

import ast
import datetime as dt
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "streaming" / "producer"))
from replay_clickstream import FIELDS, to_message  # noqa: E402


def test_producer_message_keeps_raw_values_and_uses_event_id_as_key():
    row = {f: "" for f in FIELDS} | {"event_id": "e1", "timestamp": "2024/31/01 25:61:00"}
    key, value = to_message(row)
    event = json.loads(value)
    assert key == b"e1"
    assert list(event) == FIELDS
    assert event["timestamp"] == "2024/31/01 25:61:00"   # le producteur ne nettoie rien


def test_clickstream_dag_order():
    dag = (ROOT / "airflow" / "dags" / "clickstream_pipeline.py").read_text(encoding="utf-8")
    ids = [kw.value.value for node in ast.walk(ast.parse(dag)) if isinstance(node, ast.Call)
           for kw in node.keywords if kw.arg == "task_id"]
    assert ids == ["ingest_clickstream_bronze", "transform_silver_clickstream", "dbt_build_funnel"]
    assert "ingest_clickstream_bronze >> transform_silver_clickstream >> dbt_build_funnel" in dag


def test_main_dag_does_not_need_kafka():
    dag = (ROOT / "airflow" / "dags" / "lakehouse_pipeline.py").read_text(encoding="utf-8")
    assert "--exclude source:silver.clickstream+" in dag


pytest.importorskip("pyspark")
sys.path.append(str(ROOT / "spark" / "common"))
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.functions import lit, to_timestamp  # noqa: E402
from rejection_rules import clickstream_rules, split_by_rules  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[1]").appName("test-clickstream")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    yield session
    session.stop()


def test_clickstream_rules_reject_missing_and_future_timestamps(spark):
    rows = [
        ("e1", "page_view", dt.datetime(2025, 10, 1, 12, 0)),   # valide
        ("e2", "page_view", None),                              # date illisible
        ("e3", "add_to_cart", dt.datetime(2027, 5, 1, 0, 0)),   # après l'extraction
        ("e4", "purchase", dt.datetime(2025, 10, 1, 12, 0)),    # type inconnu
    ]
    df = spark.createDataFrame(rows, "event_id string, event_type string, event_ts timestamp")
    reference = to_timestamp(lit("2025-12-02 16:05:00"))
    valid, rejected = split_by_rules(df, clickstream_rules(reference))
    assert [r.event_id for r in valid.collect()] == ["e1"]
    reasons = {r.event_id: r._rejection_reason for r in rejected.collect()}
    assert reasons == {"e2": "event_ts", "e3": "event_ts_future", "e4": "event_type"}
