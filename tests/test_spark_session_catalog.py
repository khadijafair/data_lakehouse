"""La configuration des catalogues Iceberg doit rester alignée entre Spark et Trino."""

import configparser
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "spark" / "common"))
from spark_session import LAYERS, iceberg_catalog_conf  # noqa: E402


def _trino_catalog(layer):
    parser = configparser.ConfigParser(interpolation=None)
    parser.read_string("[c]\n" + (ROOT / "trino" / "catalog" / f"{layer}.properties").read_text())
    return parser["c"]


def test_spark_uses_jdbc_catalog_for_every_layer():
    for layer in LAYERS:
        conf = iceberg_catalog_conf(layer, f"s3a://{layer}/")
        assert conf[f"spark.sql.catalog.{layer}.catalog-impl"] == "org.apache.iceberg.jdbc.JdbcCatalog"
        assert conf[f"spark.sql.catalog.{layer}.warehouse"] == f"s3a://{layer}/"


def test_trino_reads_the_same_catalogs_as_spark():
    for layer in LAYERS:
        trino = _trino_catalog(layer)
        assert trino["iceberg.catalog.type"] == "jdbc"
        # même nom de catalogue que dans Spark, sinon Trino ne voit aucune table
        assert trino["iceberg.jdbc-catalog.catalog-name"] == layer
        assert trino["iceberg.jdbc-catalog.connection-url"].endswith("/iceberg")
        assert trino["iceberg.jdbc-catalog.default-warehouse-dir"] == f"s3a://{layer}/"
