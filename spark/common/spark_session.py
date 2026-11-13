"""
Point d'entrée unique pour créer une SparkSession connectée à MinIO
et aux 3 catalogues Iceberg (bronze, silver, gold).

Catalogue Iceberg = JDBC (PostgreSQL). La liste des tables et le pointeur
vers leur dernier fichier de métadonnées sont rangés dans la base
`iceberg` du PostgreSQL du docker-compose ; les données et les métadonnées
Iceberg restent dans les buckets MinIO.

Pourquoi JDBC et plus Hadoop : Trino (utilisé par dbt pour la couche Gold)
ne sait pas lire un catalogue Iceberg de type « hadoop ». Il accepte
hive_metastore, glue, jdbc, rest, nessie et snowflake. Avec JDBC, Spark et
Trino lisent et écrivent exactement le même catalogue, sans service en plus.
Les anciennes tables Hadoop se migrent avec spark/common/migrate_catalog_to_jdbc.py
(historique de time travel conservé).
"""

import os
from pathlib import Path

from pyspark.sql import SparkSession

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "doujalouja")

ICEBERG_CATALOG_URI = os.environ.get(
    "ICEBERG_CATALOG_URI", "jdbc:postgresql://localhost:5432/iceberg"
)
ICEBERG_CATALOG_USER = os.environ.get("ICEBERG_CATALOG_USER", "iceberg")
ICEBERG_CATALOG_PASSWORD = os.environ.get("ICEBERG_CATALOG_PASSWORD", "iceberg")

LAYERS = ("bronze", "silver", "gold")

PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.postgresql:postgresql:42.7.4",  # pilote du catalogue JDBC
])


def iceberg_catalog_conf(name: str, warehouse: str) -> dict[str, str]:
    """Configuration Spark d'un catalogue Iceberg JDBC.
    Le nom du catalogue Spark (bronze, silver, gold) est aussi le
    `catalog_name` enregistré dans PostgreSQL : Trino doit utiliser le même."""
    prefix = f"spark.sql.catalog.{name}"
    return {
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
        f"{prefix}.uri": ICEBERG_CATALOG_URI,
        f"{prefix}.jdbc.user": ICEBERG_CATALOG_USER,
        f"{prefix}.jdbc.password": ICEBERG_CATALOG_PASSWORD,
        # V1 = schéma de tables récent du catalogue JDBC (gère aussi les vues)
        f"{prefix}.jdbc.schema-version": "V1",
        f"{prefix}.warehouse": warehouse,
    }


def get_spark_session(app_name: str = "lakehouse-local", extra_conf: dict | None = None) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        # --- Jars nécessaires (Spark les télécharge depuis Maven au 1er lancement) ---
        .config("spark.jars.packages", PACKAGES)
        # --- Connexion S3A vers MinIO (pas AWS réel) ---
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")   # obligatoire pour MinIO
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    )

    # --- 3 catalogues Iceberg JDBC, un par couche, chacun dans son bucket ---
    for layer in LAYERS:
        for key, value in iceberg_catalog_conf(layer, f"s3a://{layer}/").items():
            builder = builder.config(key, value)

    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()

    normalizers_path = Path(__file__).resolve().parent / "normalizers.py"
    spark.sparkContext.addPyFile(str(normalizers_path))

    return spark
