"""
Point d'entrée unique pour créer une SparkSession connectée à MinIO
et aux 3 catalogues Iceberg (bronze, silver, gold).

Pourquoi centraliser ça : chaque job (ingestion, bronze, silver, gold)
importera cette fonction au lieu de recopier 20 lignes de config partout.
Si l'endpoint MinIO change un jour, on ne modifie qu'ici.
"""

from pyspark.sql import SparkSession
from pathlib import Path

import os

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "doujalouja")


def get_spark_session(app_name: str = "lakehouse-local") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        # --- Jars nécessaires (Spark les télécharge depuis Maven au 1er lancement) ---
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        # --- Connexion S3A vers MinIO (pas AWS réel) ---
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")   # obligatoire pour MinIO
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # --- Catalogue Iceberg "bronze" -> bucket bronze ---
        .config("spark.sql.catalog.bronze", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.bronze.type", "hadoop")
        .config("spark.sql.catalog.bronze.warehouse", "s3a://bronze/")
        # --- Catalogue Iceberg "silver" -> bucket silver ---
        .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.silver.type", "hadoop")
        .config("spark.sql.catalog.silver.warehouse", "s3a://silver/")
        # --- Catalogue Iceberg "gold" -> bucket gold ---
        .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.gold.type", "hadoop")
        .config("spark.sql.catalog.gold.warehouse", "s3a://gold/")
    )


    spark = builder.getOrCreate()

    normalizers_path = Path(__file__).resolve().parent / "normalizers.py"
    spark.sparkContext.addPyFile(str(normalizers_path))

    return spark
