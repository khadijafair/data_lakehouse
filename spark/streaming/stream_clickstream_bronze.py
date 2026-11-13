"""
Ingestion streaming Bronze : topic Kafka `clickstream` -> table Iceberg bronze.ecommerce.clickstream.

Spark Structured Streaming lit Kafka par micro-lots et ajoute chaque lot à la
table Iceberg. Le checkpoint (offsets Kafka déjà traités) est stocké dans MinIO :
un redémarrage reprend exactement où il s'était arrêté, sans perte ni doublon
(chaque micro-lot est un commit Iceberg atomique).

Règle Bronze, comme pour les sources batch : aucune transformation métier, tout
reste en texte. On ajoute seulement les métadonnées Kafka (partition, offset,
horodatage) pour la traçabilité. Un message JSON illisible n'est pas perdu :
il est gardé tel quel dans `_raw_value`.

Deux modes :
    python stream_clickstream_bronze.py                     # traite tout ce qui est en attente puis s'arrête (Airflow)
    python stream_clickstream_bronze.py --mode continuous   # tourne en continu, un micro-lot toutes les 10 s
"""

import argparse
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent / "common"))
from spark_session import PACKAGES, get_spark_session  # noqa: E402

from pyspark.sql.functions import col, current_timestamp, from_json, when  # noqa: E402
from pyspark.sql.types import StringType, StructField, StructType  # noqa: E402

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "clickstream"
TABLE = "bronze.ecommerce.clickstream"
CHECKPOINT = os.environ.get(
    "CLICKSTREAM_CHECKPOINT", "s3a://bronze/_checkpoints/clickstream_bronze"
)
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2"

EVENT_FIELDS = ["event_id", "session_id", "customer_id", "event_type",
                "page_url", "device_id", "timestamp", "ingest_run_id"]
EVENT_SCHEMA = StructType([StructField(f, StringType()) for f in EVENT_FIELDS])


def create_table_if_needed(spark) -> None:
    columns = ",\n".join(f"  {f} STRING" for f in EVENT_FIELDS)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze.ecommerce")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
        {columns},
          _raw_value STRING,
          _kafka_partition INT,
          _kafka_offset BIGINT,
          _kafka_timestamp TIMESTAMP,
          _ingested_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(_kafka_timestamp))
    """)


def build_stream(spark):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 100_000)   # taille max d'un micro-lot
        .load()
    )
    value = col("value").cast("string")
    parsed = raw.withColumn("event", from_json(value, EVENT_SCHEMA))
    return parsed.select(
        *[col(f"event.{f}").alias(f) for f in EVENT_FIELDS],
        when(col("event.event_id").isNull(), value).alias("_raw_value"),
        col("partition").alias("_kafka_partition"),
        col("offset").alias("_kafka_offset"),
        col("timestamp").alias("_kafka_timestamp"),
        current_timestamp().alias("_ingested_at"),
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["available-now", "continuous"], default="available-now")
    parser.add_argument("--trigger-seconds", type=int, default=10)
    args = parser.parse_args()

    spark = get_spark_session(
        "clickstream-bronze-stream",
        extra_conf={"spark.jars.packages": f"{PACKAGES},{KAFKA_PACKAGE}"},
    )
    spark.sparkContext.setLogLevel("WARN")
    create_table_if_needed(spark)
    before = spark.table(TABLE).count()

    writer = (
        build_stream(spark).writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .queryName("clickstream_bronze")
    )
    if args.mode == "available-now":
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=f"{args.trigger_seconds} seconds")

    query = writer.toTable(TABLE)
    query.awaitTermination()

    after = spark.table(TABLE).count()
    print(f"{TABLE} : {before} -> {after} lignes (+{after - before} depuis Kafka)")
    spark.stop()


if __name__ == "__main__":
    main()
