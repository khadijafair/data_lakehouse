"""
Silver clickstream : nettoie les événements de navigation arrivés par Kafka.

- Dédoublonnage sur event_id (un événement rejoué deux fois dans Kafka n'est compté qu'une fois).
- Horodatage : 4 écritures valides de la même date coexistent (ISO avec microsecondes
  et fuseau, la même avec un espace au lieu du « T », ISO sans fraction, ISO sans fuseau
  = UTC) ; elles sont toutes acceptées. Sont rejetées : les valeurs vides et la valeur
  impossible « 2024/31/01 25:61:00 » (25 h 61, répétée 20 011 fois).
- Dates futures : le jeu de données a été extrait le 2025-12-02 à 16:05 UTC (les vrais
  événements couvrent les 91 jours précédents). Les ~20 000 événements datés de
  décembre 2025 à 2028, répartis uniformément, sont des anomalies injectées.
  La date de référence est FIXE (pas l'horloge du jour), pour un résultat reproductible.
- product_id extrait de l'URL (/product/PROD-0109) ; customer_id vide = visiteur anonyme (gardé).

Chaque ligne rejetée porte la liste de ses motifs dans `_rejection_reason`.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent / "common"))
from spark_session import get_spark_session  # noqa: E402
from rejection_rules import clickstream_rules, split_by_rules  # noqa: E402

from pyspark.sql import Window  # noqa: E402
from pyspark.sql.functions import (  # noqa: E402
    coalesce, col, lit, lower, nullif, regexp_extract, row_number, to_timestamp, trim,
)

SOURCE = "bronze.ecommerce.clickstream"
EXTRACTION_TS = "2025-12-02 16:05:00"   # date d'extraction du jeu de données (UTC)
# Formats acceptés, du plus fréquent au plus rare (sans fuseau = UTC)
TIMESTAMP_FORMATS = [
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX",
    "yyyy-MM-dd HH:mm:ss.SSSSSSXXX",
    "yyyy-MM-dd'T'HH:mm:ssXXX",
    "yyyy-MM-dd'T'HH:mm:ss",
]


def clean_clickstream(spark):
    df = spark.table(SOURCE)
    rows_before = df.count()

    # Dédoublonnage : on garde la première arrivée dans Kafka de chaque event_id
    first_arrival = Window.partitionBy("event_id").orderBy(
        col("_kafka_timestamp"), col("_kafka_partition"), col("_kafka_offset")
    )
    deduped = (
        df.withColumn("_rn", row_number().over(first_arrival))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )
    rows_after_dedup = deduped.count()

    cleaned = (
        deduped
        .withColumn("event_type", lower(trim(col("event_type"))))
        .withColumn("customer_id", nullif(trim(col("customer_id")), lit("")))
        .withColumn("page_url", nullif(trim(col("page_url")), lit("")))
        .withColumn("timestamp_raw", col("timestamp"))
        .withColumn("event_ts", coalesce(*[to_timestamp(trim(col("timestamp")), f) for f in TIMESTAMP_FORMATS]))
        .withColumn("product_id",
                    nullif(regexp_extract(col("page_url"), r"/product/([^/?#]+)", 1), lit("")))
        .drop("timestamp", "_raw_value")
    )
    reference = to_timestamp(lit(EXTRACTION_TS))
    valid, rejected = split_by_rules(cleaned, clickstream_rules(reference))

    print(f"clickstream : {rows_before} lignes Bronze -> {rows_after_dedup} après dédup "
          f"-> {valid.count()} valides, {rejected.count()} rejetées")
    return valid, rejected


def main():
    spark = get_spark_session("silver-clickstream")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS silver.ecommerce")

    valid, rejected = clean_clickstream(spark)
    (
        valid.writeTo("silver.ecommerce.clickstream")
        .using("iceberg")
        .partitionedBy(col("event_type"))
        .createOrReplace()
    )
    rejected.writeTo("silver.ecommerce.clickstream_rejected").using("iceberg").createOrReplace()
    print("Silver clickstream terminé.")
    spark.stop()


if __name__ == "__main__":
    main()
