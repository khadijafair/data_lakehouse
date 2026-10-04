"""
Ingestion Bronze : lit les fichiers sources CSV et les écrit en tables
Iceberg dans le bucket bronze, sans transformation autre que l'ajout
de métadonnées d'ingestion.

Règle Bronze : tout est lu en STRING, jamais de type inféré automatiquement,
pour ne perdre aucune valeur sale silencieusement.
"""

import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent / "common"))
from spark_session import get_spark_session  # noqa: E402

# Racine du projet, peu importe d'où le script est lancé
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "sample"

# Un seul endroit pour dire quel fichier source va vers quelle table Bronze.
# On ignore volontairement clickstream et support_tickets pour le MVP.
SOURCES = [
    {"file": "crm_50000_customers_dirty_v3.csv", "table": "customers"},
    {"file": "product_catalog_dirty_30pct.csv", "table": "products"},
    {"file": "orders_300k_dirty.csv", "table": "orders"},
]

BATCH_ID = str(uuid.uuid4())
INGESTED_AT = datetime.now(timezone.utc).isoformat()


def ingest_one_source(spark, file_name: str, table_name: str) -> None:
    source_path = DATA_DIR / file_name

    if not source_path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {source_path}")

    # file:/// + chemin en slashes, obligatoire pour que Spark lise
    # correctement un chemin local sous Windows.
    spark_path = "file:///" + str(source_path).replace("\\", "/")

    print(f"Lecture de {file_name} ...")
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")   # tout en string, volontairement
        .csv(spark_path)
    )

    row_count = df.count()
    print(f"  -> {row_count} lignes lues, colonnes : {df.columns}")


    from pyspark.sql.functions import lit
    df_with_meta = (
        df.withColumn("_ingested_at", lit(INGESTED_AT))
        .withColumn("_source_file", lit(file_name))
        .withColumn("_batch_id", lit(BATCH_ID))
    )

    full_table_name = f"bronze.ecommerce.{table_name}"
    print(f"  -> écriture dans {full_table_name}")

    (
        df_with_meta.writeTo(full_table_name)
        .using("iceberg")
        .createOrReplace()  # MVP : on écrase à chaque run. On passera à
                            # append + gestion d'idempotence plus tard.
    )
    print(f"  -> OK ({row_count} lignes écrites)\n")


def main():
    spark = get_spark_session("bronze-ingestion")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze.ecommerce")

    for source in SOURCES:
        ingest_one_source(spark, source["file"], source["table"])

    print("Ingestion Bronze terminée pour :", [s["table"] for s in SOURCES])
    spark.stop()


if __name__ == "__main__":
    main()