"""
Démonstration de MERGE INTO Iceberg : ingestion incrémentale et idempotente.
Montre qu'relancer la même ingestion plusieurs fois ne duplique rien,
et qu'une vraie mise à jour (nouvelle ligne + ligne modifiée) est gérée
correctement sans réécrire toute la table.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))
from spark_session import get_spark_session  # noqa: E402
from pyspark.sql import Row
from pyspark.sql.functions import lit, current_timestamp

spark = get_spark_session("demo-merge-idempotent")
spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze.ecommerce")

TABLE = "bronze.ecommerce.customers_merge_demo"

# ---------- Étape 0 : table de démo vide, structure simplifiée ----------
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        customer_id STRING,
        email STRING,
        city STRING,
        _last_updated TIMESTAMP
    ) USING iceberg
""")
spark.sql(f"DELETE FROM {TABLE}")  # table vide pour un test reproductible

print("=== Étape 1 : première ingestion (3 clients) ===")
batch_1 = spark.createDataFrame([
    Row(customer_id="C001", email="a@test.com", city="Paris"),
    Row(customer_id="C002", email="b@test.com", city="Lyon"),
    Row(customer_id="C003", email="c@test.com", city="Nice"),
]).withColumn("_last_updated", current_timestamp())

batch_1.createOrReplaceTempView("batch_source")
spark.sql(f"""
    MERGE INTO {TABLE} t
    USING batch_source s
    ON t.customer_id = s.customer_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
count_after_1 = spark.table(TABLE).count()
print(f"Lignes après ingestion 1 : {count_after_1}")

print("\n=== Étape 2 : relancer EXACTEMENT le même batch (test idempotence) ===")
batch_1.createOrReplaceTempView("batch_source")
spark.sql(f"""
    MERGE INTO {TABLE} t
    USING batch_source s
    ON t.customer_id = s.customer_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
count_after_2 = spark.table(TABLE).count()
print(f"Lignes après re-ingestion du même batch : {count_after_2}")
print(f"--> Idempotent : {count_after_1 == count_after_2} (aucune duplication)")

print("\n=== Étape 3 : batch incrémental (1 nouveau client + 1 ville modifiée) ===")
batch_2 = spark.createDataFrame([
    Row(customer_id="C002", email="b@test.com", city="Marseille"),  # ville changée
    Row(customer_id="C004", email="d@test.com", city="Lille"),       # nouveau client
]).withColumn("_last_updated", current_timestamp())

batch_2.createOrReplaceTempView("batch_source")
spark.sql(f"""
    MERGE INTO {TABLE} t
    USING batch_source s
    ON t.customer_id = s.customer_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
count_after_3 = spark.table(TABLE).count()
print(f"Lignes après batch incrémental : {count_after_3}")
print(f"--> Attendu : 4 (3 existants + 1 nouveau, C002 mis à jour pas dupliqué)")

print("\n=== Résultat final de la table ===")
spark.table(TABLE).orderBy("customer_id").show(truncate=False)

print("\n=== Historique des opérations (preuve de l'incrémental) ===")
spark.sql(f"SELECT committed_at, operation, summary FROM {TABLE}.snapshots") \
    .show(10, truncate=False)

spark.stop()