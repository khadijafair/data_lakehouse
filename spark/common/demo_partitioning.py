"""
Démonstration de l'effet du partitionnement sur silver.ecommerce.orders.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))
from spark_session import get_spark_session  # noqa: E402

spark = get_spark_session("demo-partitioning")

print("=== Partitions physiques créées par Iceberg ===")
spark.sql("SELECT * FROM silver.ecommerce.orders.partitions").show(30, truncate=False)

print("\n=== Plan d'exécution pour une requête filtrée sur mars 2024 ===")
df = spark.table("silver.ecommerce.orders").filter(
    "order_date >= '2024-03-01' AND order_date < '2024-04-01'"
)
df.explain(mode="formatted")

print(f"\nLignes retournées pour mars 2024 : {df.count()}")

spark.stop()
