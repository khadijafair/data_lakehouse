"""
Vérification du contenu de silver.ecommerce.orders_enriched
avant de passer à la couche Gold.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))
from spark_session import get_spark_session  # noqa: E402
from pyspark.sql.functions import col

spark = get_spark_session("verify-silver")

df = spark.table("silver.ecommerce.orders_enriched")

print("\n=== Schéma de la table ===")
df.printSchema()

print(f"\n=== Nombre de lignes ===\n{df.count()}")

print("\n=== 10 lignes d'exemple (colonnes clés) ===")
df.select(
    "order_id", "customer_id", "product_id",
    "order_amount", "order_date", "status", "payment_method",
    "city", "state", "country",
    "product_name", "category",
).show(10, truncate=False)

print("\n=== Aucune valeur nulle ne doit apparaître ici (colonnes critiques) ===")
for c in ["order_amount", "order_date", "city", "product_name", "category"]:
    n_nulls = df.filter(col(c).isNull()).count()
    print(f"{c:15s} : {n_nulls} valeurs nulles")

print("\n=== Vérification arithmétique simple : CA total ===")
total_revenue = df.selectExpr("sum(order_amount * quantity) as total").collect()[0]["total"]
print(f"Chiffre d'affaires total (valid orders) : {total_revenue:,.2f}")

print("\n=== Répartition par status (doit être propre : success/failed/refunded) ===")
df.groupBy("status").count().show()

print("\n=== Répartition par category (doit être les 8 catégories canoniques) ===")
df.groupBy("category").count().orderBy("count", ascending=False).show()

spark.stop()
