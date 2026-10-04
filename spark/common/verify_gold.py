"""
Vérification du contenu des 4 tables Gold contre les questions métier
définies dans le cahier des charges (section 4).
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))
from spark_session import get_spark_session  # noqa: E402
from pyspark.sql.functions import date_format, sum as spark_sum, round as spark_round

spark = get_spark_session("verify-gold")

# ---------- Question : quelle est l'évolution des ventes / le CA mensuel ? ----------
print("\n=== CA mensuel (agrégé depuis gold_sales_daily) ===")
daily = spark.table("gold.ecommerce.sales_daily")
monthly = (
    daily.withColumn("month", date_format("date", "yyyy-MM"))
    .groupBy("month")
    .agg(
        spark_sum("total_orders").alias("total_orders"),
        spark_round(spark_sum("total_revenue"), 2).alias("total_revenue"),
    )
    .orderBy("month")
)
monthly.show(50, truncate=False)

# ---------- Question : quels produits sont les plus vendus ? ----------
print("\n=== Top 10 produits par revenu ===")
spark.table("gold.ecommerce.sales_by_product") \
    .orderBy("revenue", ascending=False) \
    .show(10, truncate=False)

print("\n=== Top 10 produits par unités vendues ===")
spark.table("gold.ecommerce.sales_by_product") \
    .orderBy("units_sold", ascending=False) \
    .show(10, truncate=False)

# ---------- Question : quelles régions génèrent le plus de revenus ? ----------
print("\n=== Top 10 régions (pays) par revenu ===")
spark.table("gold.ecommerce.sales_by_region") \
    .orderBy("revenue", ascending=False) \
    .show(10, truncate=False)

# ---------- Question : quelle est la valeur moyenne d'une commande ? ----------
print("\n=== Panier moyen global (recalculé depuis sales_daily) ===")
totals = daily.agg(
    spark_sum("total_revenue").alias("total_revenue"),
    spark_sum("total_orders").alias("total_orders"),
).collect()[0]
global_aov = totals["total_revenue"] / totals["total_orders"]
print(f"CA total     : {totals['total_revenue']:,.2f}")
print(f"Commandes    : {totals['total_orders']:,}")
print(f"Panier moyen : {global_aov:,.2f}")

# ---------- Question : quels sont les clients les plus actifs ? ----------
print("\n=== Top 10 clients par CA total ===")
spark.table("gold.ecommerce.customer_metrics") \
    .orderBy("total_spent", ascending=False) \
    .show(10, truncate=False)

print("\n=== Top 10 clients par nombre de commandes ===")
spark.table("gold.ecommerce.customer_metrics") \
    .orderBy("total_orders", ascending=False) \
    .show(10, truncate=False)

spark.stop()