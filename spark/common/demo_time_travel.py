"""
Démonstration du time travel Iceberg sur gold.ecommerce.sales_daily.
Montre : l'historique des snapshots, une requête sur une ancienne version,
et une comparaison avec la version actuelle.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))
from spark_session import get_spark_session  # noqa: E402

spark = get_spark_session("demo-time-travel")

print("=== 1. Historique des snapshots (versions) de gold.ecommerce.sales_daily ===")
history_df = spark.sql("SELECT * FROM gold.ecommerce.sales_daily.history")
history_df.show(20, truncate=False)

print("\n=== 2. Détail des snapshots (taille, nombre de fichiers) ===")
spark.sql("SELECT * FROM gold.ecommerce.sales_daily.snapshots").select(
    "committed_at", "snapshot_id", "operation"
).show(20, truncate=False)

# On récupère le snapshot le plus ANCIEN pour la démonstration
oldest_snapshot_id = (
    history_df.orderBy("made_current_at").first()["snapshot_id"]
)
print(f"\nSnapshot le plus ancien sélectionné pour le time travel : {oldest_snapshot_id}")

print("\n=== 3. Requête sur l'ANCIENNE version (VERSION AS OF) ===")
old_df = spark.sql(f"""
    SELECT * FROM gold.ecommerce.sales_daily VERSION AS OF {oldest_snapshot_id}
    ORDER BY date
""")
old_count = old_df.count()
old_total_revenue = old_df.selectExpr("sum(total_revenue) as total").collect()[0]["total"]
print(f"Ancienne version -> {old_count} lignes, CA total = {old_total_revenue:,.2f}")
old_df.show(5, truncate=False)

print("\n=== 4. Requête sur la version ACTUELLE (sans time travel) ===")
current_df = spark.table("gold.ecommerce.sales_daily")
current_count = current_df.count()
current_total_revenue = current_df.selectExpr("sum(total_revenue) as total").collect()[0]["total"]
print(f"Version actuelle  -> {current_count} lignes, CA total = {current_total_revenue:,.2f}")

print("\n=== 5. Comparaison ===")
print(f"Différence de lignes      : {current_count - old_count:+d}")
print(f"Différence de CA          : {current_total_revenue - old_total_revenue:+,.2f}")
print(
    "\nCette différence illustre concrètement l'intérêt du time travel : "
    "on peut quantifier l'impact exact d'une évolution du pipeline "
    "(ici, notamment le correctif du bug NaN et le partitionnement) "
    "en comparant directement deux versions de la même table, "
    "sans avoir besoin de sauvegardes manuelles séparées."
)

spark.stop()