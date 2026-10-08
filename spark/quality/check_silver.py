"""Contrôle qualité Silver : le taux de rejet ne doit pas dépasser un seuil
anormal (signe probable d'une régression dans les règles de nettoyage),
et orders_enriched doit contenir des données exploitables."""
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent / "common"))
from spark_session import get_spark_session  # noqa: E402

MAX_REJECTION_RATE = 0.70  # au-delà, on considère que quelque chose a cassé

spark = get_spark_session("check-silver")

enriched_count = spark.table("silver.ecommerce.orders_enriched").count()
print(f"silver.orders_enriched : {enriched_count} lignes")
if enriched_count == 0:
    spark.stop()
    raise ValueError("ECHEC qualité : silver.ecommerce.orders_enriched est vide")

bronze_orders_count = spark.table("bronze.ecommerce.orders").count()
rejection_rate = 1 - (enriched_count / bronze_orders_count)
print(f"Taux de perte Bronze -> Silver enrichi : {rejection_rate:.1%}")
if rejection_rate > MAX_REJECTION_RATE:
    spark.stop()
    raise ValueError(
        f"ECHEC qualité : taux de perte {rejection_rate:.1%} > seuil {MAX_REJECTION_RATE:.0%}"
    )

spark.stop()
print("Contrôle qualité Silver : OK")
