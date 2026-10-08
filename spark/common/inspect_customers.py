"""
Inspection rapide de bronze.ecommerce.customers pour comprendre :
1. le format de device_id(s) quand il y en a plusieurs
2. comment repérer les doublons de profils
Aucune écriture, uniquement de la lecture/analyse.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))
from spark_session import get_spark_session  # noqa: E402

spark = get_spark_session("inspect-customers")

df = spark.table("bronze.ecommerce.customers")

print("\n=== 1. Exemples de device_id(s) avec plusieurs valeurs ===")
(
    df.select("customer_id", "`device_id(s)`")
    .filter(df["`device_id(s)`"].contains(","))  # suppose une séparation par virgule
    .show(10, truncate=False)
)

print("\n=== 2. Format général de device_id(s) (10 valeurs au hasard) ===")
df.select("`device_id(s)`").distinct().show(10, truncate=False)

print("\n=== 3. Doublons par customer_id exact (même clé répétée) ===")
df.groupBy("customer_id").count().filter("count > 1").show(10)

print("\n=== 4. Doublons potentiels par email (même personne, id différent) ===")
(
    df.filter(df.email.isNotNull())
    .groupBy("email")
    .count()
    .filter("count > 1")
    .orderBy("count", ascending=False)
    .show(10)
)

print("\n=== 5. Doublons potentiels par (first_name, last_name, phone_number) ===")
(
    df.groupBy("first_name", "last_name", "phone_number")
    .count()
    .filter("count > 1")
    .orderBy("count", ascending=False)
    .show(10)
)

spark.stop()
