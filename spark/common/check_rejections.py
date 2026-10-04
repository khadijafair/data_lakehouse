"""
Analyse rapide de pourquoi les lignes orders sont rejetées,
colonne par colonne, pour valider que nos règles ont du sens.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))
from spark_session import get_spark_session  # noqa: E402
from pyspark.sql.functions import col

spark = get_spark_session("check-rejections")
rejected = spark.table("silver.ecommerce.orders_rejected")

total = rejected.count()
print(f"Total rejetées : {total}\n")

checks = {
    "order_amount invalide (null ou <0)": (col("order_amount").isNull()) | (col("order_amount") < 0),
    "quantity invalide (null ou <=0)": (col("quantity").isNull()) | (col("quantity") <= 0),
    "status non reconnu": col("status").isNull(),
    "payment_method non reconnu": col("payment_method").isNull(),
    "order_date invalide (null ou future)": col("order_date").isNull(),
}

for label, condition in checks.items():
    count = rejected.filter(condition).count()
    pct = 100 * count / total
    print(f"{label:45s} : {count:>7} ({pct:5.1f}%)")

spark.stop()
