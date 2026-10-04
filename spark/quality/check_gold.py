"""Contrôle qualité Gold : les 4 tables doivent exister, ne pas être vides,
et le CA total doit être un nombre positif fini (protection contre le bug
NaN déjà rencontré une fois)."""
import sys
import math
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent / "common"))
from spark_session import get_spark_session  # noqa: E402
from pyspark.sql.functions import sum as spark_sum

spark = get_spark_session("check-gold")

for table in ["sales_daily", "sales_by_product", "sales_by_region", "customer_metrics"]:
    count = spark.table(f"gold.ecommerce.{table}").count()
    print(f"gold.{table} : {count} lignes")
    if count == 0:
        spark.stop()
        raise ValueError(f"ECHEC qualité : gold.ecommerce.{table} est vide")

total_revenue = (
    spark.table("gold.ecommerce.sales_daily")
    .agg(spark_sum("total_revenue").alias("total"))
    .collect()[0]["total"]
)
print(f"CA total Gold : {total_revenue}")
if total_revenue is None or math.isnan(total_revenue) or total_revenue <= 0:
    spark.stop()
    raise ValueError(f"ECHEC qualité : CA total invalide ({total_revenue})")

spark.stop()
print("Contrôle qualité Gold : OK")