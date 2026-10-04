"""
Diagnostic : combien de lignes ont un order_amount == NaN (valeur flottante
invalide qui n'est PAS détectée par isNull()).
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))
from spark_session import get_spark_session  # noqa: E402
from pyspark.sql.functions import col, isnan

spark = get_spark_session("diagnose-nan")

df = spark.table("silver.ecommerce.orders_enriched")

nan_count = df.filter(isnan(col("order_amount"))).count()
print(f"Lignes avec order_amount == NaN : {nan_count}")

df.filter(isnan(col("order_amount"))).select(
    "order_id", "order_amount_raw", "order_amount"
).show(10, truncate=False)

spark.stop()