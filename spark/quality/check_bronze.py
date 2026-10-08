"""Contrôle qualité Bronze : les 3 tables doivent exister et être non vides."""
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent / "common"))
from spark_session import get_spark_session  # noqa: E402

spark = get_spark_session("check-bronze")

for table in ["customers", "products", "orders"]:
    count = spark.table(f"bronze.ecommerce.{table}").count()
    print(f"bronze.{table} : {count} lignes")
    if count == 0:
        spark.stop()
        raise ValueError(f"ECHEC qualité : bronze.ecommerce.{table} est vide")

spark.stop()
print("Contrôle qualité Bronze : OK")
