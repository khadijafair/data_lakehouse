"""
Inspection des valeurs sales de products et orders, avant d'écrire
les règles de nettoyage Silver. Aucune écriture, lecture seule.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))
from spark_session import get_spark_session  # noqa: E402

spark = get_spark_session("inspect-products-orders")

print("\n=== PRODUCTS : valeurs distinctes de price (30 au hasard) ===")
spark.table("bronze.ecommerce.products").select("price").distinct().show(30, truncate=False)

print("\n=== PRODUCTS : valeurs distinctes de category (30 au hasard) ===")
spark.table("bronze.ecommerce.products").select("category").distinct().show(30, truncate=False)

orders = spark.table("bronze.ecommerce.orders")

print("\n=== ORDERS : valeurs distinctes de order_amount (30 au hasard) ===")
orders.select("order_amount").distinct().show(30, truncate=False)

print("\n=== ORDERS : valeurs distinctes de quantity (30 au hasard) ===")
orders.select("quantity").distinct().show(30, truncate=False)

print("\n=== ORDERS : valeurs distinctes de status (toutes, probablement peu nombreuses) ===")
orders.select("status").distinct().show(50, truncate=False)

print("\n=== ORDERS : valeurs distinctes de payment_method (toutes) ===")
orders.select("payment_method").distinct().show(50, truncate=False)

print("\n=== ORDERS : exemples de order_date (30 au hasard) ===")
orders.select("order_date").distinct().show(30, truncate=False)

spark.stop()
