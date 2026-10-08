"""
Transformation Silver -> Gold : construit les 4 tables business
définies dans le cahier des charges (section 9).

Décision métier documentée : seules les commandes status == 'success'
comptent dans le chiffre d'affaires et les métriques de vente.
Les commandes failed/refunded n'ont jamais généré de revenu réel,
les inclure fausserait le CA et le panier moyen.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent / "common"))
from spark_session import get_spark_session  # noqa: E402

from pyspark.sql.functions import (
    col, sum as spark_sum, countDistinct, round as spark_round,
)


def load_successful_orders(spark):
    """Base commune aux 4 tables Gold : uniquement les commandes réussies,
    avec le revenu de ligne déjà calculé (order_amount * quantity)."""
    return (
        spark.table("silver.ecommerce.orders_enriched")
        .filter(col("status") == "success")
        .withColumn("line_revenue", col("order_amount") * col("quantity"))
    )


def build_sales_daily(orders):
    return (
        orders.groupBy(col("order_date").alias("date"))
        .agg(
            countDistinct("order_id").alias("total_orders"),
            spark_round(spark_sum("line_revenue"), 2).alias("total_revenue"),
        )
        .withColumn(
            "average_order_value",
            spark_round(col("total_revenue") / col("total_orders"), 2),
        )
        .orderBy("date")
    )


def build_sales_by_product(orders):
    return (
        orders.groupBy("product_id", "product_name", "category")
        .agg(
            spark_sum("quantity").alias("units_sold"),
            spark_round(spark_sum("line_revenue"), 2).alias("revenue"),
        )
        .orderBy(col("revenue").desc())
    )


def build_sales_by_region(orders):
    # "region" = pays, le niveau le plus pertinent pour ce dataset international
    # (state/city donneraient des groupes trop fins et peu lisibles en BI).
    return (
        orders.groupBy(col("country").alias("region"))
        .agg(
            countDistinct("order_id").alias("orders"),
            spark_round(spark_sum("line_revenue"), 2).alias("revenue"),
        )
        .withColumn(
            "average_order_value",
            spark_round(col("revenue") / col("orders"), 2),
        )
        .orderBy(col("revenue").desc())
    )


def build_customer_metrics(orders):
    return (
        orders.groupBy("customer_id")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            spark_round(spark_sum("line_revenue"), 2).alias("total_spent"),
        )
        .withColumn(
            "average_order_value",
            spark_round(col("total_spent") / col("total_orders"), 2),
        )
        .orderBy(col("total_spent").desc())
    )


def main():
    spark = get_spark_session("gold-transform")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS gold.ecommerce")

    orders = load_successful_orders(spark)
    orders.cache()  # réutilisée 4 fois, on évite de relire/recalculer depuis Silver à chaque fois
    n_orders = orders.count()
    print(f"Base Gold : {n_orders} lignes de commandes 'success' (sur {spark.table('silver.ecommerce.orders_enriched').count()} au total)\n")

    tables = {
        "sales_daily": build_sales_daily(orders),
        "sales_by_product": build_sales_by_product(orders),
        "sales_by_region": build_sales_by_region(orders),
        "customer_metrics": build_customer_metrics(orders),
    }

    for name, df in tables.items():
        row_count = df.count()
        full_name = f"gold.ecommerce.{name}"
        df.writeTo(full_name).using("iceberg").createOrReplace()
        print(f"gold_{name:20s} : {row_count} lignes -> écrit dans {full_name}")

    orders.unpersist()
    print("\nGold terminé.")
    spark.stop()


if __name__ == "__main__":
    main()
