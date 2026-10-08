"""
Transformation Bronze -> Silver pour customers.
Nettoyage : dédup sur customer_id, normalisation des chaînes,
typage des dates. Les doublons "même personne, id différent"
(email/téléphone partagés) sont volontairement conservés
(cf. décision documentée : risque de faux positifs, hors scope MVP).
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent / "common"))
from spark_session import get_spark_session  # noqa: E402
from normalizers import (  # noqa: E402
    normalize_category, normalize_quantity,
    normalize_status, normalize_payment,
)

from pyspark.sql import Window
from pyspark.sql.functions import (
    col, row_number, trim, lower, initcap, to_date, lit
)

from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import (
    udf, when,
)


from pyspark.sql.functions import isnan

# Ajout pour le partitionnement de orders / orders_enriched
from pyspark.sql.functions import months

# UDFs construites à partir des fonctions pures importées de normalizers.py
# (ces fonctions sont testées indépendamment dans tests/test_silver.py,
# sans dépendance à Spark)
normalize_category_udf = udf(normalize_category)
normalize_quantity_udf = udf(normalize_quantity, IntegerType())
normalize_status_udf = udf(normalize_status)
normalize_payment_udf = udf(normalize_payment)


def clean_customers(spark):
    df = spark.table("bronze.ecommerce.customers")
    rows_before = df.count()

    # 1. Dédup sur customer_id : on garde la ligne la plus récente
    #    (_ingested_at le plus grand) en cas de doublon exact de clé.
    window = Window.partitionBy("customer_id").orderBy(col("_ingested_at").desc())
    df_dedup = (
        df.withColumn("_rn", row_number().over(window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )
    rows_after_dedup = df_dedup.count()

    # 2. Normalisation des chaînes : trim + casse cohérente
    df_clean = (
        df_dedup.withColumn("first_name", initcap(trim(col("first_name"))))
        .withColumn("last_name", initcap(trim(col("last_name"))))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("city", initcap(trim(col("city"))))
        .withColumn("state", trim(col("state")))
        .withColumn("country", initcap(trim(col("country"))))
    )

    # 3. Typage des dates. Les formats sont inconnus/sales -> to_date
    #    renvoie NULL si le parsing échoue, au lieu de planter.
    #    On garde la colonne originale en _raw pour traçabilité du rejet.
    df_typed = (
        df_clean.withColumn("dob_raw", col("dob"))
        .withColumn("dob", to_date(col("dob")))

        .withColumn("signup_date_raw", col("signup_date"))
        .withColumn("signup_date", to_date(col("signup_date")))
    )

    # 4. Validation : customer_id et email ne doivent pas être nuls.
    #    On sépare les lignes valides des lignes rejetées.
    valid_df = df_typed.filter(
        col("customer_id").isNotNull() & col("email").isNotNull()
    )
    rejected_df = df_typed.filter(
        col("customer_id").isNull() | col("email").isNull()
    ).withColumn("_rejection_reason", lit("customer_id ou email manquant"))

    print(f"customers : {rows_before} lignes brutes -> {rows_after_dedup} après dédup "
          f"-> {valid_df.count()} valides, {rejected_df.count()} rejetées")

    return valid_df, rejected_df


def clean_products(spark):
    df = spark.table("bronze.ecommerce.products")
    rows_before = df.count()

    df_clean = (
        df.withColumn("price_raw", col("price"))
        .withColumn("price", trim(col("price")).cast(DoubleType()))
        .withColumn("category_raw", col("category"))
        .withColumn("category", normalize_category_udf(col("category")))
    )

    valid_df = df_clean.filter(
        col("product_id").isNotNull()
        & col("price").isNotNull() & ~isnan(col("price")) & (col("price") >= 0)
        & col("category").isNotNull()
    )
    rejected_df = df_clean.filter(
        col("product_id").isNull()
        | col("price").isNull() | isnan(col("price")) | (col("price") < 0)
        | col("category").isNull()
    ).withColumn("_rejection_reason", lit("price ou category invalide"))

    print(f"products : {rows_before} lignes brutes -> "
          f"{valid_df.count()} valides, {rejected_df.count()} rejetées")
    return valid_df, rejected_df


def clean_orders(spark):
    df = spark.table("bronze.ecommerce.orders")
    rows_before = df.count()

    from pyspark.sql.functions import instr
    parsed_date = when(
        instr(col("order_date"), "T") > 0,  # format timestamp = corrompu par construction
        None,
    ).otherwise(to_date(col("order_date"), "yyyy-MM-dd"))

    df_clean = (
        df.withColumn("order_amount_raw", col("order_amount"))
        .withColumn("order_amount", trim(col("order_amount")).cast(DoubleType()))
        .withColumn("quantity_raw", col("quantity"))
        .withColumn("quantity", normalize_quantity_udf(col("quantity")))
        .withColumn("status_raw", col("status"))
        .withColumn("status", normalize_status_udf(col("status")))
        .withColumn("payment_method_raw", col("payment_method"))
        .withColumn("payment_method", normalize_payment_udf(col("payment_method")))
        .withColumn("order_date_raw", col("order_date"))
        .withColumn("order_date", parsed_date)

    )
    valid_df = df_clean.filter(
        col("order_id").isNotNull()
        & col("customer_id").isNotNull()
        & col("order_amount").isNotNull() & ~isnan(col("order_amount")) & (col("order_amount") >= 0)
        & col("quantity").isNotNull() & (col("quantity") > 0)
        & col("status").isNotNull()
        & col("payment_method").isNotNull()
        & col("order_date").isNotNull()
    )
    rejected_df = df_clean.subtract(valid_df).withColumn(
        "_rejection_reason", lit("un ou plusieurs champs invalides")
    )

    print(f"orders : {rows_before} lignes brutes -> "
          f"{valid_df.count()} valides, {rejected_df.count()} rejetées")
    return valid_df, rejected_df


def build_silver_orders(spark):
    """
    Jointure finale : enrichit orders avec les infos customers et products,
    pour préparer directement les besoins des tables Gold (région, catégorie...).
    Une commande dont le customer_id ou product_id ne correspond à aucune ligne
    Silver valide est considérée orpheline (problème de cohérence référentielle,
    cf. section 10 du cahier des charges) et isolée séparément.
    """
    orders = spark.table("silver.ecommerce.orders")
    customers = spark.table("silver.ecommerce.customers").select(
        col("customer_id"),
        col("city"), col("state"), col("country"),
    )
    products = spark.table("silver.ecommerce.products").select(
        col("product_id"),
        col("product_name"),
        col("category"),
    )

    joined = (
        orders.join(customers, "customer_id", "left")
        .join(products, "product_id", "left")
    )

    valid_df = joined.filter(col("city").isNotNull() & col("product_name").isNotNull())
    orphaned_df = joined.filter(
        col("city").isNull() | col("product_name").isNull()
    ).withColumn("_rejection_reason", lit("customer_id ou product_id introuvable en Silver"))

    print(f"silver_orders (jointure) : {joined.count()} lignes -> "
          f"{valid_df.count()} valides, {orphaned_df.count()} orphelines")

    return valid_df, orphaned_df


def main():
    spark = get_spark_session("silver-transform")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS silver.ecommerce")

    # customers et products : pas de partitionnement
    # (volumes trop petits - 47k et 405 lignes - pour en tirer un bénéfice)
    for table_name, clean_fn in [
        ("customers", clean_customers),
        ("products", clean_products),
    ]:
        valid_df, rejected_df = clean_fn(spark)
        valid_df.writeTo(f"silver.ecommerce.{table_name}").using("iceberg").createOrReplace()
        if rejected_df.count() > 0:
            rejected_df.writeTo(f"silver.ecommerce.{table_name}_rejected").using("iceberg").createOrReplace()

    # orders : partitionné par année/mois de order_date (section 13 du cahier des charges)
    valid_orders_raw, rejected_orders_raw = clean_orders(spark)
    (
        valid_orders_raw
        .writeTo("silver.ecommerce.orders")
        .using("iceberg")
        .partitionedBy(months("order_date"))
        .createOrReplace()
    )
    if rejected_orders_raw.count() > 0:
        rejected_orders_raw.writeTo("silver.ecommerce.orders_rejected").using("iceberg").createOrReplace()

    # Jointure finale, une fois les 3 tables Silver individuelles prêtes
    # orders_enriched : même partitionnement que orders, pour les mêmes raisons
    valid_orders, orphaned_orders = build_silver_orders(spark)
    (
        valid_orders
        .writeTo("silver.ecommerce.orders_enriched")
        .using("iceberg")
        .partitionedBy(months("order_date"))
        .createOrReplace()
    )
    if orphaned_orders.count() > 0:
        orphaned_orders.writeTo("silver.ecommerce.orders_enriched_rejected").using("iceberg").createOrReplace()

    print("Silver terminé pour customers, products, orders.")
    spark.stop()


if __name__ == "__main__":
    main()
