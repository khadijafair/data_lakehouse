"""
Contrôles qualité formalisés avec Great Expectations, en complément
(pas en remplacement) des scripts check_bronze.py / check_silver.py /
check_gold.py déjà utilisés dans le DAG Airflow.

Apporte : une suite de règles documentées et versionnées, et un rapport
HTML consultable (Data Docs) plutôt que de simples print() de comptages.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent / "common"))
from spark_session import get_spark_session  # noqa: E402

import great_expectations as gx

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
GX_ROOT = PROJECT_ROOT / "gx"  # dossier de config GX, créé automatiquement

CANONICAL_CATEGORIES = [
    "electronics", "sports", "beauty", "automotive",
    "home", "kitchen", "toys", "clothing",
]
VALID_STATUSES = ["success", "failed", "refunded"]
VALID_PAYMENT_METHODS = ["wallet", "cash", "upi", "card"]


def get_validator(context, df, asset_name, suite_name):
    """Crée (ou réutilise) un datasource/asset/suite GX pour un DataFrame Spark donné."""
    datasource = context.sources.add_or_update_spark(name="spark_datasource")
    asset = datasource.add_dataframe_asset(name=asset_name)
    batch_request = asset.build_batch_request(dataframe=df)
    suite = context.add_or_update_expectation_suite(suite_name)
    validator = context.get_validator(batch_request=batch_request, expectation_suite=suite)
    return validator


def run_checkpoint(context, validator, checkpoint_name):
    checkpoint = context.add_or_update_checkpoint(name=checkpoint_name, validator=validator)
    result = checkpoint.run()
    status = "SUCCESS" if result["success"] else "FAILED"
    print(f"  -> Checkpoint {checkpoint_name} : {status}")
    return result["success"]


def check_bronze(spark, context):
    print("\n=== BRONZE ===")
    all_ok = True
    for table, required_cols in [
        ("customers", ["customer_id", "email"]),
        ("products", ["product_id", "price", "category"]),
        ("orders", ["order_id", "customer_id", "product_id"]),
    ]:
        df = spark.table(f"bronze.ecommerce.{table}")
        v = get_validator(context, df, f"bronze_{table}", f"bronze_{table}_suite")
        v.expect_table_row_count_to_be_between(min_value=1)
        for col in required_cols:
            v.expect_column_to_exist(col)
        v.save_expectation_suite(discard_failed_expectations=False)
        ok = run_checkpoint(context, v, f"bronze_{table}_checkpoint")
        all_ok = all_ok and ok
    return all_ok


def check_silver(spark, context):
    print("\n=== SILVER ===")
    all_ok = True

    # customers
    df = spark.table("silver.ecommerce.customers")
    v = get_validator(context, df, "silver_customers", "silver_customers_suite")
    v.expect_column_values_to_not_be_null("customer_id")
    v.expect_column_values_to_not_be_null("email")
    v.expect_column_values_to_be_unique("customer_id")
    v.save_expectation_suite(discard_failed_expectations=False)
    all_ok = run_checkpoint(context, v, "silver_customers_checkpoint") and all_ok

    # products
    df = spark.table("silver.ecommerce.products")
    v = get_validator(context, df, "silver_products", "silver_products_suite")
    v.expect_column_values_to_not_be_null("product_id")
    v.expect_column_values_to_be_between("price", min_value=0)
    v.expect_column_values_to_be_in_set("category", CANONICAL_CATEGORIES)
    v.save_expectation_suite(discard_failed_expectations=False)
    all_ok = run_checkpoint(context, v, "silver_products_checkpoint") and all_ok

    # orders
    df = spark.table("silver.ecommerce.orders")
    v = get_validator(context, df, "silver_orders", "silver_orders_suite")
    v.expect_column_values_to_not_be_null("order_id")
    v.expect_column_values_to_be_between("order_amount", min_value=0)
    v.expect_column_values_to_be_between("quantity", min_value=1)
    v.expect_column_values_to_be_in_set("status", VALID_STATUSES)
    v.expect_column_values_to_be_in_set("payment_method", VALID_PAYMENT_METHODS)
    v.save_expectation_suite(discard_failed_expectations=False)
    all_ok = run_checkpoint(context, v, "silver_orders_checkpoint") and all_ok

    return all_ok


def check_gold(spark, context):
    print("\n=== GOLD ===")
    all_ok = True

    df = spark.table("gold.ecommerce.sales_daily")
    v = get_validator(context, df, "gold_sales_daily", "gold_sales_daily_suite")
    v.expect_column_values_to_not_be_null("date")
    v.expect_column_values_to_be_between("total_revenue", min_value=0)
    v.expect_column_values_to_be_between("total_orders", min_value=1)
    v.save_expectation_suite(discard_failed_expectations=False)
    all_ok = run_checkpoint(context, v, "gold_sales_daily_checkpoint") and all_ok

    df = spark.table("gold.ecommerce.customer_metrics")
    v = get_validator(context, df, "gold_customer_metrics", "gold_customer_metrics_suite")
    v.expect_column_values_to_be_between("total_spent", min_value=0)
    v.expect_column_values_to_be_between("total_orders", min_value=1)
    v.save_expectation_suite(discard_failed_expectations=False)
    all_ok = run_checkpoint(context, v, "gold_customer_metrics_checkpoint") and all_ok

    return all_ok


def main():
    spark = get_spark_session("great-expectations-checks")
    context = gx.get_context(mode="file", project_root_dir=str(GX_ROOT))

    bronze_ok = check_bronze(spark, context)
    silver_ok = check_silver(spark, context)
    gold_ok = check_gold(spark, context)

    print("\n=== RÉSUMÉ ===")
    print(f"Bronze : {'OK' if bronze_ok else 'ÉCHEC'}")
    print(f"Silver : {'OK' if silver_ok else 'ÉCHEC'}")
    print(f"Gold   : {'OK' if gold_ok else 'ÉCHEC'}")

    context.build_data_docs()
    docs_path = GX_ROOT / "uncommitted" / "data_docs" / "local_site" / "index.html"
    print(f"\nRapport HTML généré : {docs_path}")
    print("Ouvre ce fichier dans un navigateur pour voir le détail des validations.")

    spark.stop()


if __name__ == "__main__":
    main()
