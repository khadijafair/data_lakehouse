"""
Exporte les 4 tables Gold en CSV pour connexion à Power BI / Metabase.
Power BI n'a pas de connecteur natif simple vers Iceberg/S3 en local,
donc on passe par un export de fichiers plats.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))
from spark_session import get_spark_session  # noqa: E402
from pyspark.sql.functions import col  # noqa: E402

spark = get_spark_session("export-gold-csv")

OUTPUT_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "gold_export"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Tri explicite à l'export : une table Iceberg ne garantit pas l'ordre des lignes
# (et dbt ne trie pas). Même ordre que l'ancienne couche Gold Spark.
SORT_ORDER = {
    "sales_daily": ("date", True, "date"),
    "sales_by_product": ("revenue", False, "product_id"),
    "sales_by_region": ("revenue", False, "region"),
    "customer_metrics": ("total_spent", False, "customer_id"),
}
TABLES = list(SORT_ORDER)

for table in TABLES:
    sort_col, ascending, tie_breaker = SORT_ORDER[table]
    df = spark.table(f"gold.ecommerce.{table}").orderBy(
        col(sort_col).asc() if ascending else col(sort_col).desc(),
        col(tie_breaker).asc(),  # ordre stable en cas d'égalité
    )
    # toPandas() : les tables Gold sont petites (quelques centaines à
    # dizaines de milliers de lignes), donc un seul fichier CSV lisible
    # directement par Power BI, plutôt que des fragments Spark multiples.
    pandas_df = df.toPandas()
    out_path = OUTPUT_DIR / f"{table}.csv"
    pandas_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"{table} : {len(pandas_df)} lignes -> {out_path}")

spark.stop()
print(f"\nExport terminé dans {OUTPUT_DIR}")
