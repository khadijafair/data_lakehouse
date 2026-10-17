"""
Exporte les 4 tables Gold en CSV pour connexion à Power BI / Metabase.
Power BI n'a pas de connecteur natif simple vers Iceberg/S3 en local,
donc on passe par un export de fichiers plats.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))
from spark_session import get_spark_session  # noqa: E402

spark = get_spark_session("export-gold-csv")

OUTPUT_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "gold_export"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TABLES = ["sales_daily", "sales_by_product", "sales_by_region", "customer_metrics"]

for table in TABLES:
    df = spark.table(f"gold.ecommerce.{table}")
    # toPandas() : les tables Gold sont petites (quelques centaines à
    # dizaines de milliers de lignes), donc un seul fichier CSV lisible
    # directement par Power BI, plutôt que des fragments Spark multiples.
    pandas_df = df.toPandas()
    out_path = OUTPUT_DIR / f"{table}.csv"
    pandas_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"{table} : {len(pandas_df)} lignes -> {out_path}")

spark.stop()
print(f"\nExport terminé dans {OUTPUT_DIR}")