"""
Génère un rapport consolidé Markdown après un run complet du pipeline.
Rassemble les métriques de chaque couche (Bronze/Silver/Gold) et le
statut qualité, en un seul document lisible et archivable.
"""

import sys
import time
from pathlib import Path
from datetime import datetime, timezone

sys.path.append(str(Path(__file__).resolve().parent.parent / "common"))
from spark_session import get_spark_session  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
REPORTS_DIR = PROJECT_ROOT / "docs" / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

start_time = time.time()
spark = get_spark_session("generate-report")

run_timestamp = datetime.now(timezone.utc)
lines = []

lines.append(f"# Rapport d'exécution — Pipeline Lakehouse")
lines.append(f"\n**Date du run** : {run_timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC\n")

# ---------- BRONZE ----------
lines.append("## Bronze\n")
lines.append("| Table | Lignes |")
lines.append("|---|---|")
bronze_counts = {}
for table in ["customers", "products", "orders"]:
    count = spark.table(f"bronze.ecommerce.{table}").count()
    bronze_counts[table] = count
    lines.append(f"| {table} | {count:,} |")

# ---------- SILVER ----------
lines.append("\n## Silver\n")
lines.append("| Table | Valides | Rejetées | Taux de conservation |")
lines.append("|---|---|---|---|")
for table in ["customers", "products", "orders"]:
    valid = spark.table(f"silver.ecommerce.{table}").count()
    try:
        rejected = spark.table(f"silver.ecommerce.{table}_rejected").count()
    except Exception:
        rejected = 0
    total = valid + rejected
    rate = (valid / total * 100) if total > 0 else 0
    lines.append(f"| {table} | {valid:,} | {rejected:,} | {rate:.1f}% |")

enriched = spark.table("silver.ecommerce.orders_enriched").count()
try:
    enriched_rejected = spark.table("silver.ecommerce.orders_enriched_rejected").count()
except Exception:
    enriched_rejected = 0
lines.append(f"| orders_enriched (jointure) | {enriched:,} | {enriched_rejected:,} | "
             f"{(enriched/(enriched+enriched_rejected)*100 if (enriched+enriched_rejected) > 0 else 0):.1f}% |")

# ---------- GOLD ----------
lines.append("\n## Gold\n")
lines.append("| Table | Lignes |")
lines.append("|---|---|")
gold_counts = {}
for table in ["sales_daily", "sales_by_product", "sales_by_region", "customer_metrics"]:
    count = spark.table(f"gold.ecommerce.{table}").count()
    gold_counts[table] = count
    lines.append(f"| {table} | {count:,} |")

total_revenue = (
    spark.table("gold.ecommerce.sales_daily")
    .selectExpr("sum(total_revenue) as total")
    .collect()[0]["total"]
)
lines.append(f"\n**Chiffre d'affaires total (commandes success)** : {total_revenue:,.2f}")

# ---------- QUALITÉ ----------
lines.append("\n## Qualité des données\n")
lines.append("Contrôles qualité exécutés : `check_bronze.py`, `check_silver.py`, `check_gold.py` "
              "(bloquants dans le DAG Airflow) + suites Great Expectations "
              "(voir `gx/uncommitted/data_docs/local_site/index.html` pour le détail).")

# ---------- DURÉE ----------
elapsed = time.time() - start_time
lines.append(f"\n## Durée de génération de ce rapport\n")
lines.append(f"{elapsed:.1f} secondes")

spark.stop()

report_filename = f"run_{run_timestamp.strftime('%Y%m%d_%H%M%S')}.md"
report_path = REPORTS_DIR / report_filename
report_path.write_text("\n".join(lines), encoding="utf-8")

print(f"Rapport généré : {report_path}")
print("\n".join(lines))