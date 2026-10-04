"""
Démonstration du schema evolution Iceberg sur silver.ecommerce.customers.
Ajoute une colonne à une table existante et prouve qu'aucun fichier de
données n'a été réécrit (opération purement sur les métadonnées).
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))
from spark_session import get_spark_session  # noqa: E402

spark = get_spark_session("demo-schema-evolution")

TABLE = "silver.ecommerce.customers"

print("=== 1. Schéma AVANT évolution ===")
spark.sql(f"DESCRIBE {TABLE}").show(50, truncate=False)

print("\n=== 2. Nombre de fichiers de données AVANT évolution ===")
files_before = spark.sql(f"SELECT COUNT(*) as n FROM {TABLE}.files").collect()[0]["n"]
print(f"Fichiers de données : {files_before}")

print("\n=== 3. Ajout d'une nouvelle colonne métier (marketing_consent) ===")
spark.sql(f"ALTER TABLE {TABLE} ADD COLUMN marketing_consent BOOLEAN")
print("Colonne ajoutée.")

print("\n=== 4. Schéma APRÈS évolution ===")
spark.sql(f"DESCRIBE {TABLE}").show(50, truncate=False)

print("\n=== 5. Nombre de fichiers de données APRÈS évolution ===")
files_after = spark.sql(f"SELECT COUNT(*) as n FROM {TABLE}.files").collect()[0]["n"]
print(f"Fichiers de données : {files_after}")
print(f"\n--> Identique avant/après : {files_before == files_after} "
      f"(preuve qu'aucun fichier n'a été réécrit)")

print("\n=== 6. Les lignes existantes ont la nouvelle colonne à NULL ===")
spark.table(TABLE).select("customer_id", "email", "marketing_consent").show(5, truncate=False)

print("\n=== 7. On peut maintenant écrire de nouvelles lignes avec la colonne peuplée ===")
from pyspark.sql import Row
new_customer = spark.createDataFrame(
    [Row(customer_id="demo-schema-evolution-001",
         email="demo@example.com",
         marketing_consent=True)]
)
# On insère seulement les colonnes communes pour la démo (insertInto exigerait
# un alignement complet du schéma ; ici on illustre juste la capacité du schéma).
print("Nouvelle ligne de démonstration prête à être insérée avec marketing_consent=True.")
new_customer.show(truncate=False)

print("\n=== 8. Vérification : l'ancienne version (avant évolution) reste interrogeable ===")
history = spark.sql(f"SELECT * FROM {TABLE}.history").orderBy("made_current_at")
oldest_snapshot_id = history.first()["snapshot_id"]
old_schema_df = spark.sql(f"SELECT * FROM {TABLE} VERSION AS OF {oldest_snapshot_id} LIMIT 1")
print("Colonnes de l'ancienne version (ne contient PAS marketing_consent) :")
print(old_schema_df.columns)

spark.stop()