"""
Migration unique : catalogue Iceberg Hadoop (fichiers dans MinIO) -> catalogue JDBC (PostgreSQL).

Pour chaque table des anciens catalogues Hadoop, on enregistre son dernier
fichier de métadonnées dans le catalogue JDBC (procédure Iceberg
`register_table`). Aucune donnée n'est copiée ni réécrite : la table garde
ses fichiers, ses snapshots et donc son historique de time travel
(dont la démo de l'écart de 821 M dû au bug NaN).

À lancer une seule fois, après le passage à spark_session.py en JDBC :
    cd spark/common && python migrate_catalog_to_jdbc.py
Sans danger si relancé : les tables déjà présentes dans le catalogue JDBC sont ignorées.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))
from spark_session import LAYERS, get_spark_session  # noqa: E402

NAMESPACE = "ecommerce"


def legacy_conf() -> dict[str, str]:
    """Anciens catalogues Hadoop, montés sous les noms legacy_bronze, etc."""
    conf = {}
    for layer in LAYERS:
        prefix = f"spark.sql.catalog.legacy_{layer}"
        conf[prefix] = "org.apache.iceberg.spark.SparkCatalog"
        conf[f"{prefix}.type"] = "hadoop"
        conf[f"{prefix}.warehouse"] = f"s3a://{layer}/"
    return conf


def current_metadata_file(spark, legacy_table: str) -> str:
    row = spark.sql(
        f"SELECT file FROM {legacy_table}.metadata_log_entries ORDER BY timestamp DESC LIMIT 1"
    ).collect()
    if not row:
        raise RuntimeError(f"Aucun fichier de métadonnées pour {legacy_table}")
    return row[0]["file"]


def main():
    spark = get_spark_session("migrate-catalog-to-jdbc", extra_conf=legacy_conf())
    migrated, skipped = 0, 0

    for layer in LAYERS:
        legacy_ns = f"legacy_{layer}.{NAMESPACE}"
        try:
            tables = [r["tableName"] for r in spark.sql(f"SHOW TABLES IN {legacy_ns}").collect()]
        except Exception:
            print(f"{legacy_ns} : aucun ancien namespace, rien à migrer")
            continue

        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {layer}.{NAMESPACE}")
        existing = {r["tableName"] for r in spark.sql(f"SHOW TABLES IN {layer}.{NAMESPACE}").collect()}

        for table in tables:
            target = f"{layer}.{NAMESPACE}.{table}"
            if table in existing:
                print(f"{target} : déjà dans le catalogue JDBC, ignorée")
                skipped += 1
                continue
            metadata_file = current_metadata_file(spark, f"{legacy_ns}.{table}")
            spark.sql(
                f"CALL {layer}.system.register_table("
                f"table => '{NAMESPACE}.{table}', metadata_file => '{metadata_file}')"
            )
            snapshots = spark.sql(f"SELECT count(*) AS n FROM {target}.snapshots").collect()[0]["n"]
            print(f"{target} : enregistrée ({snapshots} snapshots conservés)")
            migrated += 1

    print(f"\nMigration terminée : {migrated} tables enregistrées, {skipped} ignorées.")
    spark.stop()


if __name__ == "__main__":
    main()
