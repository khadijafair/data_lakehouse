"""
Valide la chaîne Spark -> Iceberg -> MinIO en isolation,
sans logique métier. Si ce script passe, on sait que l'infra
est prête pour écrire les vrais jobs Bronze.
"""

from spark_session import get_spark_session

spark = get_spark_session("test-connection")

# Crée un namespace et une table minuscule dans le catalogue bronze
spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze.ecommerce")
spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze.ecommerce.test_table (
        id INT, message STRING
    ) USING iceberg
""")
spark.sql("INSERT INTO bronze.ecommerce.test_table VALUES (1, 'ça fonctionne')")

result = spark.sql("SELECT * FROM bronze.ecommerce.test_table")
result.show()

spark.stop()
