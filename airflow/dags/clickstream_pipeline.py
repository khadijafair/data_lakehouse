"""
DAG du flux temps réel (clickstream) : Kafka -> Bronze (Spark Structured Streaming)
-> Silver -> Gold (dbt : entonnoir de conversion par produit).

Prérequis : services du profil `streaming` démarrés (Kafka + producteur), et le DAG
`lakehouse_pipeline` exécuté au moins une fois (l'entonnoir utilise les commandes Silver).

La tâche d'ingestion lance le job de streaming en mode « availableNow » : elle lit tout
ce qui est arrivé dans Kafka depuis le dernier passage (offsets gardés dans le
checkpoint), l'écrit dans Iceberg, puis s'arrête. Pour une ingestion en continu,
lancer le même script avec `--mode continuous` (voir README).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/project/spark"
DBT_PROJECT = "/opt/project/dbt"
DBT = "/opt/dbt_venv/bin/dbt"

default_args = {
    "owner": "data-lakehouse",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="clickstream_pipeline",
    description="Clickstream Kafka -> Bronze -> Silver -> entonnoir dbt",
    default_args=default_args,
    schedule=None,  # déclenchement manuel ; un intervalle court (ex. 15 min) est possible
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "streaming"],
) as dag:

    ingest_clickstream_bronze = BashOperator(
        task_id="ingest_clickstream_bronze",
        bash_command=f"cd {PROJECT}/streaming && python stream_clickstream_bronze.py --mode available-now",
    )

    transform_silver_clickstream = BashOperator(
        task_id="transform_silver_clickstream",
        bash_command=f"cd {PROJECT}/silver && python silver_clickstream.py",
    )

    # Modèle product_funnel + ses tests + les tests des sources qu'il lit
    dbt_build_funnel = BashOperator(
        task_id="dbt_build_funnel",
        bash_command=(
            f"{DBT} build --select product_funnel source:silver.clickstream source:silver.products "
            f"--project-dir {DBT_PROJECT} --profiles-dir {DBT_PROJECT} "
            "--target-path /tmp/dbt_target_funnel --log-path /tmp/dbt_logs_funnel"
        ),
    )

    ingest_clickstream_bronze >> transform_silver_clickstream >> dbt_build_funnel
