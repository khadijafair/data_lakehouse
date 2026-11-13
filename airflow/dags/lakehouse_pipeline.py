"""
DAG principal du Data Lakehouse : enchaîne ingestion -> bronze -> qualité
-> silver -> qualité -> gold -> qualité, avec dépendances explicites
et échec visible en cas de problème (aucune erreur n'est avaler silencieusement).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/project/spark"
DBT_PROJECT = "/opt/project/dbt"
DBT = "/opt/dbt_venv/bin/dbt"  # dbt installé dans un venv séparé (voir docker/airflow/Dockerfile)

default_args = {
    "owner": "data-lakehouse",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="lakehouse_pipeline",
    description="Pipeline Bronze -> Silver -> Gold du projet e-commerce",
    default_args=default_args,
    schedule=None,  # déclenchement manuel pour l'instant ; @daily plus tard si besoin
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse"],
) as dag:

    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=f"cd {PROJECT}/ingestion && python ingest_sources.py",
    )

    bronze_quality_check = BashOperator(
        task_id="bronze_quality_check",
        bash_command=f"cd {PROJECT}/quality && python check_bronze.py",
    )

    transform_silver = BashOperator(
        task_id="transform_silver",
        bash_command=f"cd {PROJECT}/silver && python silver_jobs.py",
    )

    silver_quality_check = BashOperator(
        task_id="silver_quality_check",
        bash_command=f"cd {PROJECT}/quality && python check_silver.py",
    )

    # Couche Gold en SQL avec dbt, exécutée par Trino sur les tables Iceberg Silver.
    # `dbt build` = modèles + tests : un test en échec arrête le DAG ici.
    dbt_build_gold = BashOperator(
        task_id="dbt_build_gold",
        bash_command=(
            # La partie clickstream (entonnoir) est construite par le DAG clickstream_pipeline,
            # pour que ce DAG marche aussi sans Kafka.
            f"{DBT} build --exclude source:silver.clickstream+ "
            f"--project-dir {DBT_PROJECT} --profiles-dir {DBT_PROJECT} "
            "--target-path /tmp/dbt_target --log-path /tmp/dbt_logs"
        ),
    )

    gold_quality_check = BashOperator(
        task_id="gold_quality_check",
        bash_command=f"cd {PROJECT}/quality && python check_gold.py",
    )

    generate_report = BashOperator(
        task_id="generate_report",
        bash_command=f"cd {PROJECT}/quality && python generate_report.py",
    )

    (
        ingest_bronze
        >> bronze_quality_check
        >> transform_silver
        >> silver_quality_check
        >> dbt_build_gold
        >> gold_quality_check
        >> generate_report
    )
