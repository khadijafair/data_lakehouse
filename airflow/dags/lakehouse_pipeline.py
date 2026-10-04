"""
DAG principal du Data Lakehouse : encha\u00eene ingestion -> bronze -> qualit\u00e9
-> silver -> qualit\u00e9 -> gold -> qualit\u00e9, avec d\u00e9pendances explicites
et \u00e9chec visible en cas de probl\u00e8me (aucune erreur n'est avaler silencieusement).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/project/spark"

default_args = {
    "owner": "data-lakehouse",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="lakehouse_pipeline",
    description="Pipeline Bronze -> Silver -> Gold du projet e-commerce",
    default_args=default_args,
    schedule=None,  # d\u00e9clenchement manuel pour l'instant ; @daily plus tard si besoin
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

    transform_gold = BashOperator(
        task_id="transform_gold",
        bash_command=f"cd {PROJECT}/gold && python gold_jobs.py",
    )

    gold_quality_check = BashOperator(
        task_id="gold_quality_check",
        bash_command=f"cd {PROJECT}/quality && python check_gold.py",
    )

    (
        ingest_bronze
        >> bronze_quality_check
        >> transform_silver
        >> silver_quality_check
        >> transform_gold
        >> gold_quality_check
    )