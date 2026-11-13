FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
RUN pip install --no-cache-dir pyspark==3.5.2

# dbt dans un venv séparé : ses dépendances entrent en conflit avec celles d'Airflow.
# Versions testées le 13/11/2026 avec Trino 445.
RUN python -m venv /opt/dbt_venv && \
    /opt/dbt_venv/bin/pip install --no-cache-dir dbt-core==1.12.7 dbt-trino==1.11.0
