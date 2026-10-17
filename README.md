# Data Lakehouse — Projet E-commerce

Architecture Lakehouse locale (Bronze → Silver → Gold) construite avec MinIO, Apache Spark, Apache Iceberg et Apache Airflow, sur un jeu de données e-commerce synthétique volontairement sale (25-30% d'anomalies).

## Sommaire

- [Problématique](#problématique)
- [Architecture](#architecture)
- [Technologies et choix techniques](#technologies-et-choix-techniques)
- [Démarrage](#démarrage)
- [Structure du projet](#structure-du-projet)
- [Pipeline de données](#pipeline-de-données)
- [Qualité des données](#qualité-des-données)
- [Robustesse Iceberg](#robustesse-iceberg)
- [Orchestration](#orchestration)
- [CI/CD](#cicd)
- [Dashboard](#Power-BI-Export-CSV-des-tables-Gold)
- [Résultats](#résultats)
- [Limites connues et pistes non retenues](#limites-connues-et-pistes-non-retenues)
- [Extensions possibles](#extensions-possibles)

## Problématique

Comment construire une architecture locale capable de transformer des données brutes hétérogènes en données fiables, structurées et exploitables pour l'analyse, tout en conservant la traçabilité des données originales ?

## Architecture
Sources CSV (customers, products, orders)
↓
Airflow (orchestration)
↓
MinIO (stockage S3 local)
↓
┌─────────┬─────────┬─────────┐
│ Bronze │ Silver │ Gold │
│ (brut) │(nettoyé)│(business)│
└─────────┴─────────┴─────────┘
↓
Analyse / Dashboard

Chaque couche est un espace de noms Iceberg distinct (`bronze.ecommerce`, `silver.ecommerce`, `gold.ecommerce`), stocké dans son propre bucket MinIO (`bronze/`, `silver/`, `gold/`).

## Technologies et choix techniques

| Composant | Choix | Pourquoi |
|---|---|---|
| Stockage objet | MinIO | Compatible API S3, reproduit un vrai environnement cloud en local |
| Table format | **Apache Iceberg** (vs Delta Lake) | Standard ouvert indépendant d'un fournisseur cloud, largement adopté (Snowflake, Trino...) |
| Traitement | Apache Spark 3.5 (PySpark) | Logique de nettoyage riche (UDFs) nécessaire sur ce dataset très sale |
| Orchestration | Apache Airflow 2.9 (Docker) | Standard du marché pour l'orchestration de pipelines batch |
| Qualité des données | Règles manuelles (Bronze/Silver/Gold) + **Great Expectations** en complément | Grande Expectations formalise et documente visuellement les règles déjà implémentées |
| CI/CD | GitHub Actions (lint, tests, build Docker) | Vérifie automatiquement la logique de nettoyage et la buildabilité de l'infra à chaque push |

**dbt a été évalué et volontairement abandonné** — voir [Limites connues](#limites-connues-et-pistes-non-retenues).

## Démarrage

### Prérequis
- Docker + Docker Compose
- Python 3.11, venv
- Java 17 (JDK)

### 1. Lancer l'infrastructure

```powershell
cd docker
docker compose up -d --build
```

Crée un fichier `docker/.env` (non versionné) avec :
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=<ton_mot_de_passe>


Vérifie que MinIO (http://localhost:9000) et Airflow (http://localhost:8080) répondent.

### 2. Préparer l'environnement Python

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Placer les données sources

Déposer les CSV du dataset dans `data/sample/` :
- `crm_50000_customers_dirty_v3.csv`
- `product_catalog_dirty_30pct.csv`
- `orders_300k_dirty.csv`

### 4. Lancer le pipeline

**Via Airflow** (recommandé, orchestré) : activer et déclencher le DAG `lakehouse_pipeline` depuis http://localhost:8080

**Manuellement** (debug) :
```powershell
cd spark\ingestion && python ingest_sources.py
cd ..\silver && python silver_jobs.py
cd ..\gold && python gold_jobs.py
```

## Structure du projet
data-lakehouse/
├── docker/ # docker-compose, Dockerfile Airflow
├── airflow/dags/ # DAG d’orchestration
├── spark/
│ ├── common/ # config Spark/Iceberg, fonctions de normalisation, scripts de démo
│ ├── ingestion/ # Bronze
│ ├── silver/ # nettoyage, validation, jointure
│ ├── gold/ # agrégats business
│ └── quality/ # contrôles qualité (scripts + Great Expectations)
├── data/sample/gold_export # CSV sources 
├── tests/ # tests unitaires (pytest)
├── docs/ # documentation, schémas
├── .github/workflows/ # CI/CD
└── requirements.txt


## Pipeline de données

### Bronze
Ingestion brute, typage `string` uniquement (évite toute perte silencieuse de valeurs sales), ajout de métadonnées (`_ingested_at`, `_source_file`, `_batch_id`).

### Silver
- **customers** : déduplication sur `customer_id` (garde la ligne la plus récente), normalisation des chaînes, typage des dates
- **products** : normalisation de `category` (8 catégories canoniques), cast `price`
- **orders** : normalisation `status`/`payment_method`/`quantity` par tables de correspondance, parsing `order_date` (détection des formats corrompus)
- **orders_enriched** : jointure `orders + customers + products`, partitionnée par mois

### Gold
4 tables business, filtrées sur `status = 'success'` uniquement (les commandes échouées/remboursées n'ont jamais généré de revenu réel) :
- `sales_daily`, `sales_by_product`, `sales_by_region`, `customer_metrics`

## Qualité des données

Deux bugs réels trouvés et corrigés pendant le développement :

1. **`NaN` invisible** : caster la chaîne `"NaN"` en `double` produit un flottant `NaN`, pas `NULL` — indétectable par `isNull()`. Corrigé avec une vérification explicite `~isnan(...)`.
2. **Non-reproductibilité des dates** : la règle de rejet comparait `order_date > current_date()`, donnant un résultat différent selon le jour d'exécution. Corrigé en détectant la corruption par sa forme (présence d'un timestamp complet) plutôt que par comparaison à l'horloge système.

Les contrôles qualité existent à deux niveaux : scripts `check_bronze/silver/gold.py` (bloquants dans Airflow) et **Great Expectations** (suites de validation documentées, rapport HTML).

## Robustesse Iceberg

Trois capacités démontrées avec des preuves chiffrées (scripts dans `spark/common/demo_*.py`) :

- **Partitionnement** : `orders` partitionné par mois — confirmé par la table système `.partitions` et le plan d'exécution (pruning de partition)
- **Time travel** : comparaison de deux versions réelles de `sales_daily`, révélant un écart de 821M€ entre la version initiale (bug NaN non corrigé) et la version actuelle
- **Schema evolution** : ajout d'une colonne à `customers` sans réécriture — confirmé par un nombre de fichiers de données strictement identique avant/après (8 = 8)

## Orchestration

DAG Airflow unique (`lakehouse_pipeline`) :


Chaque étape bloque la suivante en cas d'échec. Retries configurés (1 tentative, 2 min de délai) pour les échecs transitoires.

## CI/CD

GitHub Actions, déclenché à chaque push sur `main` :
- **unit-tests** : lint (ruff) → tests unitaires (pytest, fonctions de normalisation testées indépendamment de Spark) → couverture
- **docker-build-check** : vérifie que l'image Airflow personnalisée reste buildable

Les fonctions de nettoyage (`normalize_category`, `normalize_quantity`, `normalize_status`, `normalize_payment`) sont isolées dans `spark/common/normalizers.py`, sans dépendance Spark, pour permettre un CI rapide (quelques secondes, sans installer PySpark).

## Résultats

| Étape | Lignes |
|---|---|
| Sources brutes | 350 500 (customers + products + orders) |
| Bronze | idem (aucune perte, traçabilité totale) |
| Silver (orders_enriched) | 118 909 |
| Gold (commandes success) | 87 627 |

CA total calculé : ~13,4 milliards (données synthétiques, échelle arbitraire du dataset).

## Limites connues et pistes non retenues

**dbt** a été testé comme alternative SQL pour la couche Gold. La connexion a fonctionné, mais l'adaptateur `dbt-spark` en mode `session` ne supporte qu'un seul catalogue Spark par défaut — incompatible avec l'architecture à 3 catalogues Iceberg distincts (un par bucket bronze/silver/gold) mise en place dès le début du projet. Une migration vers un catalogue unique avec des namespaces aurait résolu le problème, mais au prix d'une refonte touchant une dizaine de fichiers déjà testés et de la perte de l'historique de time travel démontré. Le choix a été fait de ne pas migrer, dbt n'étant pas une exigence du cahier des charges (listé en extension optionnelle).

**Doublons d'identité** (même email, `customer_id` différent) : détectés dans `customers` mais volontairement non fusionnés automatiquement (risque de faux positifs) — relève de l'entity resolution/MDM, hors scope du MVP.

**Régions synthétiques** : `sales_by_region` reflète des données générées aléatoirement, sans signification économique réelle.

## Extensions possibles


- Intégration Support Tickets + Clickstream (sources non utilisées dans le MVP)
- Prédiction (churn, recommandation)
- Entity resolution sur les doublons clients identifiés