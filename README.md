# Data Lakehouse — Projet E-commerce

Architecture Lakehouse locale (Bronze → Silver → Gold) construite avec MinIO, Apache Spark, Apache Iceberg, Trino, dbt, Apache Kafka et Apache Airflow, sur un jeu de données e-commerce synthétique volontairement sale (43,6 % des lignes sources écartées en Silver, motif de rejet tracé ligne par ligne).

## Sommaire

- [Problématique](#problématique)
- [Architecture](#architecture)
- [Technologies et choix techniques](#technologies-et-choix-techniques)
- [Démarrage](#démarrage)
- [Structure du projet](#structure-du-projet)
- [Pipeline de données](#pipeline-de-données)
- [Flux temps réel : clickstream avec Kafka](#flux-temps-réel--clickstream-avec-kafka)
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
┌──────────────┬───────────────┬──────────────────┐
│   Bronze     │    Silver     │      Gold        │
│   (brut)     │  (nettoyé)    │   (business)     │
│   Spark      │    Spark      │  dbt sur Trino   │
└──────────────┴───────────────┴──────────────────┘
↓
Analyse / Dashboard (Power BI, ou SQL direct via Trino)

Flux temps réel (optionnel, profil Docker `streaming`) :
clickstream CSV → producteur Python → **Kafka** → **Spark Structured Streaming** → Bronze → Silver → dbt (`product_funnel`)

Chaque couche est un espace de noms Iceberg distinct (`bronze.ecommerce`, `silver.ecommerce`, `gold.ecommerce`), stocké dans son propre bucket MinIO (`bronze/`, `silver/`, `gold/`).

**Catalogue Iceberg partagé (JDBC / PostgreSQL)** : la liste des tables et le pointeur vers leurs métadonnées sont rangés dans la base `iceberg` du PostgreSQL. Spark (Bronze, Silver) et Trino (Gold via dbt) lisent et écrivent donc **le même catalogue**, sans service supplémentaire.

## Technologies et choix techniques

| Composant | Choix | Pourquoi |
|---|---|---|
| Stockage objet | MinIO | Compatible API S3, reproduit un vrai environnement cloud en local |
| Table format | **Apache Iceberg** (vs Delta Lake) | Standard ouvert indépendant d'un fournisseur cloud, largement adopté (Snowflake, Trino...) |
| Traitement | Apache Spark 3.5 (PySpark) | Logique de nettoyage riche (UDFs) nécessaire sur ce dataset très sale |
| Catalogue Iceberg | JDBC (PostgreSQL) | Lu par Spark **et** par Trino ; Trino ne sait pas lire un catalogue « hadoop » (types acceptés : hive_metastore, glue, jdbc, rest, nessie, snowflake) |
| Moteur SQL | **Trino** 445 | Requêtes SQL interactives sur les tables Iceberg, moteur d'exécution de dbt |
| Streaming | **Apache Kafka** 3.9 (KRaft) + **Spark Structured Streaming** | Ingestion continue des événements de navigation, reprise exacte grâce au checkpoint |
| Couche Gold | **dbt** (dbt-trino) | Modèles SQL versionnés, tests de données (33), documentation et lignage générés |
| Orchestration | Apache Airflow 2.9 (Docker) | Standard du marché pour l'orchestration de pipelines batch |
| Qualité des données | Règles manuelles (Bronze/Silver/Gold) + **Great Expectations** en complément | Grande Expectations formalise et documente visuellement les règles déjà implémentées |
| CI/CD | GitHub Actions (lint, tests, build Docker) | Vérifie automatiquement la logique de nettoyage et la buildabilité de l'infra à chaque push |

**dbt** avait d'abord été abandonné (dbt-spark ne gérait pas les 3 catalogues) ; il est revenu avec Trino — voir [Couche Gold avec dbt et Trino](#gold).

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


Vérifie que MinIO (http://localhost:9000), Airflow (http://localhost:8080) et Trino (http://localhost:8085) répondent.

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
cd ..\..\dbt && dbt build --profiles-dir .
```

**Requêter en SQL** : `docker exec -it lakehouse-trino trino`, puis par exemple `SELECT * FROM gold.ecommerce.sales_daily ORDER BY date DESC LIMIT 10;`

**Mise à jour d'une installation existante (anciennes tables en catalogue « hadoop »)** : après `docker compose up -d --build`, lancer une seule fois
```powershell
cd spark\common && python migrate_catalog_to_jdbc.py
```
Les tables sont enregistrées dans le nouveau catalogue sans être réécrites (historique de time travel conservé). Sans danger si relancé.

## Structure du projet
data-lakehouse/
├── docker/ # docker-compose, Dockerfile Airflow
├── airflow/dags/ # DAG d’orchestration
├── dbt/ # couche Gold : modèles SQL, tests, macros (dbt-trino)
├── trino/catalog/ # catalogues Trino bronze/silver/gold (Iceberg JDBC)
├── streaming/producer/ # producteur Kafka : rejeu du clickstream (simulation de temps réel)
├── spark/
│ ├── common/ # config Spark/Iceberg, fonctions de normalisation, scripts de démo
│ ├── ingestion/ # Bronze
│ ├── streaming/ # Kafka -> Bronze (Spark Structured Streaming)
│ ├── silver/ # nettoyage, validation, jointure (+ silver_clickstream.py)
│ ├── gold/ # ancienne couche Gold en PySpark (référence, hors DAG)
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

Construites par **dbt sur Trino** (`dbt/models/gold/`), à partir des tables Silver écrites par Spark :
- `stg_successful_orders` (éphémère) : base commune, commandes `success` et revenu de ligne ;
- 4 modèles `table` en `CREATE OR REPLACE` : la table Iceberg est conservée et chaque exécution ajoute un snapshot, donc le time travel continue de fonctionner ;
- macro `round_half_up` : arrondi calculé en décimal. Avec le `round()` de Trino sur des doubles, 370 paniers moyens différaient d'un centime de l'ancienne version Spark ; avec la macro, **0 valeur différente sur les 4 tables** (comparaison ligne à ligne du 13/11/2026).

**Tests dbt (33, lancés par `dbt build` à chaque exécution)** : clés non nulles et uniques, intégrité référentielle (`customer_metrics` → `silver.customers`, `orders_enriched` → `silver.customers`), montants positifs et non NaN (tests génériques `positive` / `non_negative`), et réconciliation : les 4 tables Gold doivent donner le même CA total.

Un avertissement (non bloquant) signale **4 286 commandes « success » à montant 0** : 12 988 commandes sources ont `0` comme montant, accepté par la règle Silver `order_amount >= 0`. À trancher : les rejeter en Silver ou les garder.

## Flux temps réel : clickstream avec Kafka

**Pourquoi** : les clients, produits et commandes arrivent en lots (batch). Les événements de navigation arrivent, eux, en continu. Les deux se rejoignent dans la couche Gold : l'entonnoir `product_funnel` (vues → ajouts au panier → commandes) n'est calculable qu'en croisant les deux sources (lien par `product_id`, présent dans l'URL de 356 366 événements).

**Simulation, à dire clairement** : `streaming/producer/replay_clickstream.py` rejoue `clickstream_500k_events.csv` (Kaggle) dans le topic `clickstream`, au débit choisi (`--rate`, 2 000 événements/s par défaut). Les valeurs sont envoyées telles quelles, sales comprises.

**Bronze** (`spark/streaming/stream_clickstream_bronze.py`) : Spark Structured Streaming lit Kafka par micro-lots (100 000 messages max) et les ajoute à `bronze.ecommerce.clickstream` (Iceberg). Tout reste en texte, avec partition, offset et horodatage Kafka. Le checkpoint (dans MinIO) garde les offsets traités : une relance reprend exactement où elle s'était arrêtée (vérifié : 2e passage = +0 ligne ; 5 000 messages republiés = +5 000 lignes). Deux modes : `--mode available-now` (traite ce qui est en attente puis s'arrête, utilisé par Airflow) et `--mode continuous` (micro-lot toutes les 10 s).

**Silver** (`spark/silver/silver_clickstream.py`), motifs de rejet tracés par ligne :
- dédoublonnage sur `event_id` (un message rejoué n'est compté qu'une fois) ;
- 4 écritures valides de l'horodatage acceptées (ISO avec ou sans microsecondes et fuseau, espace au lieu du « T ») ;
- rejetés : 20 160 horodatages vides, 20 011 × `2024/31/01 25:61:00` (25 h 61), 19 919 dates postérieures à l'extraction du jeu de données (2025-12-02 16:05 UTC). La date de référence est **fixe** et non l'horloge du jour : le résultat est reproductible ;
- `product_id` extrait de l'URL ; `customer_id` vide = visiteur anonyme (gardé).

Résultat (13/11/2026) : 505 000 messages Kafka → 500 000 après dédoublonnage → **439 910 valides**, 60 090 rejetés. Période réelle couverte : 2025-09-02 → 2025-12-02.

**Gold** (`dbt/models/gold/product_funnel.sql`) : par produit, sur la période commune aux deux sources (2025-09-03 → 2025-12-01, variables dbt) : 201 481 vues → 50 054 ajouts au panier (24,8 %) → 10 950 commandes (21,9 % des paniers, 5,4 % des vues). Tests : clés, taux entre 0 et 1, intégrité vers `silver.products`, et totaux identiques aux événements Silver (`assert_funnel_matches_silver`).

Limites (données synthétiques) : `session_id` regroupe ~43 clients différents par session, donc pas d'analyse par session ; les taux sont presque identiques d'une catégorie à l'autre ; ils illustrent la mécanique, pas un vrai comportement d'achat.

**Lancer** :
```powershell
cd docker
docker compose --profile streaming up -d --build        # + Kafka et le producteur (environ 1 Go de RAM en plus)
```
Puis, dans Airflow : DAG `lakehouse_pipeline` (au moins une fois, pour les commandes), puis DAG `clickstream_pipeline`.
Mode continu, pour voir le flux arriver : `docker exec -it lakehouse-airflow-scheduler bash -c "cd /opt/project/spark/streaming && python stream_clickstream_bronze.py --mode continuous"`.
Sans le profil `streaming`, le reste du projet fonctionne normalement (le DAG principal exclut la partie clickstream de dbt).

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

```
ingest_bronze → bronze_quality_check → transform_silver → silver_quality_check
→ dbt_build_gold → gold_quality_check → generate_report
```

DAG du flux temps réel (`clickstream_pipeline`, nécessite le profil `streaming`) :

```
ingest_clickstream_bronze → transform_silver_clickstream → dbt_build_funnel
```

`dbt_build_gold` lance `dbt build` (modèles + tests) : un test dbt en échec arrête le DAG. dbt est installé dans un venv séparé de l'image Airflow (`/opt/dbt_venv`) pour éviter les conflits de dépendances.

Chaque étape bloque la suivante en cas d'échec. Retries configurés (1 tentative, 2 min de délai) pour les échecs transitoires.

## CI/CD

GitHub Actions, déclenché à chaque push sur `main` :
- **unit-tests** : lint (ruff) → vérification du projet dbt (`dbt parse`) → tests unitaires (pytest : fonctions de normalisation sans Spark, motifs de rejet Silver sur une SparkSession locale, structure des DAG, alignement des catalogues Spark/Trino, producteur Kafka et règles du clickstream) → couverture
- **docker-build-check** : vérifie que l'image Airflow personnalisée reste buildable

Les fonctions de nettoyage (`normalize_category`, `normalize_quantity`, `normalize_status`, `normalize_payment`) sont isolées dans `spark/common/normalizers.py`, sans dépendance Spark, pour des tests rapides. Les règles de rejet Silver (`spark/common/rejection_rules.py`) sont testées sur une SparkSession locale, sans Iceberg ni MinIO.

## Résultats

| Étape | Lignes |
|---|---|
| Sources brutes | 350 500 (customers + products + orders) |
| Bronze | idem (aucune perte, traçabilité totale) |
| Silver (orders_enriched) | 118 909 |
| Gold (commandes success) | 87 627 |

CA total calculé : ~13,4 milliards (données synthétiques, échelle arbitraire du dataset).

Chiffres remesurés le 13/11/2026 sur les CSV Kaggle `vinaykandimalla/customer-360` : identiques au run du 17/10, et les 4 tables Gold sont identiques valeur par valeur aux exports de `data/gold_export/`. Pipeline complet relancé le même jour avec la Gold en dbt sur Trino (Iceberg réel, catalogue JDBC) : mêmes chiffres, 0 valeur différente.

### Lignes écartées en Silver, par motif

Chaque ligne rejetée porte la liste exacte des champs fautifs dans `_rejection_reason` (une ligne peut cumuler plusieurs motifs).

| Table | Sources | Conservées | Écartées | Motifs |
|---|---|---|---|---|
| customers | 50 000 | 47 200 | 2 800 | 1 800 doublons de `customer_id`, 1 000 emails manquants |
| products | 500 | 405 | 95 | prix invalide 82, catégorie inconnue 30 |
| orders | 300 000 | 150 137 | 149 863 | date 90 000, quantité 44 867, montant 38 537, moyen de paiement 10 586 |
| **Total** | **350 500** | **197 742** | **152 758 (43,6 %)** | |

Puis 31 228 commandes valides sont isolées comme orphelines (client ou produit absent de Silver) : `orders_enriched` = 118 909.

Pourquoi 90 000 dates sont rejetées : seul le format `yyyy-MM-dd` est accepté. Les autres formats ne contiennent chacun qu'**une seule valeur répétée** (`2024/31/01` ×18 110, `31-12-2023` ×18 050, `2025/01/10 12:00` ×17 952), à quoi s'ajoutent 17 974 horodatages ISO dans le futur et 17 914 dates vides. Ce sont des valeurs sentinelles : les convertir créerait trois pics artificiels dans `sales_daily`.

## Limites connues et pistes non retenues

**dbt-spark (abandonné), puis dbt-trino (retenu)** : l'adaptateur `dbt-spark` en mode `session` ne gère qu'un seul catalogue Spark, incompatible avec les 3 catalogues Iceberg. Trino lit plusieurs catalogues, mais pas les catalogues Iceberg de type « hadoop » utilisés au départ. Solution retenue : passer les 3 catalogues en **JDBC (PostgreSQL)**, lisibles par Spark et Trino. Les tables existantes ont été migrées avec `spark/common/migrate_catalog_to_jdbc.py` (procédure Iceberg `register_table`) : aucune donnée réécrite, historique de time travel conservé.

**Doublons d'identité** (même email, `customer_id` différent) : détectés dans `customers` mais volontairement non fusionnés automatiquement (risque de faux positifs) — relève de l'entity resolution/MDM, hors scope du MVP.

**Régions synthétiques** : `sales_by_region` reflète des données générées aléatoirement, sans signification économique réelle.

## Extensions possibles


- Intégration des Support Tickets (source non utilisée)
- Prédiction (churn, recommandation)
- Entity resolution sur les doublons clients identifiés