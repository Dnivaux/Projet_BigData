# Projet Data Engineering - H&M Fashion Medallion

Ce projet implemente le sujet d'examen M1 Data Engineering avec le dataset H&M et une architecture medaillon:

- `raw` : ingestion brute partitionnee par date d'ingestion
- `silver` : nettoyage + validation + enrichissement
- `datamart` : tables metier pour API et visualisation

Il est compatible avec:

1. execution locale Spark
2. stack Docker du professeur: `Marcel-Jan/docker-hadoop-spark`

## 1) Structure

```text
fashion_medallion/
  api/main.py
  dashboard/app.py
  spark_jobs/
    feeder.py
    processor.py
    datamart.py
    common.py
  scripts/
    run_pipeline_local.sh
    run_pipeline_prof_stack.sh
    run_api.sh
    run_dashboard.sh
  logs/
  tests/
  config/.env.example
  requirements.txt
```

## 2) Prerequis

- Python 3.10+
- Java 11+
- Spark 3.5+
- Dataset present dans `../Dataset`

Installation:

```bash
cd fashion_medallion
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 3) Execution locale

```bash
cd fashion_medallion
chmod +x scripts/*.sh
./scripts/run_pipeline_local.sh
```

Sorties:

- `data_lake/raw`
- `data_lake/silver`
- `data_lake/datamart`
- `logs/*.txt`

## 4) Execution sur le repo Docker du prof

### 4.1 Lancer la stack

```bash
git clone https://github.com/Marcel-Jan/docker-hadoop-spark
cd docker-hadoop-spark
docker compose up -d
```

### 4.2 Monter ce projet dans `spark-master`

Ajoute un volume dans ton `docker-compose.yml` (service `spark-master`) vers ton dossier local `fashion_medallion`.

Exemple:

```yaml
services:
  spark-master:
    volumes:
      - /Users/daenivaux/Desktop/EFREI/BIGDATA/pROJET/fashion_medallion:/opt/fashion_medallion
      - /Users/daenivaux/Desktop/EFREI/BIGDATA/pROJET/Dataset:/opt/Dataset
```

### 4.3 Lancer le pipeline Spark sur cluster

```bash
cd /Users/daenivaux/Desktop/EFREI/BIGDATA/pROJET/fashion_medallion
chmod +x scripts/run_pipeline_prof_stack.sh
PROJECT_DIR_IN_CONTAINER=/opt/fashion_medallion DATASET_DIR_IN_CONTAINER=/opt/Dataset ./scripts/run_pipeline_prof_stack.sh
```

## 5) Jobs Spark

### feeder.py

- lit les 3 CSV source
- ecrit en brut dans `raw`
- partitionnement par date d'ingestion `year=YYYY/month=MM/day=DD`
- params via `spark-submit`
- log dans un fichier `.txt`

### processor.py

- lit depuis `raw`
- applique des regles de validation (>= 5):
  - IDs non nuls
  - date transaction valide
  - prix dans `(0,1)`
  - canal de vente dans `{1,2}`
  - deduplication
  - age client borne + imputation mediane
- jointure transactions + articles + clients
- agregations metier
- window function (top 12 recents par client)
- `persist(MEMORY_AND_DISK)`
- ecriture en `silver`

### datamart.py

- lit depuis `silver`
- construit:
  - `dm_customer_rfm`
  - `dm_top_weekly_articles`
  - `dm_sales_channel_monthly`
- ecriture en `datamart`

## 6) API REST securisee JWT

Lancement:

```bash
cd fashion_medallion
source .venv/bin/activate
./scripts/run_api.sh
```

Endpoints:

- `POST /token` : recupere un token JWT
- `GET /datamarts` : liste des datamarts (auth)
- `GET /datamarts/{name}?page=1&page_size=50` : lecture paginee (auth)
- `GET /health`

Auth par defaut:

- username: `admin`
- password: `admin123`

A changer avec les variables d'environnement `API_USERNAME`, `API_PASSWORD`, `API_SECRET_KEY`.

## 7) Visualisation (3 graphiques)

Lancement:

```bash
cd fashion_medallion
source .venv/bin/activate
./scripts/run_dashboard.sh
```

Graphiques:

- bar chart top articles hebdo
- line chart revenus mensuels par canal
- donut chart repartition segments RFM

## 8) Logging

Chaque job ecrit un fichier:

- `logs/feeder.txt`
- `logs/processor.txt`
- `logs/datamart.txt`

avec `INFO` et `ERROR`.

## 9) Mapping rapide avec le sujet

- Ingestion raw: `spark_jobs/feeder.py`
- Silver (validation/jointure/agregation/window/cache): `spark_jobs/processor.py`
- Datamarts: `spark_jobs/datamart.py`
- API JWT + pagination: `api/main.py`
- Visualisation: `dashboard/app.py`
- Architecture modulaire: separation `spark_jobs`, `api`, `dashboard`, `scripts`

## 10) Commandes de demo video (5-8 min)

1. montrer `spark-submit` (`run_pipeline_prof_stack.sh`)
2. montrer `raw` et `silver` dans HDFS
3. montrer Spark UI (`http://localhost:8080` selon stack)
4. montrer Resource Manager (si active)
5. montrer datamarts
6. tester API JWT + pagination
7. ouvrir dashboard

## 11) Limites et extension

Le CDC mentionne MAP@12 et modele ALS/Ranker. Cette version couvre le socle Data Engineering evalue par le sujet d'examen. L'ajout du module ML (ALS + ranking) peut etre fait dans une phase 2 avec un job Spark dedie.
