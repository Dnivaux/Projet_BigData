# H&M Data Engineering Pipeline

Bienvenue dans l'architecture médaillon complète de ce projet data.

## Prérequis
- Docker et Docker Compose installés.
- Les datasets doivent être en format Parquet dans `Dataset_Parquet` (fait via `convert_to_parquet.py`).
- Le fichier `postgresql-42.2.18.jar` est préparé dans `hm_data_pipeline/jars`.

## 1. Démarrer l'infrastructure complète
Ouvrez un terminal et exécutez les commandes suivantes :
```bash
cd hm_data_pipeline/docker-hadoop-spark
docker-compose -f docker-compose.yml -f docker-compose.override.yml up -d
```
Attendez 1 à 2 minutes pour que Hadoop, Spark, Hive, et PostgreSQL (ainsi que les conteneurs API et Dashboard) soient entièrement démarrés.

## 2. Ingestion des données vers Raw (`feeder.py`)
Dans un terminal, exécutez :
```bash
docker exec -e PYSPARK_PYTHON=python3 -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /scripts/feeder.py --source /data/parquet --destination hdfs://namenode:9000/raw
```

## 3. Transformation des données vers Silver (`processor.py`)
Exécutez ce script qui va nettoyer (5 règles), joindre, cacher, windowing et écrire en Silver.
```bash
docker exec -e PYSPARK_PYTHON=python3 -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /scripts/processor.py --source hdfs://namenode:9000/raw --destination hdfs://namenode:9000/silver
```

## 4. Création des Datamarts (`datamart.py`)
Ce script exporte le datamart vers PostgreSQL via JDBC.
```bash
docker exec -e PYSPARK_PYTHON=python3 -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --jars /jars/postgresql-42.2.18.jar /scripts/datamart.py --source hdfs://namenode:9000/silver --db-url jdbc:postgresql://datamart-db:5432/datamarts --db-user admin --db-password admin
```

## 5. API Backend (FastAPI)
L'API est accessible publiquement sur http://localhost:8000.
Documentation: http://localhost:8000/docs (Swagger).
Authentification requise pour l'endpoint `/datamart/top_articles` ; pagination supportée (`?skip=0&limit=100`).

## 6. Visualisation (Streamlit)
Le dashboard est accessible sur http://localhost:8501.
Connectez-vous via le menu gauche avec `admin` / `admin`. Vous visionnerez les données provenant de l'API avec les graphiques.

## Informations pour la vidéo
- **Spark UI** : http://localhost:8080 (Affichez-le pendant l'exécution des Spark-submit, particulièrement le processor.py pour montrer l'utilisation du Cache et la DAG).
- **HDFS Namenode** : http://localhost:9870 (Affichez le système de fichier /raw et /silver).
- **Resource Manager YARN / Job History** : http://localhost:8088 (Le repo git inclu historyserver et resourcemanager).
