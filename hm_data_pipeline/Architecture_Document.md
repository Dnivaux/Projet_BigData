# Architecture Document - Data Engineering Platform

## Architecture Globale (Médaillon)

Le projet implémente une architecture Data Lake au format Médaillon :
1. **Couche Raw** : Données sources importées dans HDFS après conversion en Parquet.
2. **Couche Silver** : Données nettoyées, jointes, et enrichies.
3. **Couche Datamart** : Agrégations servies via PostgreSQL et exposées via FastAPI.

## Choix Techniques

### 1. Ingestion `feeder.py`
Les datasets (Articles, Customers, Transactions) ont été pré-convertis en Parquet en amont pour éviter les traitements lourds CSV pendant les jobs Spark. Lors de l'ingestion, un partitionnement natif via `partitionBy("year", "month", "day")` est appliqué. L'année, mois et jour sont déduits de la date d'ingestion courante.

### 2. Traitement `processor.py`
Cinq règles de validation sont appliquées (comme filtrer les prix > 0 ou l'âge).
Une jointure est effectuée entre les trois entités. 
Avant les agrégations complexes, nous utilisons explicitement `.cache()` sur le DataFrame afin d'optimiser le plan d'exécution et réduire le temps de calcul.
Une Window Function `rank().over(partitionBy("age_group").orderBy(desc("purchase_count")))` est utilisée pour déduire le mode de consommation et les meilleurs articles pour chaque groupe d'âge.

### 3. Service `datamart.py`
Pour permettre l'accès rapide aux données par une API et le Dashboard, nous exportons la couche Silver finale (top articles par groupe d'âge) vers une base **PostgreSQL**.

### 4. API (FastAPI) & Dashboard (Streamlit)
L'API est construite avec **FastAPI** pour offrir une documentation interactive (Swagger). L'endpoint est protégé via JWT et utilise SqlAlchemy (avec paramètres `skip` et `limit`) pour répondre au **besoin obligatoire de pagination**.
**Streamlit** gère la visualisation grâce à `plotly` avec 3 graphiques (BarChart, PieChart, et Scatter Plot multicritères) et s'interface localement avec l'API grâce au Token JWT.

## Démonstration
Pour effectuer la vidéo, suivez précisément les étapes définies dans le fichier `README.md`.
Vous pourrez y présenter chaque UI, l'exécution spark-submit, HDFS, le Datamart et finalement l'API puis le rendu visuel.
