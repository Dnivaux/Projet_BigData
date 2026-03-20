# Traceabilite Exigences -> Implementation

## Sujet examen

- Ingestion raw (2 pts): `spark_jobs/feeder.py`
- Traitement silver (4 pts): `spark_jobs/processor.py`
- Logs (1 pt): `logs/*.txt`, logger dans `spark_jobs/common.py`
- Pertinence business (1 pt): recommandation mode H&M (dataset Kaggle)
- Analyse business (1.5 pts): datamarts RFM, top ventes, canal
- Datamarts (4 pts): `spark_jobs/datamart.py`
- API (2 pts): `api/main.py` JWT + pagination
- Visualisation (1.5 pts): `dashboard/app.py` (3 graphiques)
- Architecture modulaire (1 pt): structure du projet
- Video (2 pts): script de demo dans README section 10

## CDC Fashion_HM

- Bronze/Silver/Gold: `raw` / `silver` / `datamart`
- Validation qualite (nulls, dedup, bornes age/prix): `processor.py`
- Jointures transaction/article/client: `processor.py`
- Features temporelles + ranking client: `processor.py`
- Profil client RFM: `datamart.py`
- Dashboard suivi tendances + qualite: `dashboard/app.py`

## Exigences techniques imposees

- Parametrage spark-submit: tous les jobs prennent des arguments CLI
- Pas de chemin hard-code: chemins en arguments/env
- Partition date ingestion: `raw` + `silver`
- `persist()` visible Spark UI: `sales_enriched.persist(MEMORY_AND_DISK)`
- Logs texte: `--log-file` pour chaque job
