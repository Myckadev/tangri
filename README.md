## Documentation du projet

### Services

** Streamlit(Batch Layer) **
- Permet 

## 1. Résumé fonctionnel
Application de data processing temps quasi réel + batch :
- Onglet Streamlit "Configuration Villes" : sélection / activation de villes cibles => alimente le Speed Layer (monitoring via Grafana).
- Onglet Streamlit "Requêtes Historiques" : déclenche une requête batch (années passées) orchestrée par Airflow et exécutée par Spark.
- Les résultats temps réel et batch sont exposés via Kafka; Grafana consomme le flux Speed Layer.

## 2. Architecture (Lambda simplifiée)
Speed Layer:
- Ingestion rapide via Streamlit -> Kafka (topic: weather.queries.requests)
- Traitement Spark Streaming (ou micro-batch) -> Kafka
- Visualisation: Grafana (consommation directe sur topic weather.queries.results)

Batch Layer:
- Déclenchement manuel (onglet Streamlit) -> Kafka (topic: requests.batch)
- Airflow consomme requests.batch, lance un job Spark batch (filtres: ville(s), période)
- Spark lit stockage hdfs , calcule agrégations
- Résultats envoyés sur Kafka (topic: results.batch) + option de restitution dans Streamlit

Orchestration:
- Airflow DAG "batch_city_query_dag" écoute le topic requests.batch (consumer dédié ou opérateur sensor)
- Lorsqu'un message valide arrive: lancement tâche SparkSubmitOperator

## 3. Flux de données

### Schéma global
```
┌─────────────────────┐
│      Streamlit      │
│---------------------│
│ - Config villes     │
│ - Requêtes batch    │
└─────────┬──────────┘
          │
          ▼
┌─────────────────────┐
│        Kafka        │
│---------------------│
│ Topic de requête    │
│ (Streamlit → Airflow)│
└─────────┬──────────┘
          │
          ▼
┌─────────────────────┐
│       Airflow       │
│---------------------│
│ - Écoute topic req  │
│ - Déclenche jobs    │
│   • Speed layer     │
│   • Batch layer     │
└─────────┬──────────┘
          │
          ▼
┌─────────────────────┐
│        Spark        │
│---------------------│
│ - Traitement batch  │
│ - Traitement speed  │
│ - Publication résultats │
└─────────┬──────────┘
          │
          ▼
┌─────────────────────┐
│        Kafka        │
│---------------------│
│ Topic de résultats  │
│ (Spark → Grafana/UI) │
└───────┬────────────┘
        │
  ┌─────┴─────┐
  │           │
  ▼           ▼
┌──────────────┐   ┌──────────────┐
│   Grafana    │   │   Streamlit  │
│ - Consomme   │   │ - Affiche    │
│   topic rés  │   │   résultats  │
└──────────────┘   └───────────────┘
```
