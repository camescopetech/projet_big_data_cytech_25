# Exercice 6 : Orchestration Airflow

## Présentation

Ce module ajoute **Apache Airflow** pour automatiser l'ensemble des exercices du projet Big Data NYC Taxi sous forme de pipeline.

## Démarrage Rapide

```bash
# Première utilisation
./run_ex06.sh init

# Démarrages suivants
./run_ex06.sh start
```

## Accès

| Service | URL | Identifiants |
|---------|-----|--------------|
| **Airflow** | http://localhost:8080 | admin / admin |
| **MinIO** | http://localhost:9001 | minio / minio123 |
| **Spark** | http://localhost:8081 | - |

## Commandes

```bash
./run_ex06.sh start     # Démarrer
./run_ex06.sh stop      # Arrêter
./run_ex06.sh status    # Statut
./run_ex06.sh logs      # Logs
```

## DAG

Le DAG `nyc_taxi_bigdata_pipeline` orchestre :

```
Ex1 (Collecte) -> Ex2 (Nettoyage/Ingestion) -> Ex3 (SQL) -> Ex4 (Dashboard) -> Ex5 (ML)
```

## Documentation

Voir le guide complet : [ex06-guide.md](./ex06-guide.md)

## Structure

```
 ex06_airflow/
  ├── docker-compose.airflow.yml   # Services Airflow (webserver,
  scheduler, worker, redis, postgres)
  ├── run_ex06.sh                  # Script d'exécution
  ├── README.md                    # Documentation rapide
  ├── ex06-guide.md                # Guide complet
  ├── dags/
  │   └── nyc_taxi_pipeline.py     # DAG principal orchestrant tous les
  exercices
  ├── logs/                        # Logs Airflow
  ├── plugins/                     # Plugins personnalisés
  ├── config/                      # Configuration
  └── scripts/                     # Scripts utilitaires
```
