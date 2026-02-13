# Exercice 6 : Orchestration avec Apache Airflow

## Objectif

L'objectif de cet exercice est de proposer un système **entièrement automatisé** avec **Apache Airflow** pour orchestrer l'ensemble des exercices du projet Big Data NYC Taxi.

Airflow permet de :
- Définir des pipelines de données (DAGs)
- Planifier l'exécution automatique des tâches
- Gérer les dépendances entre les exercices
- Monitorer l'exécution via une interface web

## Architecture

```
                    +------------------+
                    |    Airflow UI    |
                    |  (Port 8080)     |
                    +--------+---------+
                             |
              +--------------+--------------+
              |              |              |
     +--------v---+   +------v-----+   +----v--------+
     |  Scheduler |   |   Worker   |   |  Webserver  |
     +-----+------+   +------+-----+   +-------------+
           |                |
           v                v
     +-----+----------------+-----+
     |       Redis (Broker)       |
     +----------------------------+
     |    PostgreSQL (Metadata)   |
     +----------------------------+
              |
              v
     +--------+--------+--------+
     |        |        |        |
     v        v        v        v
  +-----+  +-----+  +-----+  +-----+
  | Ex1 |->| Ex2 |->| Ex3 |->| Ex5 |
  +-----+  +-----+  +-----+  +-----+
```

## Services Docker

| Service | Port | Description |
|---------|------|-------------|
| `airflow-webserver` | 8080 | Interface web Airflow |
| `airflow-scheduler` | - | Planificateur de tâches |
| `airflow-worker` | - | Exécuteur de tâches (Celery) |
| `airflow-postgres` | - | Base de métadonnées Airflow |
| `airflow-redis` | - | Broker Celery |
| `minio` | 9000/9001 | Stockage S3 (Data Lake) |
| `postgres` | 5432 | Data Warehouse |
| `spark-master` | 8081 | Cluster Spark |

## Structure du DAG

Le DAG `nyc_taxi_bigdata_pipeline` orchestre les exercices dans l'ordre suivant :

```
start
  |
  v
pre_checks (Vérification MinIO + PostgreSQL)
  |
  v
ex01_data_retrieval (Collecte des données)
  |
  v
ex02_data_processing
  |-- branch1: Nettoyage -> MinIO (nyc-cleaned)
  |-- branch2: Ingestion -> PostgreSQL
  |
  v
ex03_sql_tables (Création/Vérification des tables)
  |
  v
ex04_dashboard (Vérification de la configuration)
  |
  v
ex05_ml_training (Entraînement du modèle ML)
  |
  v
end
```

## Utilisation

### Démarrage Rapide

```bash
cd ex06_airflow

# Première utilisation : initialiser Airflow
./run_ex06.sh init

# Démarrages suivants
./run_ex06.sh start
```

### Commandes Disponibles

```bash
./run_ex06.sh start     # Démarre tous les services
./run_ex06.sh stop      # Arrête tous les services
./run_ex06.sh restart   # Redémarre les services
./run_ex06.sh status    # Affiche le statut
./run_ex06.sh logs      # Affiche les logs Airflow
./run_ex06.sh init      # Initialise Airflow (1ère fois)
```

### Accès aux Interfaces

| Interface | URL | Identifiants |
|-----------|-----|--------------|
| **Airflow** | http://localhost:8080 | admin / admin |
| **MinIO** | http://localhost:9001 | minio / minio123 |
| **Spark** | http://localhost:8081 | - |

## Configuration du DAG

### Paramètres

Le DAG est configuré avec les paramètres suivants :

```python
default_args = {
    'owner': 'bigdata-cytech',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

schedule_interval = '@monthly'  # Exécution mensuelle
```

### Variables d'Environnement

Les connexions aux services sont configurées via variables d'environnement :

| Variable | Valeur par défaut |
|----------|-------------------|
| `MINIO_ENDPOINT` | minio:9000 |
| `MINIO_ACCESS_KEY` | minio |
| `MINIO_SECRET_KEY` | minio123 |
| `POSTGRES_HOST` | postgres |
| `POSTGRES_PORT` | 5432 |
| `POSTGRES_USER` | datawarehouse |
| `POSTGRES_PASSWORD` | datawarehouse123 |
| `POSTGRES_DB` | nyc_taxi_dw |

## Exécution des Exercices

### Exécution Manuelle via Airflow

1. Accédez à l'interface Airflow : http://localhost:8080
2. Activez le DAG `nyc_taxi_bigdata_pipeline`
3. Cliquez sur "Trigger DAG" pour lancer l'exécution
4. Suivez l'avancement dans la vue "Graph"

### Exécution Automatique

Le DAG est configuré pour s'exécuter automatiquement :
- **Fréquence** : Mensuelle (`@monthly`)
- **Catch-up** : Désactivé (pas d'exécution rétroactive)

### Exécution Individuelle des Exercices

Les exercices peuvent toujours être exécutés individuellement :

```bash
# Exercice 1
cd ../ex01_data_retrieval && ./run_ex01.sh

# Exercice 2 - Branche 1
cd ../ex02_data_cleaning && ./run_ex02.sh

# Exercice 2 - Branche 2
cd ../ex02_data_ingestion && ./run_ex02_branch2.sh

# Exercice 3
cd ../ex03_sql_table_creation && ./run_ex03.sh

# Exercice 4
cd ../ex04_dashboard && ./run_ex04.sh

# Exercice 5
cd ../ex05_ml_prediction_service && ./run_ex05.sh
```

## Structure des Fichiers

```
ex06_airflow/
├── docker-compose.airflow.yml   # Services Airflow
├── run_ex06.sh                  # Script de lancement
├── ex06-guide.md                # Ce guide
├── README.md                    # Documentation rapide
├── dags/
│   └── nyc_taxi_pipeline.py     # DAG principal
├── logs/                        # Logs Airflow (généré)
├── plugins/                     # Plugins personnalisés
├── config/                      # Configuration
└── scripts/                     # Scripts utilitaires
```

## Dépannage

### Airflow ne démarre pas

```bash
# Vérifier les logs
docker logs airflow-webserver
docker logs airflow-scheduler

# Réinitialiser Airflow
./run_ex06.sh stop
docker volume rm projet_big_data_cytech_25_airflow_postgres_data
./run_ex06.sh init
```

### DAG non visible dans l'interface

```bash
# Vérifier les erreurs de syntaxe dans le DAG
docker exec airflow-webserver python /opt/airflow/dags/nyc_taxi_pipeline.py

# Forcer la détection des DAGs
docker exec airflow-scheduler airflow dags list
```

### Erreur de connexion MinIO/PostgreSQL

```bash
# Vérifier que les services sont en cours
docker ps | grep -E 'minio|postgres'

# Tester la connexion MinIO
curl http://localhost:9000/minio/health/live

# Tester la connexion PostgreSQL
docker exec postgres pg_isready -U datawarehouse -d nyc_taxi_dw
```

## Bonnes Pratiques

1. **Idempotence** : Les tâches sont conçues pour être ré-exécutables sans effet de bord
2. **Vérification** : Chaque exercice inclut une étape de vérification
3. **Logs** : Tous les logs sont centralisés dans Airflow
4. **Monitoring** : L'interface web permet de suivre l'exécution en temps réel

## Prochaines Améliorations

- Alertes par email en cas d'échec
- Intégration avec Prometheus/Grafana pour le monitoring
- Parallélisation des tâches indépendantes
- Gestion des versions des données avec DVC
