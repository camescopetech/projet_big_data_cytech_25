# Projet Big Data - NYC Taxi Pipeline

Projet par Camille Bezet, Brasa Franklin, Kenmogne Loic, Martin Soares Flavio en ING3 IA A.

Pipeline Big Data complet pour l'analyse des courses de taxis jaunes de New York (juin - août 2025, ~9 millions de trajets).

Le rapport détaillé se trouve dans le fichier `Rapport.md` à la racine du projet.

---

## Prérequis

- **Docker Desktop** en cours d'exécution
- **Java 17** (requis pour Spark 3.5)
- **SBT** (Scala Build Tool) pour les exercices 1, 2
- **Python 3.8+** pour les exercices 4, 5
- **uv** (gestionnaire d'environnement Python) pour l'exercice 5

---

## Lancement de l'infrastructure

Avant de lancer les exercices, démarrer les services de base (Spark, MinIO, PostgreSQL) :

```bash
docker compose up -d
```

**Interfaces disponibles :**

| Service | URL | Identifiants |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | minio / minio123 |
| Spark Master UI | http://localhost:8081 | - |
| PostgreSQL | localhost:5432 | datawarehouse / datawarehouse123 |

---

## Exécution des exercices

Les exercices doivent être exécutés dans l'ordre (chaque exercice dépend des précédents).

### Exercice 1 : Collecte des données

Télécharge les fichiers Parquet depuis NYC Open Data et les stocke dans MinIO (bucket `nyc-raw`).

```bash
cd ex01_data_retrieval
./run_ex01.sh
```

**Prérequis** : Docker + MinIO démarrés, Java 17, SBT

### Exercice 2 : Nettoyage et ingestion

**Branche 1** - Nettoyage des données (11 règles de validation) et stockage dans MinIO (`nyc-cleaned`) :

```bash
cd ex02_data_cleaning
./run_ex02.sh
```

**Branche 2** - Transformation et ingestion dans PostgreSQL (`fact_trips`) :

```bash
cd ex02_data_ingestion
./run_ex02_branch2.sh
```

**Prérequis** : Exercice 1 terminé, exercice 3 terminé (pour la branche 2)

### Exercice 3 : Création du Data Warehouse

Crée les tables du modèle en flocon (snowflake schema) et insère les données de référence dans PostgreSQL.

```bash
cd ex03_sql_table_creation
./run_ex03.sh
```

**Prérequis** : Docker + PostgreSQL démarrés

### Exercice 4 : Dashboard de visualisation

Lance le tableau de bord interactif Streamlit (KPIs, analyses temporelles, géographiques, financières).

```bash
cd ex04_dashboard
./run_ex04.sh
```

Accessible sur http://localhost:8501

**Prérequis** : Exercices 1, 2, 3 terminés, Python 3.8+

### Exercice 5 : Service de prédiction ML

Entraîne un modèle Gradient Boosting pour prédire le tarif des courses, puis lance une application Streamlit de démonstration.

```bash
cd ex05_ml_prediction_service

# Entraînement + application
./run_ex05.sh

# Entraînement seul
./run_ex05.sh --train-only

# Application seule (modèle déjà entraîné)
./run_ex05.sh --app-only

# Tests unitaires
./run_ex05.sh --test
```

Accessible sur http://localhost:8502

**Prérequis** : Données de l'exercice 1 disponibles, `uv` installé

### Exercice 6 : Orchestration avec Airflow

Orchestre l'ensemble du pipeline via Apache Airflow (DAG mensuel).

```bash
cd ex06_airflow

# Première utilisation : initialisation
./run_ex06.sh init

# Démarrer les services
./run_ex06.sh start

# Arrêter les services
./run_ex06.sh stop

# Voir le statut
./run_ex06.sh status

# Voir les logs
./run_ex06.sh logs
```

Interface Airflow accessible sur http://localhost:8080 (identifiants : admin / admin)

**Prérequis** : Docker + Docker Compose

---

## Ordre d'exécution recommandé

```
1. docker compose up -d          # Infrastructure
2. ./ex01_data_retrieval/run_ex01.sh        # Collecte
3. ./ex02_data_cleaning/run_ex02.sh         # Nettoyage
4. ./ex03_sql_table_creation/run_ex03.sh    # Data Warehouse
5. ./ex02_data_ingestion/run_ex02_branch2.sh # Ingestion
6. ./ex04_dashboard/run_ex04.sh             # Dashboard
7. ./ex05_ml_prediction_service/run_ex05.sh # ML
8. ./ex06_airflow/run_ex06.sh init          # Orchestration
```

---

## Modalités de rendu

1. Pull Request vers la branch `master`
2. Dépôt du rapport et du code source zippé dans cours.cyu.fr

Date limite de rendu : 7 février 2026