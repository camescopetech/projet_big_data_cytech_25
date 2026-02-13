# Rapport de Projet Big Data - NYC Taxi

**Projet** : Pipeline Big Data pour l'analyse des courses de taxis jaunes de New York

**Période des données** : Juin - Août 2025 (3 mois)

**Date de rendu** : 14 Février 2026

Projet par Camille Bezet, Kenmogne Loïc, Brasa Franklin et Martin Soares Flavio en ING3 IA A.

---

## Table des matières

1. [Introduction](#introduction)
2. [Architecture globale du projet](#architecture-globale-du-projet)
3. [Infrastructure technique](#infrastructure-technique)
4. [Exercice 1 : Collecte et stockage des données](#exercice-1--collecte-et-stockage-des-données)
5. [Exercice 2 : Nettoyage et ingestion des données](#exercice-2--nettoyage-et-ingestion-des-données)
6. [Exercice 3 : Data Warehouse et modèle dimensionnel](#exercice-3--data-warehouse-et-modèle-dimensionnel)
7. [Exercice 4 : Dashboard de visualisation](#exercice-4--dashboard-de-visualisation)
8. [Exercice 5 : Service de prédiction ML](#exercice-5--service-de-prédiction-ml)
9. [Exercice 6 : Orchestration avec Airflow](#exercice-6--orchestration-avec-airflow)
10. [Conclusion générale](#conclusion-générale)

---

## Introduction

Ce rapport présente la réalisation d'un projet Big Data complet autour des données des taxis jaunes de New York City (NYC TLC - Taxi and Limousine Commission). Le projet couvre l'ensemble du cycle de vie des données, depuis leur collecte brute jusqu'à leur exploitation via un modèle de Machine Learning, en passant par le nettoyage, le stockage dans un Data Warehouse et la visualisation.

Les données utilisées correspondent aux courses de taxis jaunes pour les mois de **juin, juillet et août 2025**, représentant environ **9 millions de trajets**. Chaque enregistrement contient 19 colonnes décrivant les caractéristiques d'une course : horodatage, distance, tarif, mode de paiement, zones de prise en charge et de dépose, etc.

Le projet s'articule autour de **6 exercices** complémentaires, chacun adressant une étape clé d'un pipeline de données Big Data :

| Exercice | Étape | Rôle |
|----------|-------|------|
| Ex. 1 | Collecte | Récupération et stockage des données brutes |
| Ex. 2 | Nettoyage & Ingestion | Validation, nettoyage et chargement |
| Ex. 3 | Modélisation | Création du Data Warehouse |
| Ex. 4 | Visualisation | Dashboard interactif |
| Ex. 5 | Machine Learning | Prédiction des tarifs |
| Ex. 6 | Orchestration | Automatisation du pipeline |

---

## Architecture globale du projet

```
NYC Open Data (Parquet)
        |
        v
  [Ex1] Collecte (Spark/Scala)
        |
        v
  MinIO (bucket: nyc-raw) ── Data Lake
        |
        v
  [Ex2 - Branche 1] Nettoyage (Spark/Scala)
        |
        +──────────────────────────────────┐
        v                                  v
  MinIO (bucket: nyc-cleaned)    [Ex3] PostgreSQL (Data Warehouse)
        |                                  |
        v                                  v
  [Ex5] ML Prediction           [Ex2 - Branche 2] Ingestion fact_trips
        |                                  |
        v                                  v
  Streamlit (port 8502)         [Ex4] Dashboard Streamlit (port 8501)

  [Ex6] Airflow orchestre l'ensemble (port 8080)
```

---

## Infrastructure technique

L'infrastructure repose sur **Docker** et **Docker Compose** pour garantir la reproductibilité de l'environnement. Les services déployés sont les suivants :

| Service | Image / Technologie | Port | Rôle |
|---------|---------------------|------|------|
| Spark Master | Image custom (Bitnami) | 8081 | Moteur de traitement distribué |
| Spark Worker x2 | Image custom (Bitnami) | - | Workers Spark (2 Go RAM, 2 cores chacun) |
| MinIO | `minio/minio` | 9000 / 9001 | Stockage objet S3-compatible (Data Lake) |
| PostgreSQL | `postgres:15-alpine` | 5432 | Data Warehouse relationnel |
| Airflow | Docker Compose dédié | 8080 | Orchestrateur de workflows |

Le fichier `docker-compose.yml` à la racine du projet définit les services principaux (Spark, MinIO, PostgreSQL), tandis qu'un fichier `docker-compose.airflow.yml` dédié gère les composants Airflow (webserver, scheduler, worker, Redis, PostgreSQL metadata).

---

## Exercice 1 : Collecte et stockage des données

### Objectif

Télécharger les fichiers Parquet des courses de taxis jaunes de NYC pour 3 mois (juin, juillet, août 2025) depuis le portail NYC Open Data, les stocker localement puis les uploader vers MinIO (Data Lake).

### Méthode et implémentation

Le programme est écrit en **Scala** et utilise **Apache Spark** pour la lecture et l'écriture des fichiers Parquet. Le processus se déroule en 3 étapes pour chaque mois :

1. **Téléchargement** : Récupération du fichier Parquet depuis l'URL publique NYC Open Data via une requête HTTP, stockage local dans `data/raw/`
2. **Lecture et validation** : Chargement du fichier avec `spark.read.parquet()`, affichage d'un aperçu et du schéma pour vérification
3. **Upload vers MinIO** : Écriture du DataFrame vers le bucket `nyc-raw` de MinIO via le protocole S3A (`s3a://nyc-raw/`)

La configuration Spark est adaptée pour communiquer avec MinIO en utilisant le connecteur Hadoop-AWS avec les paramètres S3A (endpoint, access key, path style access).

### Outils et technologies

| Outil | Version | Usage |
|-------|---------|-------|
| Scala | 2.12 | Langage de programmation |
| Apache Spark | 3.5.0 | Traitement des données |
| Hadoop AWS | 3.3.4 | Connecteur S3A pour MinIO |
| SBT | - | Build tool Scala |
| ScalaTest | - | Tests unitaires (14 tests) |
| Java | 17 (LTS) | Runtime JVM |

### Tests unitaires

14 tests couvrent : l'initialisation de SparkSession, la lecture/écriture Parquet, le schéma NYC Taxi (19 colonnes), la validation des données, la gestion des erreurs et les transformations.

### Schéma des données

Les données brutes contiennent **19 colonnes** : VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Airport_fee.

### Conclusion

L'exercice 1 permet de constituer le socle de données du projet. Environ **3 millions de lignes par mois** sont collectées et stockées dans MinIO, formant ainsi le Data Lake initial. La compatibilité avec Java 17 a nécessité l'ajout d'options JVM spécifiques (`--add-opens`) pour contourner les restrictions du système de modules Java.

---

## Exercice 2 : Nettoyage et ingestion des données

### Objectif

Créer un pipeline Spark à deux branches pour traiter les données brutes :
- **Branche 1** : Nettoyage des données et stockage dans MinIO (bucket `nyc-cleaned`)
- **Branche 2** : Transformation et ingestion dans PostgreSQL (table `fact_trips`)

### Branche 1 : Nettoyage

#### Méthode

Les données brutes sont lues depuis MinIO (`nyc-raw`), validées selon un **contrat de données** basé sur les spécifications NYC TLC, puis les enregistrements invalides sont filtrés. Les données nettoyées sont ensuite écrites dans le bucket `nyc-cleaned`.

#### Règles de validation appliquées

| Règle | Colonne | Condition |
|-------|---------|-----------|
| VendorID valide | VendorID | IN (1, 2) |
| Passagers | passenger_count | Entre 1 et 9 |
| Distance | trip_distance | Entre 0 et 100 miles |
| Tarif positif | fare_amount | >= 0 |
| Montant total positif | total_amount | >= 0 |
| Pourboire positif | tip_amount | >= 0 |
| Péages positifs | tolls_amount | >= 0 |
| Chronologie cohérente | pickup < dropoff | Durée positive |
| Code tarifaire | RatecodeID | Entre 1 et 6 |
| Type de paiement | payment_type | Entre 1 et 6 |
| Durée raisonnable | trip_duration | Entre 1 et 240 minutes |

Le taux de rejet est d'environ **5-6%** des données, ce qui est un taux acceptable garantissant la qualité sans perte excessive d'information.

### Branche 2 : Ingestion vers PostgreSQL

#### Méthode

Les données nettoyées depuis MinIO sont transformées pour correspondre au modèle dimensionnel (snowflake schema) de l'exercice 3, puis insérées dans la table de faits `fact_trips` via JDBC.

Les transformations incluent :
- **Calcul des clés de dimension** : `date_id` au format YYYYMMDD, `time_id` au format HHMM (arrondi à 30 minutes)
- **Renommage des colonnes** : Mapping des noms Parquet vers les noms PostgreSQL (ex: `VendorID` → `vendor_id`, `PULocationID` → `pickup_location_id`)
- **Insertion par batch** : Lots de 10 000 lignes pour optimiser les performances

### Outils et technologies

| Outil | Usage |
|-------|-------|
| Apache Spark | Lecture MinIO, transformations, écriture JDBC |
| Scala | Langage d'implémentation |
| MinIO (S3A) | Source et destination (Branche 1) |
| PostgreSQL (JDBC) | Destination (Branche 2) |
| ScalaTest | Tests unitaires |

### Tests

- **Branche 1** : 14 tests couvrant chaque règle de validation, les valeurs nulles et le pipeline complet
- **Branche 2** : 20 tests couvrant le calcul des clés de dimension, la validation des dates/heures, les transformations DataFrame et les cas limites

### Conclusion

L'exercice 2 constitue le cœur du pipeline ETL. La séparation en deux branches permet d'alimenter à la fois le Data Lake (MinIO) pour les usages analytiques et ML, et le Data Warehouse (PostgreSQL) pour les requêtes SQL et la visualisation. Environ **8,4 millions de lignes** sont insérées dans PostgreSQL pour les 3 mois traités.

---

## Exercice 3 : Data Warehouse et modèle dimensionnel

### Objectif

Concevoir et implémenter un modèle de données multi-dimensionnel (snowflake schema) dans PostgreSQL, optimisé pour les analyses OLAP des données NYC Taxi.

### Méthode : Modèle en flocon (Snowflake Schema)

Le choix du modèle en flocon se justifie par :
1. **Normalisation** : La table `dim_location` référence `dim_borough`, évitant la redondance des noms d'arrondissements
2. **Intégrité référentielle** : Les données de référence NYC TLC sont normalisées
3. **Flexibilité** : Analyses possibles à différents niveaux de granularité (zone, arrondissement)

### Structure du schéma

**Table de faits :**

| Table | Grain | Description |
|-------|-------|-------------|
| `fact_trips` | 1 ligne = 1 course | Mesures : fare_amount, tip_amount, total_amount, trip_distance, trip_duration_minutes, passenger_count |

**Dimensions de niveau 1 :**

| Table | Cardinalité | Description |
|-------|-------------|-------------|
| `dim_vendor` | 2 | Fournisseurs TPEP (CMT, VeriFone) |
| `dim_rate_code` | 6 | Codes tarifaires (Standard, JFK, Newark...) |
| `dim_payment_type` | 6 | Types de paiement (CB, Cash, No charge...) |
| `dim_location` | ~265 | Zones de taxi NYC |
| `dim_date` | 365 | Calendrier complet 2025 |
| `dim_time` | 48 | Créneaux de 30 minutes |

**Dimension de niveau 2 (flocon) :**

| Table | Cardinalité | Description |
|-------|-------------|-------------|
| `dim_borough` | 7 | Arrondissements NYC |

### Implémentation

Deux fichiers SQL :
- `creation.sql` : Création des 8 tables avec contraintes (PRIMARY KEY, FOREIGN KEY, NOT NULL)
- `insertion.sql` : Insertion des données de référence (vendor, rate_code, payment_type, location, borough, date, time)

### Outils et technologies

| Outil | Usage |
|-------|-------|
| PostgreSQL 15 | SGBD relationnel |
| SQL (DDL/DML) | Création et insertion |
| Docker | Conteneurisation du service |
| Script bash | Automatisation de l'exécution |

### Conclusion

Le modèle snowflake schema fournit une base solide pour les analyses multidimensionnelles. Les tables de dimensions pré-remplies avec les données de référence NYC TLC garantissent l'intégrité des jointures. Le calendrier complet de l'année 2025 est généré automatiquement, incluant les noms de jours et les périodes.

---

## Exercice 4 : Dashboard de visualisation

### Objectif

Créer un tableau de bord interactif pour visualiser et analyser les données des taxis jaunes de New York stockées dans le Data Warehouse PostgreSQL.

### Méthode

L'application est développée avec **Streamlit**, un framework Python permettant de créer des interfaces web interactives rapidement. Le dashboard se connecte directement à PostgreSQL via SQLAlchemy et exécute des requêtes SQL sur le modèle dimensionnel pour générer des visualisations.

### Fonctionnalités du dashboard

Le dashboard est organisé en **4 sections** :

| Section | Contenu |
|---------|---------|
| **Vue d'ensemble** | KPIs (total courses, revenu total, distance moyenne, durée moyenne), courses par jour de la semaine, répartition par type de paiement et par fournisseur |
| **Analyse temporelle** | Évolution quotidienne des courses, distribution horaire, heures de pointe vs heures creuses, comparaison weekend vs semaine |
| **Analyse géographique** | Courses par arrondissement (Manhattan, Brooklyn, Queens...), treemap hiérarchique, tableau avec distance et montant moyen par zone |
| **Analyse financière** | Revenus totaux et moyens, évolution des revenus quotidiens, revenus par type de paiement, analyse des pourboires |

### Outils et technologies

| Outil | Usage |
|-------|-------|
| Streamlit | Framework web Python |
| Plotly | Graphiques interactifs (barres, camemberts, lignes, treemaps) |
| Pandas | Manipulation des données |
| SQLAlchemy | ORM et connexion PostgreSQL |
| psycopg2 | Driver PostgreSQL natif |
| Python 3.8+ | Langage d'implémentation |

### Exécution

Le dashboard est accessible sur `http://localhost:8501` après lancement via le script `run_ex04.sh` qui gère automatiquement la création de l'environnement virtuel, l'installation des dépendances et le démarrage de l'application.

### Conclusion

Le dashboard Streamlit offre une interface intuitive pour explorer les données NYC Taxi sous différents angles (temporel, géographique, financier). L'utilisation de Plotly permet des graphiques interactifs (zoom, filtrage, survol) qui facilitent l'analyse exploratoire. La connexion directe au Data Warehouse garantit que les visualisations reflètent les données les plus récentes.

---

## Exercice 5 : Service de prédiction ML

### Objectif

Concevoir un service de Machine Learning pour prédire le tarif total (`total_amount`) des courses de taxis, en utilisant les données nettoyées des exercices précédents. Cet exercice constitue une introduction au **MLOps**.

### Méthode

#### Choix de l'algorithme : Gradient Boosting Regressor

Le **Gradient Boosting Regressor** a été choisi pour ses performances sur les problèmes de régression tabulaire. Il construit un ensemble d'arbres de décision de manière séquentielle, chaque nouvel arbre corrigeant les erreurs du précédent.

#### Features utilisées

| Feature | Type | Description |
|---------|------|-------------|
| `trip_distance` | Numérique | Distance du trajet (miles) |
| `passenger_count` | Entier | Nombre de passagers |
| `PULocationID` | Catégorique | Zone de prise en charge (1-265) |
| `DOLocationID` | Catégorique | Zone de dépose (1-265) |
| `RatecodeID` | Catégorique | Type de tarif (1-6) |
| `hour` | Numérique | Heure de la course (0-23) - extraite |
| `day_of_week` | Numérique | Jour de la semaine (0-6) - extrait |
| `month` | Numérique | Mois (1-12) - extrait |

Les 3 dernières features (`hour`, `day_of_week`, `month`) sont des **features engineerées** extraites de l'horodatage de la course.

#### Métrique de performance : RMSE

La **RMSE (Root Mean Squared Error)** a été retenue comme métrique principale car :
- Elle **pénalise davantage les grandes erreurs** (une prédiction à 50$ au lieu de 10$ est plus grave qu'une erreur de 2$)
- Elle est exprimée dans la **même unité que la cible** (dollars), ce qui facilite l'interprétation
- C'est le **standard industriel** pour les problèmes de régression de prix

| Métrique | Objectif | Description |
|----------|----------|-------------|
| RMSE | < 10 $ | Erreur quadratique moyenne |
| MAE | < 5 $ | Erreur absolue moyenne |
| R² | > 0.7 | Coefficient de détermination |

#### Pipeline d'entraînement

1. Chargement des données Parquet (locales ou depuis MinIO)
2. Extraction des features temporelles (heure, jour, mois)
3. Préparation des features et de la target
4. Validation des données (plages, types, nulls)
5. Entraînement du Gradient Boosting Regressor
6. Évaluation sur un jeu de test
7. Sauvegarde du modèle (`models/latest_model.joblib`)

### Structure du code

| Module | Rôle |
|--------|------|
| `src/train.py` | Pipeline d'entraînement complet |
| `src/predict.py` | Inférence (CLI interactif ou JSON) |
| `src/data_validation.py` | Validation des données d'entrée |
| `app.py` | Application Streamlit de démonstration |
| `tests/` | Tests unitaires (validation + modèle) |

### Outils et technologies

| Outil | Usage |
|-------|-------|
| Python | Langage d'implémentation |
| uv | Gestionnaire d'environnement et dépendances |
| scikit-learn | Gradient Boosting Regressor |
| Pandas | Manipulation des DataFrames |
| PyArrow | Lecture des fichiers Parquet |
| joblib | Sérialisation du modèle |
| Streamlit | Interface de démonstration |
| pytest | Tests unitaires |
| flake8 | Conformité PEP 8 |

### Contraintes respectées

- Scripts Python (pas de notebooks)
- Documentation au format **NumpyDoc** (compatible pyment)
- Conformité **PEP 8** (vérifiable avec flake8)
- Tests unitaires sur la validation des données
- Utilisation exclusive d'environnements virtuels gérés par **uv** (pas de Python natif)

### Conclusion

Le service de prédiction ML atteint les objectifs de performance fixés (RMSE < 10, R² > 0.7). Le pipeline est conçu selon les principes MLOps : reproductibilité (scripts, pas de notebooks), validation des données en entrée, tests automatisés et interface de démonstration. Le modèle Gradient Boosting s'avère bien adapté à ce type de données tabulaires avec des features mixtes (numériques et catégoriques).

---

## Exercice 6 : Orchestration avec Airflow

### Objectif

Mettre en place un système d'orchestration avec **Apache Airflow** pour automatiser l'exécution de l'ensemble des exercices du projet sous forme de pipeline de données (DAG).

### Méthode

#### DAG principal : `nyc_taxi_bigdata_pipeline`

Le DAG orchestre les exercices dans un ordre séquentiel respectant leurs dépendances :

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

#### Configuration du DAG

| Paramètre | Valeur | Justification |
|-----------|--------|---------------|
| `schedule_interval` | `@monthly` | Les données NYC sont publiées mensuellement |
| `retries` | 1 | Une seule tentative de réexécution en cas d'échec |
| `retry_delay` | 5 minutes | Délai avant la tentative de réexécution |
| `catchup` | False | Pas d'exécution rétroactive |

#### Architecture des services Airflow

| Service | Rôle |
|---------|------|
| Webserver | Interface web (port 8080) |
| Scheduler | Planification et déclenchement des tâches |
| Worker | Exécution des tâches (Celery) |
| Redis | Broker de messages pour Celery |
| PostgreSQL (metadata) | Stockage des métadonnées Airflow |

### Bonnes pratiques implémentées

1. **Idempotence** : Les tâches sont conçues pour être ré-exécutables sans effet de bord
2. **Vérification** : Chaque exercice inclut une étape de vérification des prérequis
3. **Centralisation des logs** : Tous les logs sont accessibles dans l'interface Airflow
4. **Monitoring** : Suivi en temps réel via la vue Graph du DAG

### Outils et technologies

| Outil | Usage |
|-------|-------|
| Apache Airflow | Orchestrateur de workflows |
| Celery | Exécution distribuée des tâches |
| Redis | Broker de messages |
| Docker Compose | Déploiement multi-conteneurs |
| Python | Définition du DAG |

### Conclusion

L'intégration d'Airflow permet de passer d'une exécution manuelle des exercices à un pipeline entièrement automatisé et monitorable. La planification mensuelle est adaptée au rythme de publication des données NYC TLC. L'interface web d'Airflow offre une visibilité complète sur l'état du pipeline et facilite le diagnostic en cas d'échec.

---

## Conclusion générale

Ce projet Big Data illustre la mise en œuvre d'un **pipeline de données complet et fonctionnel**, depuis la collecte de données brutes jusqu'à leur exploitation par un modèle de Machine Learning, en passant par le nettoyage, la modélisation, la visualisation et l'orchestration.

### Synthèse des technologies utilisées

| Catégorie | Technologies |
|-----------|-------------|
| **Traitement distribué** | Apache Spark 3.5.0 (Scala) |
| **Stockage Data Lake** | MinIO (S3-compatible) |
| **Data Warehouse** | PostgreSQL 15 |
| **Visualisation** | Streamlit, Plotly |
| **Machine Learning** | scikit-learn (Gradient Boosting) |
| **Orchestration** | Apache Airflow (Celery) |
| **Conteneurisation** | Docker, Docker Compose |
| **Langages** | Scala, Python, SQL, Bash |
| **Build / Dépendances** | SBT (Scala), uv (Python) |
| **Tests** | ScalaTest, pytest |

### Points forts du projet

- **Architecture cohérente** : Chaque exercice s'appuie sur les résultats du précédent, formant un pipeline linéaire et logique
- **Qualité des données** : Un contrat de données strict (11 règles de validation) garantit la fiabilité des analyses en aval
- **Reproductibilité** : L'utilisation de Docker et de scripts d'exécution automatisés permet de reproduire l'environnement sur n'importe quelle machine
- **Couverture de tests** : Chaque module dispose de tests unitaires (14 + 14 + 20 tests pour les exercices Scala, tests pytest pour le ML)
- **Modèle dimensionnel** : Le schéma en flocon offre un bon compromis entre normalisation et performance analytique
- **MLOps** : Le service ML respecte les bonnes pratiques (scripts, validation, tests, documentation PEP 8 / NumpyDoc)

### Volume de données traité

| Étape | Volume approximatif |
|-------|---------------------|
| Données brutes collectées | ~9 000 000 lignes (3 mois) |
| Données après nettoyage | ~8 500 000 lignes (~5-6% de rejet) |
| Données dans le Data Warehouse | ~8 400 000 lignes |

Ce projet démontre qu'il est possible de construire un pipeline Big Data fonctionnel en combinant des technologies open source complémentaires, chacune répondant à un besoin spécifique de la chaîne de traitement des données.
