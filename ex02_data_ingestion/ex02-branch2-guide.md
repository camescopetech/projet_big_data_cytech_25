# Exercice 2 - Branche 2 : Ingestion vers PostgreSQL

## Objectif

Insérer les données nettoyées depuis MinIO vers la table de faits `fact_trips` dans PostgreSQL, en respectant le modèle dimensionnel (snowflake schema) créé dans l'exercice 3.

## Architecture du Pipeline

```
┌─────────────────────────────────────────────────────────────┐
│                    MinIO (nyc-cleaned)                       │
│              Données nettoyées de la branche 1               │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                      Spark Job                               │
│  1. Lire le parquet nettoyé depuis MinIO                    │
│  2. Calculer les clés de dimension (date_id, time_id)       │
│  3. Transformer les colonnes pour le schéma                 │
│  4. Insérer dans fact_trips via JDBC                        │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                    PostgreSQL                                │
│                    (nyc_taxi_dw)                             │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                   fact_trips                         │    │
│  │  - vendor_id → dim_vendor                           │    │
│  │  - rate_code_id → dim_rate_code                     │    │
│  │  - payment_type_id → dim_payment_type               │    │
│  │  - pickup_location_id → dim_location                │    │
│  │  - pickup_date_id → dim_date                        │    │
│  │  - pickup_time_id → dim_time                        │    │
│  │  + mesures (fare, distance, duration, etc.)         │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

## Prérequis

1. **Exercice 2 - Branche 1 terminé** : Données nettoyées dans MinIO (`nyc-cleaned`)
2. **Exercice 3 terminé** : Tables créées dans PostgreSQL
3. **Docker** en cours d'exécution avec MinIO et PostgreSQL

## Structure du Projet

```
ex02_data_ingestion/
├── build.sbt                    # Configuration SBT
├── .jvmopts                     # Options JVM pour Java 17
├── project/
│   └── build.properties         # Version SBT
├── run_ex02_branch2.sh          # Script d'exécution
├── run_tests.sh                 # Script de tests
├── ex02-branch2-guide.md        # Ce guide
└── src/
    ├── main/scala/fr/cytech/ingestion/
    │   └── Main.scala           # Code principal
    └── test/scala/fr/cytech/ingestion/
        └── MainSpec.scala       # Tests unitaires
```

## Exécution

### 1. Lancer l'ingestion

```bash
cd ex02_data_ingestion
chmod +x run_ex02_branch2.sh
./run_ex02_branch2.sh
```

Ou manuellement :

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt run
```

### 2. Lancer les tests

```bash
chmod +x run_tests.sh
./run_tests.sh
```

Ou manuellement :

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt test
```

## Transformation des Données

### Calcul des Clés de Dimension

| Dimension | Format | Exemple |
|-----------|--------|---------|
| `date_id` | YYYYMMDD | 20240115 |
| `time_id` | HHMM (arrondi 30 min) | 0830 |

### Exemple de Transformation

```
Pickup: 2024-01-15 08:47:32
  → pickup_date_id: 20240115
  → pickup_time_id: 830 (arrondi à 08:30)

Dropoff: 2024-01-15 09:15:00
  → dropoff_date_id: 20240115
  → dropoff_time_id: 900 (arrondi à 09:00)
```

### Mapping des Colonnes

| Source (Parquet) | Destination (PostgreSQL) |
|------------------|--------------------------|
| VendorID | vendor_id |
| RatecodeID | rate_code_id |
| payment_type | payment_type_id |
| PULocationID | pickup_location_id |
| DOLocationID | dropoff_location_id |
| tpep_pickup_datetime | pickup_datetime |
| tpep_dropoff_datetime | dropoff_datetime |
| Airport_fee | airport_fee |

## Tests Unitaires (20 tests)

| Catégorie | Tests |
|-----------|-------|
| Calcul date_id | Format YYYYMMDD, padding mois |
| Calcul time_id | Arrondi 0 min, arrondi 30 min, heures limites |
| Validation date_id | Dates valides, dates invalides |
| Validation time_id | Heures valides, heures invalides |
| Transformation DataFrame | Calcul depuis timestamp |
| Schéma de sortie | Colonnes requises présentes |
| Types de données | Entiers, décimaux |
| Cas limites | Minuit, fin de journée, 1er janvier, 31 décembre |

## Résultat Attendu

```
==========================================
Exercice 2 - Branche 2 : Ingestion PostgreSQL
==========================================
Java Home: /Library/Java/JavaVirtualMachines/openjdk-17.jdk/Contents/Home

[1/6] Vérification de Docker...
✓ Docker est actif

[2/6] Vérification de MinIO...
✓ MinIO est actif

[3/6] Vérification de PostgreSQL...
✓ PostgreSQL est actif

[4/6] Vérification du bucket nyc-cleaned...
✓ Prérequis vérifié

[5/6] Vérification des tables PostgreSQL...
✓ Tables PostgreSQL prêtes (8 tables)

[6/6] Exécution du programme Spark...
==========================================

================================================================================
EXERCICE 2 - BRANCHE 2 : Ingestion vers PostgreSQL
================================================================================

[Étape 1/5] Lecture des données nettoyées depuis MinIO...
  Source : s3a://nyc-cleaned/yellow_tripdata_2024-01.parquet
  ✓ Fichier lu avec succès
  - Nombre de lignes : 2,698,737

[Étape 2/5] Transformation pour le modèle dimensionnel...
  ✓ Colonnes transformées pour le modèle dimensionnel
  - Clés dim_date calculées (format YYYYMMDD)
  - Clés dim_time calculées (format HHMM, arrondi 30min)

[Étape 3/5] Vérification des dimensions dans PostgreSQL...
  - dim_vendor           : 2 lignes
  - dim_rate_code        : 6 lignes
  - dim_payment_type     : 6 lignes
  - dim_location         : 234 lignes
  - dim_date             : 366 lignes
  - dim_time             : 48 lignes
  ✓ Toutes les dimensions sont prêtes

[Étape 4/5] Insertion des données dans fact_trips...
  - Lignes à insérer : 2,698,737
  ✓ 2,698,737 lignes insérées avec succès

[Étape 5/5] Vérification de l'ingestion...
  ✓ Table fact_trips : 2,698,737 lignes

================================================================================
EXERCICE 2 - BRANCHE 2 TERMINÉ AVEC SUCCÈS !
================================================================================
```

## Vérification

### Compter les lignes

```bash
docker exec postgres psql -U datawarehouse -d nyc_taxi_dw -c "SELECT COUNT(*) FROM fact_trips;"
```

### Aperçu des données

```bash
docker exec postgres psql -U datawarehouse -d nyc_taxi_dw -c "
SELECT
    f.trip_id,
    v.vendor_name,
    f.pickup_datetime,
    f.trip_distance,
    f.total_amount
FROM fact_trips f
JOIN dim_vendor v ON f.vendor_id = v.vendor_id
LIMIT 5;"
```

### Statistiques par jour

```bash
docker exec postgres psql -U datawarehouse -d nyc_taxi_dw -c "
SELECT
    d.day_name,
    COUNT(*) AS nb_courses,
    ROUND(AVG(f.total_amount)::numeric, 2) AS avg_total
FROM fact_trips f
JOIN dim_date d ON f.pickup_date_id = d.date_id
GROUP BY d.day_name, d.day_of_week
ORDER BY d.day_of_week;"
```

## Problèmes Courants

### Erreur : "Bucket nyc-cleaned not found"
**Solution** : Exécutez l'exercice 2 branche 1 d'abord

### Erreur : "Table fact_trips does not exist"
**Solution** : Exécutez l'exercice 3 d'abord

### Erreur : "Connection refused" (PostgreSQL)
**Solution** : Lancez PostgreSQL avec `docker compose up -d postgres`

### Insertion lente
**Cause** : Volume important de données (~2.7M lignes)
**Solution** : L'insertion utilise un batch de 10000 lignes

## Prochaines Étapes

1. **Exercice 4** : Visualisation des données avec Streamlit ou un notebook
2. **Exercice 5** : Machine Learning sur les données nettoyées (MinIO)
