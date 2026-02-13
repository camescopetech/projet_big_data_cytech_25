# Exercice 3 : Data Warehouse et Modèle Multi-Dimensionnel

## Objectif

Créer un modèle de données multi-dimensionnel (snowflake schema) pour le Data Warehouse NYC Taxi, optimisé pour l'analyse OLAP des données de **Juin-Août 2025** (3 mois).

## Architecture du Data Warehouse

```
                        ┌─────────────────┐
                        │   dim_borough   │
                        │  (sous-dim)     │
                        └────────┬────────┘
                                 │
┌──────────────┐    ┌────────────┴────────────┐    ┌──────────────┐
│  dim_vendor  │    │      dim_location       │    │ dim_rate_code│
└──────┬───────┘    └────────────┬────────────┘    └──────┬───────┘
       │                         │                        │
       │    ┌────────────────────┼────────────────────┐   │
       │    │                    │                    │   │
       ▼    ▼                    ▼                    ▼   ▼
┌──────────────────────────────────────────────────────────────────┐
│                         fact_trips                               │
│                                                                   │
│  Mesures: fare_amount, tip_amount, total_amount,                 │
│           trip_distance, trip_duration_minutes, passenger_count  │
└──────────────────────────────────────────────────────────────────┘
       ▲    ▲                    ▲
       │    │                    │
       │    └────────────────────┼────────────────────┐
       │                         │                    │
┌──────┴───────┐    ┌────────────┴────────────┐    ┌──────────────┐
│dim_payment   │    │        dim_date         │    │   dim_time   │
│    _type     │    │    (calendrier)         │    │   (heure)    │
└──────────────┘    └─────────────────────────┘    └──────────────┘
```

## Modèle Choisi : Flocon (Snowflake)

### Justification

1. **Normalisation** : `dim_location` référence `dim_borough` (évite la redondance)
2. **Intégrité** : Les données de référence NYC TLC sont normalisées
3. **Flexibilité** : Permet des analyses à différents niveaux de granularité

## Structure des Tables

### Table de Faits

| Table | Description | Grain |
|-------|-------------|-------|
| `fact_trips` | Courses de taxi | 1 ligne = 1 course |

### Dimensions de Niveau 1

| Table | Description | Cardinalité |
|-------|-------------|-------------|
| `dim_vendor` | Fournisseurs TPEP | 2 |
| `dim_rate_code` | Codes tarifaires | 6 |
| `dim_payment_type` | Types de paiement | 6 |
| `dim_location` | Zones de taxi | ~265 |
| `dim_date` | Calendrier | 365 (année 2025) |
| `dim_time` | Créneaux horaires | 48 (30 min) |

### Dimensions de Niveau 2 (Flocon)

| Table | Description | Cardinalité |
|-------|-------------|-------------|
| `dim_borough` | Arrondissements | 7 |

## Fichiers SQL

```
ex03_sql_table_creation/
├── creation.sql      # Création des tables avec contraintes
├── insertion.sql     # Données de référence NYC TLC
├── run_ex03.sh       # Script d'exécution
└── ex03-guide.md     # Ce guide
```

## Prérequis

1. **Docker** en cours d'exécution
2. **docker-compose.yml** avec PostgreSQL configuré

## Exécution

```bash
cd ex03_sql_table_creation
./run_ex03.sh
```

## Connexion à PostgreSQL

```
Host: localhost
Port: 5432
Database: nyc_taxi_dw
User: datawarehouse
Password: datawarehouse123
```

### Via psql

```bash
docker exec -it postgres psql -U datawarehouse -d nyc_taxi_dw
```

### Via DBeaver / DataGrip

Créez une connexion PostgreSQL avec les paramètres ci-dessus.

## Exemples de Requêtes Analytiques

### Revenus par jour de la semaine

```sql
SELECT
    d.day_name,
    COUNT(*) AS nb_courses,
    ROUND(AVG(f.total_amount), 2) AS avg_total,
    SUM(f.total_amount) AS revenue_total
FROM fact_trips f
JOIN dim_date d ON f.pickup_date_id = d.date_id
GROUP BY d.day_name, d.day_of_week
ORDER BY d.day_of_week;
```

### Analyse par zone et période

```sql
SELECT
    b.borough_name,
    t.period_of_day,
    COUNT(*) AS nb_courses,
    ROUND(AVG(f.trip_distance), 2) AS avg_distance
FROM fact_trips f
JOIN dim_location l ON f.pickup_location_id = l.location_id
JOIN dim_borough b ON l.borough_id = b.borough_id
JOIN dim_time t ON f.pickup_time_id = t.time_id
GROUP BY b.borough_name, t.period_of_day
ORDER BY b.borough_name, t.period_of_day;
```

### Comparaison des modes de paiement

```sql
SELECT
    p.payment_name,
    COUNT(*) AS nb_courses,
    ROUND(AVG(f.tip_amount), 2) AS avg_tip,
    ROUND(AVG(f.total_amount), 2) AS avg_total
FROM fact_trips f
JOIN dim_payment_type p ON f.payment_type_id = p.payment_type_id
GROUP BY p.payment_name
ORDER BY nb_courses DESC;
```

## Résultat Attendu

```
==========================================
Exercice 3 : Création du Data Warehouse
Période : Juin - Août 2025 (3 mois)
==========================================

[1/5] Vérification de Docker...
✓ Docker est actif

[2/5] Démarrage de PostgreSQL...
✓ PostgreSQL est prêt

[3/5] Création des tables (modèle flocon)...
✓ Tables créées avec succès

[4/5] Insertion des données de référence...
✓ Données de référence insérées

[5/5] Vérification des tables...

Tables créées :
    table_name     | row_count
-------------------+-----------
 dim_borough       |         7
 dim_date          |       365
 dim_location      |       178
 dim_payment_type  |         6
 dim_rate_code     |         6
 dim_time          |        48
 dim_vendor        |         2
 fact_trips        |         0

==========================================
✓ EXERCICE 3 TERMINÉ !
==========================================

Note: dim_date contient le calendrier 2025 complet,
prêt pour les données de juin-août 2025.
```

## Prochaines Étapes

1. **Exercice 2 - Branche 2** : Insérer les données nettoyées dans `fact_trips`
2. **Exercice 4** : Connecter un outil de visualisation à PostgreSQL
