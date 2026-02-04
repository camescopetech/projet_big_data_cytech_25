
# Exercice 2 : Nettoyage des Données et Ingestion Multi-Branche

## Objectif

L'exercice 2 consiste à créer un pipeline Spark avec **deux branches** pour traiter **3 mois de données** (Juin-Août 2025) :

1. **Branche 1** (ce dossier) : Nettoyage des données → MinIO
2. **Branche 2** (après exercice 3) : Transformation → PostgreSQL

## Architecture du Pipeline

```
┌─────────────────────────────────────────────────────────────┐
│                    MinIO (nyc-raw)                          │
│              Données brutes de l'exercice 1                 │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                      Spark Job                              │
│  1. Lire le parquet depuis MinIO                            │
│  2. Valider selon le contrat NYC TLC                        │
│  3. Nettoyer les données invalides                          │
│  4. Ajouter des colonnes calculées                          │
└─────────────────────────┬───────────────────────────────────┘
                          │
          ┌───────────────┴───────────────┐
          │                               │
          ▼                               ▼
┌─────────────────────┐       ┌─────────────────────┐
│    BRANCHE 1        │       │    BRANCHE 2        │
│  → MinIO            │       │  → PostgreSQL       │
│  (nyc-cleaned)      │       │  (Data Warehouse)   │
│                     │       │                     │
│  Pour ML/Data       │       │  Requiert Ex3       │
│  Science (Ex5)      │       │  (tables SQL)       │
└─────────────────────┘       └─────────────────────┘
```

## Branche 1 : Nettoyage → MinIO

### Règles de Validation (Contrat NYC TLC)

| Règle | Colonne | Condition | Description |
|-------|---------|-----------|-------------|
| 1 | VendorID | IN (1, 2) | 1=CMT, 2=VeriFone |
| 2 | passenger_count | 1-9 | Nombre de passagers |
| 3 | trip_distance | 0-100 | Distance en miles |
| 4 | fare_amount | >= 0 | Tarif de base |
| 5 | total_amount | >= 0 | Montant total |
| 6 | tip_amount | >= 0 | Pourboire |
| 7 | tolls_amount | >= 0 | Péages |
| 8 | pickup < dropoff | datetime | Durée positive |
| 9 | RatecodeID | 1-6 | Code tarifaire |
| 10 | payment_type | 1-6 | Type de paiement |
| 11 | trip_duration | 1-240 min | Durée raisonnable |

### Codes de Référence

**RatecodeID :**
| Code | Description |
|------|-------------|
| 1 | Standard rate |
| 2 | JFK |
| 3 | Newark |
| 4 | Nassau or Westchester |
| 5 | Negotiated fare |
| 6 | Group ride |

**payment_type :**
| Code | Description |
|------|-------------|
| 1 | Credit card |
| 2 | Cash |
| 3 | No charge |
| 4 | Dispute |
| 5 | Unknown |
| 6 | Voided trip |

## Structure du Projet

```
ex02_data_cleaning/
├── build.sbt                 # Configuration SBT
├── .jvmopts                  # Options JVM
├── run_ex02.sh               # Script d'exécution
├── run_tests.sh              # Script de tests
├── ex02-guide.md             # Ce guide
└── src/
    ├── main/scala/fr/cytech/cleaning/
    │   └── Main.scala        # Code principal
    └── test/scala/fr/cytech/cleaning/
        └── MainSpec.scala    # Tests unitaires
```

## Prérequis

1. **Exercice 1 terminé** : Les données brutes doivent être dans MinIO (bucket `nyc-raw`)
2. **MinIO en cours d'exécution** : `docker-compose up -d`
3. **Bucket `nyc-cleaned` créé** : Via l'interface MinIO (http://localhost:9001)

### Créer le bucket nyc-cleaned

1. Accéder à http://localhost:9001
2. Se connecter : `minio` / `minio123`
3. Cliquer sur "Create Bucket"
4. Nom : `nyc-cleaned`

## Exécution

### Lancer le nettoyage

```bash
cd ex02_data_cleaning
./run_ex02.sh
```

Ou manuellement :

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt run
```

### Lancer les tests

```bash
./run_tests.sh
```

## Résultat Attendu

```
================================================================================
EXERCICE 2 : Nettoyage des données NYC Taxi (Branche 1)
================================================================================
Période : Juin - Août 2025 (3 mois)

================================================================================
Traitement du mois 06/2025
================================================================================

[Étape 1/6] Lecture des données brutes depuis MinIO...
  ✓ Fichier lu avec succès
  - Nombre de lignes : ~3,000,000

[Étape 2/6] Analyse des données avant nettoyage...
  Statistiques des colonnes clés...

[Étape 3/6] Nettoyage des données...
  ✓ Règles de validation appliquées

[Étape 4/6] Statistiques après nettoyage...
  Résumé du nettoyage :
    - Lignes avant  : ~3,000,000
    - Lignes après  : ~2,850,000
    - Lignes supprimées : ~150,000 (5-6%)

[Étape 5/6] Écriture des données nettoyées vers MinIO...
  ✓ Données écrites avec succès

[Étape 6/6] Vérification des données...
  ✓ Vérification réussie

✓ Mois 06/2025 traité avec succès

================================================================================
Traitement du mois 07/2025
================================================================================
[...même processus pour juillet...]

================================================================================
Traitement du mois 08/2025
================================================================================
[...même processus pour août...]

================================================================================
EXERCICE 2 - BRANCHE 1 TERMINÉ AVEC SUCCÈS !
================================================================================

Les données nettoyées sont disponibles dans MinIO :
  Bucket : nyc-cleaned
  Fichiers :
    - yellow_tripdata_2025-06.parquet
    - yellow_tripdata_2025-07.parquet
    - yellow_tripdata_2025-08.parquet
```

## Tests Unitaires (14 tests)

| Test | Description |
|------|-------------|
| VendorID | Filtre les valeurs != 1, 2 |
| passenger_count | Filtre hors 1-9 |
| trip_distance | Filtre hors 0-100 |
| Montants négatifs | Filtre fare/total < 0 |
| Durée pickup >= dropoff | Filtre dates inversées |
| Durée calculée | Vérifie le calcul en minutes |
| Durée aberrante | Filtre < 1 ou > 240 min |
| RatecodeID | Filtre hors 1-6 |
| payment_type | Filtre hors 1-6 |
| Valeurs nulles | Supprime lignes incomplètes |
| Pipeline complet | Test d'intégration |

## Prochaines Étapes

1. **Exercice 3** : Créer les tables SQL (star/snowflake schema)
2. **Exercice 2 - Branche 2** : Insérer les données nettoyées dans PostgreSQL
3. **Exercice 4** : Visualisation des données
4. **Exercice 5** : Machine Learning avec les données nettoyées

## Problèmes Courants

### Erreur : "Bucket nyc-raw not found"
**Solution** : Exécuter l'exercice 1 d'abord

### Erreur : "Bucket nyc-cleaned not found"
**Solution** : Créer le bucket via http://localhost:9001

### Taux de suppression trop élevé (> 10%)
**Cause** : Données sources très bruitées
**Solution** : Vérifier les règles de validation, ajuster si nécessaire
