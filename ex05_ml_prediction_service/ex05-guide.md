# Exercice 5 : Service de Prédiction ML - NYC Taxi Fare

## Objectif

L'objectif de cet exercice est de concevoir un **modèle de Machine Learning** pour prédire le tarif total (`total_amount`) des courses de taxis jaunes de New York, en utilisant les données nettoyées des exercices précédents.

Ce service constitue une introduction au **MLOps** et comprend :
- Un pipeline d'entraînement complet
- Un script d'inférence
- Des tests unitaires sur les données d'entrée
- Une application Streamlit de démonstration (optionnelle)

### Autres contraintes respectées :
- L'entraînement et l'inférence sont réalisés sous forme de **scripts Python** (pas de notebook)
- Le code est documenté avec **NumpyDoc** (compatible pyment)
- Le code respecte la norme **PEP 8** (vérifiable avec flake8)
- Des **tests unitaires** sont implémentés pour la validation des données

## Architecture du Service

```
ex05_ml_prediction_service/
├── pyproject.toml          # Configuration du projet et dépendances (uv)
├── run_ex05.sh             # Script d'exécution principal
├── app.py                  # Application Streamlit (front-end optionnel)
├── src/
│   ├── __init__.py
│   ├── data_validation.py  # Module de validation des données
│   ├── train.py            # Script d'entraînement
│   └── predict.py          # Script d'inférence
├── tests/
│   ├── __init__.py
│   ├── test_data_validation.py  # Tests de validation
│   └── test_model.py            # Tests du modèle
├── models/                 # Modèles entraînés (généré)
│   └── latest_model.joblib
└── ex05-guide.md           # Ce guide
```

## Modèle de Machine Learning

### Target (Variable Cible)
- **`total_amount`** : Prix total de la course en dollars

### Features Utilisées

| Feature | Description | Type |
|---------|-------------|------|
| `trip_distance` | Distance du trajet (miles) | Numérique |
| `passenger_count` | Nombre de passagers | Entier |
| `PULocationID` | Zone de pickup (1-265) | Catégorique |
| `DOLocationID` | Zone de dropoff (1-265) | Catégorique |
| `RatecodeID` | Type de tarif (1-6) | Catégorique |
| `hour` | Heure de la course (0-23) | Numérique |
| `day_of_week` | Jour de la semaine (0-6) | Numérique |
| `month` | Mois (1-12) | Numérique |

### Algorithme Utilisé
- **Gradient Boosting Regressor**

### Métrique de Performance
- **RMSE (Root Mean Squared Error)** : Objectif < 10
- Métriques secondaires : MAE, R²

## Utilisation

### Prérequis

1. **Installer uv** (si pas déjà installé) :
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. **Données disponibles** : Les fichiers Parquet doivent être présents dans `../ex01_data_retrieval/data/raw/`

### Exécuter le pipeline complet

```bash
cd ex05_ml_prediction_service
./run_ex05.sh
```

Ce script va :
1. Vérifier l'installation de uv
2. Installer les dépendances
3. Vérifier la présence des données
4. Entraîner le modèle
5. Lancer l'application Streamlit

### Options du script

```bash
# Entraînement uniquement
./run_ex05.sh --train-only

# Application uniquement (nécessite un modèle pré-entraîné)
./run_ex05.sh --app-only

# Exécuter les tests
./run_ex05.sh --test

# Aide
./run_ex05.sh --help
```

### Utilisation manuelle avec uv

```bash
# Installer les dépendances
uv sync

# Entraîner le modèle
uv run python src/train.py --source local

# Faire une prédiction interactive
uv run python src/predict.py --interactive

# Faire une prédiction depuis JSON
uv run python src/predict.py --json '{"trip_distance": 5.0, "passenger_count": 2, "PULocationID": 100, "DOLocationID": 200, "RatecodeID": 1}'

# Lancer l'application Streamlit
uv run streamlit run app.py --server.port 8502

# Exécuter les tests
uv run pytest tests/ -v

# Vérifier PEP 8
uv run flake8 src/ --max-line-length=100
```

## Validation des Données

Le module `data_validation.py` implémente des validations strictes :

### Règles de Validation (NYC TLC)

| Colonne | Min | Max |
|---------|-----|-----|
| `trip_distance` | 0.0 | 100.0 miles |
| `passenger_count` | 0 | 9 |
| `PULocationID` | 1 | 265 |
| `DOLocationID` | 1 | 265 |
| `RatecodeID` | 1 | 6 |
| `total_amount` | 0.0 | 500.0 $ |

### Tests Unitaires

Les tests couvrent :
- Validation des colonnes présentes
- Validation des types de données
- Validation des plages de valeurs
- Détection des valeurs nulles
- Tests de bout en bout (train + predict)

## Application Streamlit

L'application de démonstration permet :

- **Prédiction en temps réel** : Entrez les paramètres d'une course
- **Visualisation des résultats** : Tarif prédit avec intervalle de confiance
- **Analyse** : Impact de la distance et de l'heure sur le tarif

### Captures d'écran

L'application est accessible sur `http://localhost:8502` après le lancement.

## Résultats Attendus

Après entraînement sur les données de 3 mois (Juin-Août 2025) :

- **RMSE Test** : < 10 (objectif)
- **R² Test** : > 0.7 (bon pouvoir prédictif)
- **MAE Test** : < 5 $ (erreur moyenne acceptable)

## Justification de la Métrique

La **RMSE (Root Mean Squared Error)** a été choisie car :

1. **Pénalise les grandes erreurs** : Une prédiction à 50$ au lieu de 10$ est plus grave qu'une erreur de 2$
2. **Même unité que la cible** : Facilite l'interprétation (en dollars)
3. **Standard industriel** : Couramment utilisée pour les problèmes de régression de prix

Alternative considérée : La **MAE (Mean Absolute Error)** est moins sensible aux outliers mais ne pénalise pas suffisamment les grandes erreurs de prédiction.

## Structure du Code

### train.py

```python
# Fonctions principales :
- load_data_from_local()     # Charge les Parquet locaux
- load_data_from_minio()     # Charge depuis MinIO
- extract_time_features()    # Extrait heure, jour, mois
- prepare_features()         # Prépare X et y
- train_model()              # Entraîne le modèle
- save_model()               # Sauvegarde le modèle
```

### predict.py

```python
# Fonctions principales :
- load_model()               # Charge le modèle
- prepare_input()            # Prépare les données d'entrée
- predict()                  # Effectue la prédiction
- interactive_prediction()   # Mode interactif CLI
```

### data_validation.py

```python
# Fonctions principales :
- validate_columns()         # Vérifie les colonnes
- validate_data_types()      # Vérifie les types
- validate_value_ranges()    # Vérifie les plages
- validate_no_nulls()        # Vérifie les nulls
- validate_dataframe()       # Validation complète
- clean_dataframe()          # Nettoie les données
```

## Dépendances

Définies dans `pyproject.toml` :

```toml
dependencies = [
    "pandas>=2.0.0",
    "pyarrow>=14.0.0",
    "scikit-learn>=1.3.0",
    "numpy>=1.24.0",
    "joblib>=1.3.0",
    "minio>=7.2.0",
    "streamlit>=1.28.0",
    "plotly>=5.18.0",
]
```

Dépendances de développement :
- pytest (tests)
- flake8 (PEP 8)
- pyment (documentation)

## Prochaines Étapes (MLOps)

Ce service pose les bases pour :
- **Versioning des modèles** avec MLflow
- **CI/CD** pour l'entraînement automatique
- **Monitoring** des prédictions en production
- **A/B Testing** de nouveaux modèles
