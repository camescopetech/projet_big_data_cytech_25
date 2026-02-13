# Exercice 5 : ML Prediction Service - NYC Taxi Fare

> **IMPORTANT** : L'utilisation de Python natif est **strictement interdite**.
> Vous devez utiliser les environnements virtuels gérés par **uv**.

## Présentation

Service de prédiction des tarifs de taxis jaunes de New York utilisant un modèle de Machine Learning (Gradient Boosting).

## Démarrage Rapide

```bash
# Se placer dans le répertoire
cd ex05_ml_prediction_service

# Exécuter le pipeline complet (entraînement + app)
./run_ex05.sh

# OU exécuter étape par étape :
./run_ex05.sh --train-only   # Entraînement uniquement
./run_ex05.sh --app-only     # Application uniquement
./run_ex05.sh --test         # Tests unitaires
```

## Prérequis

- **uv** : Gestionnaire d'environnement Python ([installation](https://github.com/astral-sh/uv))
- Données Parquet dans `../ex01_data_retrieval/data/raw/`

## Documentation

Consultez le guide complet : [ex05-guide.md](./ex05-guide.md)

## Structure

```
ex05_ml_prediction_service/
├── pyproject.toml          # Dépendances (uv)
├── run_ex05.sh             # Script principal
├── app.py                  # Application Streamlit
├── src/
│   ├── data_validation.py  # Validation des données
│   ├── train.py            # Entraînement
│   └── predict.py          # Inférence
├── tests/                  # Tests unitaires
└── models/                 # Modèles entraînés
```

## Performance

- **Target** : `total_amount` (tarif total)
- **Métrique** : RMSE < 10 (objectif atteint)
- **Algorithme** : Gradient Boosting Regressor
