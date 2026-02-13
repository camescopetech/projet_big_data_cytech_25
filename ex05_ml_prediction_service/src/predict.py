#!/usr/bin/env python3
"""
Script d'inference du modele ML pour la prediction des tarifs NYC Taxi.

Ce script charge un modele pre-entraine et effectue des predictions
sur de nouvelles donnees.

Notes
-----
Le modele doit avoir ete entraine au prealable avec train.py.

Examples
--------
Prediction sur un fichier Parquet:
    $ uv run python src/predict.py --input data/test.parquet

Prediction interactive:
    $ uv run python src/predict.py --interactive

Prediction depuis JSON:
    $ uv run python src/predict.py --json '{"trip_distance": 5.0, ...}'
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
import joblib

# Ajouter le repertoire parent au path
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.data_validation import validate_dataframe, clean_dataframe


MODEL_DIR = Path(__file__).parent.parent / "models"


def load_model(model_path: str = None) -> dict:
    """
    Charge le modele entraine depuis un fichier.

    Parameters
    ----------
    model_path : str, optional
        Chemin vers le fichier du modele. Si None, charge le modele "latest".

    Returns
    -------
    dict
        Dictionnaire contenant le pipeline et les metadonnees.

    Raises
    ------
    FileNotFoundError
        Si le fichier du modele n'existe pas.
    """
    if model_path is None:
        model_path = MODEL_DIR / "latest_model.joblib"
    else:
        model_path = Path(model_path)

    if not model_path.exists():
        raise FileNotFoundError(f"Modele non trouve: {model_path}")

    print(f"[INFO] Chargement du modele: {model_path}")
    model_data = joblib.load(model_path)

    print(f"[INFO] Modele cree le: {model_data.get('created_at', 'N/A')}")
    print(f"[INFO] Version: {model_data.get('version', 'N/A')}")

    return model_data


def extract_time_features_inference(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extrait les features temporelles pour l'inference.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame avec les donnees d'entree.

    Returns
    -------
    pd.DataFrame
        DataFrame avec les features temporelles ajoutees.
    """
    df = df.copy()

    if 'tpep_pickup_datetime' in df.columns:
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['hour'] = df['tpep_pickup_datetime'].dt.hour
        df['day_of_week'] = df['tpep_pickup_datetime'].dt.dayofweek
        df['month'] = df['tpep_pickup_datetime'].dt.month
    else:
        # Utiliser l'heure actuelle par defaut
        now = datetime.now()
        if 'hour' not in df.columns:
            df['hour'] = now.hour
        if 'day_of_week' not in df.columns:
            df['day_of_week'] = now.weekday()
        if 'month' not in df.columns:
            df['month'] = now.month

    return df


def prepare_input(
    data: dict or pd.DataFrame,
    feature_columns: list
) -> pd.DataFrame:
    """
    Prepare les donnees d'entree pour l'inference.

    Parameters
    ----------
    data : dict or pd.DataFrame
        Donnees d'entree.
    feature_columns : list
        Liste des colonnes de features attendues par le modele.

    Returns
    -------
    pd.DataFrame
        DataFrame pret pour l'inference.

    Raises
    ------
    ValueError
        Si des colonnes requises sont manquantes.
    """
    if isinstance(data, dict):
        df = pd.DataFrame([data])
    else:
        df = data.copy()

    # Extraction des features temporelles
    df = extract_time_features_inference(df)

    # Validation des colonnes
    is_valid, report = validate_dataframe(df, include_target=False, strict=False)
    if not report['columns']['valid']:
        missing = report['columns']['missing']
        # Filtrer les colonnes qui sont dans feature_columns
        critical_missing = [c for c in missing if c in feature_columns]
        if critical_missing:
            raise ValueError(f"Colonnes manquantes: {critical_missing}")

    # Nettoyage
    df_clean = clean_dataframe(df, include_target=False)

    # Selection des features disponibles
    available = [f for f in feature_columns if f in df_clean.columns]
    X = df_clean[available].copy()

    # Remplir les valeurs manquantes
    X = X.fillna(X.median() if len(X) > 1 else 0)

    # Ajouter les colonnes manquantes avec valeur 0
    for col in feature_columns:
        if col not in X.columns:
            X[col] = 0

    # Reordonner selon l'ordre attendu
    X = X[feature_columns]

    return X


def predict(
    model_data: dict,
    data: dict or pd.DataFrame,
    return_confidence: bool = False
) -> np.ndarray:
    """
    Effectue des predictions avec le modele entraine.

    Parameters
    ----------
    model_data : dict
        Donnees du modele (pipeline + metadonnees).
    data : dict or pd.DataFrame
        Donnees d'entree pour la prediction.
    return_confidence : bool, optional
        Si True, retourne aussi un intervalle de confiance estime.
        Par defaut False.

    Returns
    -------
    np.ndarray
        Predictions du tarif total.
    """
    pipeline = model_data['pipeline']
    feature_columns = model_data['feature_columns']

    # Preparation des donnees
    X = prepare_input(data, feature_columns)

    # Prediction
    predictions = pipeline.predict(X)

    # Assurer des predictions non negatives
    predictions = np.maximum(predictions, 0)

    if return_confidence:
        # Estimation de l'incertitude basee sur les metriques d'entrainement
        metrics = model_data.get('metrics', {})
        rmse = metrics.get('test', {}).get('rmse', 5.0)
        confidence_interval = 1.96 * rmse  # Intervalle 95%
        return predictions, confidence_interval

    return predictions


def predict_from_file(
    model_data: dict,
    input_path: str,
    output_path: str = None
) -> pd.DataFrame:
    """
    Effectue des predictions sur un fichier Parquet.

    Parameters
    ----------
    model_data : dict
        Donnees du modele.
    input_path : str
        Chemin vers le fichier d'entree.
    output_path : str, optional
        Chemin vers le fichier de sortie. Si None, affiche les resultats.

    Returns
    -------
    pd.DataFrame
        DataFrame avec les predictions.
    """
    print(f"[INFO] Chargement des donnees: {input_path}")
    df = pd.read_parquet(input_path)
    print(f"[INFO] Nombre de lignes: {len(df)}")

    # Predictions
    predictions, confidence = predict(model_data, df, return_confidence=True)
    df['predicted_total_amount'] = predictions
    df['confidence_interval'] = confidence

    if output_path:
        df.to_parquet(output_path, index=False)
        print(f"[INFO] Resultats sauvegardes: {output_path}")

    return df


def interactive_prediction(model_data: dict) -> None:
    """
    Mode de prediction interactif.

    Parameters
    ----------
    model_data : dict
        Donnees du modele.
    """
    print("\n" + "=" * 50)
    print("Mode Prediction Interactive")
    print("=" * 50)
    print("Entrez les caracteristiques du trajet:")
    print("(Ctrl+C pour quitter)\n")

    try:
        while True:
            data = {}

            # Collecte des informations
            data['trip_distance'] = float(input("Distance du trajet (miles): "))
            data['passenger_count'] = int(input("Nombre de passagers: "))
            data['PULocationID'] = int(input("Zone de pickup (1-265): "))
            data['DOLocationID'] = int(input("Zone de dropoff (1-265): "))
            data['RatecodeID'] = int(input("Code tarif (1-6): "))

            use_current_time = input("Utiliser l'heure actuelle? (O/n): ").lower()
            if use_current_time != 'n':
                now = datetime.now()
                data['hour'] = now.hour
                data['day_of_week'] = now.weekday()
                data['month'] = now.month
            else:
                data['hour'] = int(input("Heure (0-23): "))
                data['day_of_week'] = int(input("Jour de la semaine (0=Lun, 6=Dim): "))
                data['month'] = int(input("Mois (1-12): "))

            # Prediction
            predictions, confidence = predict(model_data, data, return_confidence=True)

            print("\n" + "-" * 30)
            print(f"Tarif predit: ${predictions[0]:.2f}")
            print(f"Intervalle de confiance (95%): +/- ${confidence:.2f}")
            print(f"Plage: ${max(0, predictions[0] - confidence):.2f} - ${predictions[0] + confidence:.2f}")
            print("-" * 30 + "\n")

    except KeyboardInterrupt:
        print("\n\nFin du mode interactif.")


def main():
    """Point d'entree principal du script d'inference."""
    parser = argparse.ArgumentParser(
        description="Inference du modele ML NYC Taxi Fare Prediction"
    )
    parser.add_argument(
        '--model',
        type=str,
        default=None,
        help="Chemin vers le modele (defaut: models/latest_model.joblib)"
    )
    parser.add_argument(
        '--input',
        type=str,
        default=None,
        help="Fichier Parquet d'entree pour predictions batch"
    )
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help="Fichier Parquet de sortie pour les predictions"
    )
    parser.add_argument(
        '--json',
        type=str,
        default=None,
        help="Donnees JSON pour une prediction unique"
    )
    parser.add_argument(
        '--interactive',
        action='store_true',
        help="Mode prediction interactive"
    )

    args = parser.parse_args()

    print("=" * 50)
    print("NYC Taxi Fare Prediction - Inference")
    print("=" * 50)

    try:
        # Chargement du modele
        model_data = load_model(args.model)

        if args.interactive:
            interactive_prediction(model_data)

        elif args.json:
            # Prediction depuis JSON
            data = json.loads(args.json)
            predictions, confidence = predict(model_data, data, return_confidence=True)
            print(f"\n[PREDICTION] Tarif: ${predictions[0]:.2f} (+/- ${confidence:.2f})")

        elif args.input:
            # Prediction batch
            result_df = predict_from_file(model_data, args.input, args.output)

            # Afficher quelques statistiques
            print("\n[STATISTIQUES DES PREDICTIONS]")
            print(f"  Moyenne: ${result_df['predicted_total_amount'].mean():.2f}")
            print(f"  Mediane: ${result_df['predicted_total_amount'].median():.2f}")
            print(f"  Min: ${result_df['predicted_total_amount'].min():.2f}")
            print(f"  Max: ${result_df['predicted_total_amount'].max():.2f}")

        else:
            print("[INFO] Aucune entree specifiee. Utilisez --help pour l'aide.")
            parser.print_help()
            return 1

        print("\n[SUCCESS] Inference terminee!")
        return 0

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
