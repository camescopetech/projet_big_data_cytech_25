#!/usr/bin/env python3
"""
Script d'entrainement du modele ML pour la prediction des tarifs NYC Taxi.

Ce script charge les donnees depuis MinIO ou un fichier local,
entraine un modele de regression et sauvegarde le modele entraine.

Notes
-----
L'entrainement utilise un modele GradientBoostingRegressor avec
optimisation des hyperparametres via validation croisee.

Examples
--------
Entrainement depuis MinIO:
    $ uv run python src/train.py --source minio

Entrainement depuis fichier local:
    $ uv run python src/train.py --source local --data-path data/raw/
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
import joblib
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import (
    mean_squared_error,
    mean_absolute_error,
    r2_score
)

# Ajouter le repertoire parent au path
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.data_validation import (
    validate_dataframe,
    clean_dataframe,
    TARGET_COLUMN
)


# Configuration
FEATURE_COLUMNS = [
    'trip_distance',
    'passenger_count',
    'PULocationID',
    'DOLocationID',
    'RatecodeID',
    'hour',
    'day_of_week',
    'month',
]

MODEL_DIR = Path(__file__).parent.parent / "models"
DATA_DIR = Path(__file__).parent.parent.parent / "ex01_data_retrieval" / "data" / "raw"


def load_data_from_local(
    data_path: str = None,
    sample_size: int = None
) -> pd.DataFrame:
    """
    Charge les donnees depuis des fichiers Parquet locaux.

    Parameters
    ----------
    data_path : str, optional
        Chemin vers le repertoire contenant les fichiers Parquet.
        Par defaut, utilise DATA_DIR.
    sample_size : int, optional
        Nombre de lignes a echantillonner par fichier.
        Si None, charge toutes les donnees.

    Returns
    -------
    pd.DataFrame
        DataFrame contenant toutes les donnees concatenees.

    Raises
    ------
    FileNotFoundError
        Si aucun fichier Parquet n'est trouve.
    """
    if data_path is None:
        data_path = DATA_DIR
    else:
        data_path = Path(data_path)

    parquet_files = list(data_path.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"Aucun fichier Parquet trouve dans {data_path}")

    print(f"[INFO] Chargement de {len(parquet_files)} fichier(s) Parquet...")
    dfs = []
    for pf in parquet_files:
        print(f"  - {pf.name}")
        df = pd.read_parquet(pf)
        if sample_size and len(df) > sample_size:
            df = df.sample(n=sample_size, random_state=42)
            print(f"    (echantillonne a {sample_size} lignes)")
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)


def load_data_from_minio(
    bucket_name: str = "nyc-cleaned",
    endpoint: str = "localhost:9000",
    access_key: str = "minio",
    secret_key: str = "minio123"
) -> pd.DataFrame:
    """
    Charge les donnees depuis un bucket MinIO.

    Parameters
    ----------
    bucket_name : str, optional
        Nom du bucket MinIO. Par defaut "nyc-cleaned".
    endpoint : str, optional
        Endpoint MinIO. Par defaut "localhost:9000".
    access_key : str, optional
        Cle d'acces MinIO. Par defaut "minio".
    secret_key : str, optional
        Cle secrete MinIO. Par defaut "minio123".

    Returns
    -------
    pd.DataFrame
        DataFrame contenant toutes les donnees concatenees.
    """
    from minio import Minio
    from io import BytesIO

    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )

    print(f"[INFO] Connexion a MinIO ({endpoint})...")
    objects = client.list_objects(bucket_name, recursive=True)

    dfs = []
    for obj in objects:
        if obj.object_name.endswith('.parquet'):
            print(f"  - {obj.object_name}")
            response = client.get_object(bucket_name, obj.object_name)
            data = BytesIO(response.read())
            df = pd.read_parquet(data)
            dfs.append(df)
            response.close()
            response.release_conn()

    if not dfs:
        raise FileNotFoundError(f"Aucun fichier Parquet trouve dans le bucket {bucket_name}")

    return pd.concat(dfs, ignore_index=True)


def extract_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extrait les features temporelles de la colonne datetime.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame avec colonne 'tpep_pickup_datetime'.

    Returns
    -------
    pd.DataFrame
        DataFrame avec les nouvelles colonnes temporelles.
    """
    df = df.copy()

    if 'tpep_pickup_datetime' in df.columns:
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['hour'] = df['tpep_pickup_datetime'].dt.hour
        df['day_of_week'] = df['tpep_pickup_datetime'].dt.dayofweek
        df['month'] = df['tpep_pickup_datetime'].dt.month

    return df


def prepare_features(df: pd.DataFrame) -> tuple:
    """
    Prepare les features et la cible pour l'entrainement.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame brut.

    Returns
    -------
    tuple
        (X, y) avec X les features et y la cible.
    """
    # Validation et nettoyage
    is_valid, report = validate_dataframe(df, include_target=True, strict=False)
    if not is_valid:
        print(f"[WARN] Donnees invalides: {report}")

    df_clean = clean_dataframe(df, include_target=True)
    print(f"[INFO] Donnees apres nettoyage: {len(df_clean)} lignes")

    # Extraction des features temporelles
    df_clean = extract_time_features(df_clean)

    # Selection des features
    available_features = [f for f in FEATURE_COLUMNS if f in df_clean.columns]
    X = df_clean[available_features].copy()
    y = df_clean[TARGET_COLUMN].copy()

    # Remplir les valeurs manquantes restantes
    X = X.fillna(X.median())

    return X, y, available_features


def train_model(X: pd.DataFrame, y: pd.Series) -> tuple:
    """
    Entraine le modele Gradient Boosting Regressor.

    Parameters
    ----------
    X : pd.DataFrame
        Features d'entrainement.
    y : pd.Series
        Cible.

    Returns
    -------
    tuple
        (pipeline, metrics) avec le modele entraine et les metriques.
    """
    print("[INFO] Division train/test (80/20)...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    print("[INFO] Entrainement du modele (Gradient Boosting)...")

    # Modele Gradient Boosting Regressor
    model = GradientBoostingRegressor(
        n_estimators=100,
        max_depth=5,
        learning_rate=0.1,
        min_samples_split=10,
        min_samples_leaf=5,
        random_state=42,
        verbose=0
    )

    # Pipeline avec normalisation
    pipeline = Pipeline([
        ('scaler', StandardScaler()),
        ('model', model)
    ])

    # Entrainement
    pipeline.fit(X_train, y_train)

    # Predictions
    y_pred_train = pipeline.predict(X_train)
    y_pred_test = pipeline.predict(X_test)

    # Metriques
    metrics = {
        'train': {
            'rmse': np.sqrt(mean_squared_error(y_train, y_pred_train)),
            'mae': mean_absolute_error(y_train, y_pred_train),
            'r2': r2_score(y_train, y_pred_train)
        },
        'test': {
            'rmse': np.sqrt(mean_squared_error(y_test, y_pred_test)),
            'mae': mean_absolute_error(y_test, y_pred_test),
            'r2': r2_score(y_test, y_pred_test)
        }
    }

    # Validation croisee
    print("[INFO] Validation croisee (5 folds)...")
    cv_scores = cross_val_score(
        pipeline, X, y,
        cv=5,
        scoring='neg_root_mean_squared_error',
        n_jobs=-1
    )
    metrics['cv_rmse_mean'] = -cv_scores.mean()
    metrics['cv_rmse_std'] = cv_scores.std()

    return pipeline, metrics


def save_model(
    pipeline: Pipeline,
    feature_columns: list,
    metrics: dict,
    model_name: str = None
) -> str:
    """
    Sauvegarde le modele entraine et ses metadonnees.

    Parameters
    ----------
    pipeline : Pipeline
        Pipeline scikit-learn entraine.
    feature_columns : list
        Liste des colonnes de features utilisees.
    metrics : dict
        Metriques d'evaluation.
    model_name : str, optional
        Nom du modele. Si None, genere un nom avec timestamp.

    Returns
    -------
    str
        Chemin vers le modele sauvegarde.
    """
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    if model_name is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        model_name = f"nyc_taxi_fare_model_{timestamp}"

    model_path = MODEL_DIR / f"{model_name}.joblib"

    # Sauvegarder le modele et les metadonnees
    model_data = {
        'pipeline': pipeline,
        'feature_columns': feature_columns,
        'metrics': metrics,
        'created_at': datetime.now().isoformat(),
        'version': '1.0.0'
    }

    joblib.dump(model_data, model_path)
    print(f"[INFO] Modele sauvegarde: {model_path}")

    # Sauvegarder aussi comme "latest"
    latest_path = MODEL_DIR / "latest_model.joblib"
    joblib.dump(model_data, latest_path)
    print(f"[INFO] Modele copie vers: {latest_path}")

    return str(model_path)


def print_metrics(metrics: dict) -> None:
    """
    Affiche les metriques de maniere formatee.

    Parameters
    ----------
    metrics : dict
        Dictionnaire des metriques.
    """
    print("\n" + "=" * 50)
    print("RESULTATS DE L'ENTRAINEMENT")
    print("=" * 50)

    print("\n[TRAIN]")
    print(f"  RMSE: {metrics['train']['rmse']:.4f}")
    print(f"  MAE:  {metrics['train']['mae']:.4f}")
    print(f"  R2:   {metrics['train']['r2']:.4f}")

    print("\n[TEST]")
    print(f"  RMSE: {metrics['test']['rmse']:.4f}")
    print(f"  MAE:  {metrics['test']['mae']:.4f}")
    print(f"  R2:   {metrics['test']['r2']:.4f}")

    print("\n[VALIDATION CROISEE]")
    print(f"  RMSE Moyen: {metrics['cv_rmse_mean']:.4f} (+/- {metrics['cv_rmse_std']:.4f})")

    print("\n" + "=" * 50)

    # Verification du seuil RMSE
    if metrics['test']['rmse'] < 10:
        print("[OK] RMSE < 10 : Objectif atteint !")
    else:
        print(f"[WARN] RMSE = {metrics['test']['rmse']:.4f} >= 10 : Objectif non atteint")

    print("=" * 50)


def main():
    """Point d'entree principal du script d'entrainement."""
    parser = argparse.ArgumentParser(
        description="Entrainement du modele ML NYC Taxi Fare Prediction"
    )
    parser.add_argument(
        '--source',
        choices=['local', 'minio'],
        default='local',
        help="Source des donnees: 'local' ou 'minio' (defaut: local)"
    )
    parser.add_argument(
        '--data-path',
        type=str,
        default=None,
        help="Chemin vers les fichiers Parquet (si source=local)"
    )
    parser.add_argument(
        '--model-name',
        type=str,
        default=None,
        help="Nom du modele sauvegarde"
    )
    parser.add_argument(
        '--minio-endpoint',
        type=str,
        default='localhost:9000',
        help="Endpoint MinIO (defaut: localhost:9000)"
    )
    parser.add_argument(
        '--minio-bucket',
        type=str,
        default='nyc-cleaned',
        help="Bucket MinIO (defaut: nyc-cleaned)"
    )
    parser.add_argument(
        '--sample-size',
        type=int,
        default=100000,
        help="Nombre de lignes a echantillonner par fichier (defaut: 100000, 0=tout)"
    )

    args = parser.parse_args()

    print("=" * 50)
    print("NYC Taxi Fare Prediction - Entrainement")
    print("=" * 50)

    try:
        # Chargement des donnees
        sample_size = args.sample_size if args.sample_size > 0 else None
        if args.source == 'minio':
            df = load_data_from_minio(
                bucket_name=args.minio_bucket,
                endpoint=args.minio_endpoint
            )
            if sample_size:
                df = df.sample(n=min(sample_size * 4, len(df)), random_state=42)
        else:
            df = load_data_from_local(args.data_path, sample_size=sample_size)

        print(f"[INFO] Donnees chargees: {len(df)} lignes")

        # Preparation des features
        X, y, feature_columns = prepare_features(df)
        print(f"[INFO] Features: {feature_columns}")

        # Entrainement
        pipeline, metrics = train_model(X, y)

        # Affichage des metriques
        print_metrics(metrics)

        # Sauvegarde
        model_path = save_model(
            pipeline, feature_columns, metrics, args.model_name
        )

        print("\n[SUCCESS] Entrainement termine avec succes!")
        print(f"Modele: {model_path}")

        return 0

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
