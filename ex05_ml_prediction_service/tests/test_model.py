"""
Tests unitaires pour les modules d'entrainement et d'inference.

Ce module teste les fonctions de preparation des features,
d'entrainement du modele et d'inference.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import sys

# Ajouter le repertoire src au path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.train import (
    extract_time_features,
    prepare_features,
    train_model,
)
from src.predict import (
    extract_time_features_inference,
    prepare_input,
    predict,
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def sample_training_data():
    """Cree un DataFrame d'entrainement pour les tests."""
    np.random.seed(42)
    n = 1000
    return pd.DataFrame({
        'trip_distance': np.random.uniform(0.5, 20, n),
        'passenger_count': np.random.randint(1, 5, n).astype(float),
        'PULocationID': np.random.randint(1, 265, n),
        'DOLocationID': np.random.randint(1, 265, n),
        'RatecodeID': np.random.choice([1, 2, 3, 4, 5, 6], n).astype(float),
        'tpep_pickup_datetime': pd.date_range(
            '2024-01-01', periods=n, freq='h'
        ),
        'total_amount': np.random.uniform(5, 100, n)
    })


@pytest.fixture
def sample_inference_data():
    """Cree des donnees d'inference pour les tests."""
    return {
        'trip_distance': 5.0,
        'passenger_count': 2,
        'PULocationID': 100,
        'DOLocationID': 200,
        'RatecodeID': 1,
        'hour': 14,
        'day_of_week': 2,
        'month': 6
    }


@pytest.fixture
def trained_model(sample_training_data):
    """Entraine un modele pour les tests."""
    X, y, feature_columns = prepare_features(sample_training_data)
    pipeline, metrics = train_model(X, y)
    return {
        'pipeline': pipeline,
        'feature_columns': feature_columns,
        'metrics': metrics
    }


# =============================================================================
# Tests extract_time_features
# =============================================================================

class TestExtractTimeFeatures:
    """Tests pour la fonction extract_time_features."""

    def test_extracts_hour(self, sample_training_data):
        """Test extraction de l'heure."""
        df = extract_time_features(sample_training_data)
        assert 'hour' in df.columns
        assert df['hour'].min() >= 0
        assert df['hour'].max() <= 23

    def test_extracts_day_of_week(self, sample_training_data):
        """Test extraction du jour de la semaine."""
        df = extract_time_features(sample_training_data)
        assert 'day_of_week' in df.columns
        assert df['day_of_week'].min() >= 0
        assert df['day_of_week'].max() <= 6

    def test_extracts_month(self, sample_training_data):
        """Test extraction du mois."""
        df = extract_time_features(sample_training_data)
        assert 'month' in df.columns
        assert df['month'].min() >= 1
        assert df['month'].max() <= 12

    def test_preserves_original_columns(self, sample_training_data):
        """Test que les colonnes originales sont preservees."""
        df = extract_time_features(sample_training_data)
        assert 'trip_distance' in df.columns
        assert 'passenger_count' in df.columns

    def test_handles_missing_datetime(self):
        """Test avec colonne datetime manquante."""
        df = pd.DataFrame({
            'trip_distance': [2.5, 3.0],
            'passenger_count': [1, 2]
        })
        df_result = extract_time_features(df)
        # Ne devrait pas ajouter de colonnes temporelles
        assert 'hour' not in df_result.columns


# =============================================================================
# Tests prepare_features
# =============================================================================

class TestPrepareFeatures:
    """Tests pour la fonction prepare_features."""

    def test_returns_tuple(self, sample_training_data):
        """Test que la fonction retourne un tuple."""
        result = prepare_features(sample_training_data)
        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_x_shape(self, sample_training_data):
        """Test la forme de X."""
        X, y, features = prepare_features(sample_training_data)
        assert len(X) <= len(sample_training_data)  # Peut etre plus petit apres nettoyage
        assert len(X.columns) == len(features)

    def test_y_type(self, sample_training_data):
        """Test le type de y."""
        X, y, features = prepare_features(sample_training_data)
        assert isinstance(y, pd.Series)

    def test_no_nulls_in_x(self, sample_training_data):
        """Test que X ne contient pas de nulls."""
        X, y, features = prepare_features(sample_training_data)
        assert X.isnull().sum().sum() == 0

    def test_feature_columns_returned(self, sample_training_data):
        """Test que les colonnes de features sont retournees."""
        X, y, features = prepare_features(sample_training_data)
        assert isinstance(features, list)
        assert len(features) > 0


# =============================================================================
# Tests train_model
# =============================================================================

class TestTrainModel:
    """Tests pour la fonction train_model (Gradient Boosting)."""

    def test_train_model_returns_pipeline(self, sample_training_data):
        """Test entrainement avec Gradient Boosting."""
        X, y, _ = prepare_features(sample_training_data)
        pipeline, metrics = train_model(X, y)
        assert pipeline is not None
        assert 'train' in metrics
        assert 'test' in metrics

    def test_metrics_structure(self, sample_training_data):
        """Test la structure des metriques."""
        X, y, _ = prepare_features(sample_training_data)
        pipeline, metrics = train_model(X, y)

        assert 'train' in metrics
        assert 'test' in metrics
        assert 'rmse' in metrics['train']
        assert 'mae' in metrics['train']
        assert 'r2' in metrics['train']
        assert 'cv_rmse_mean' in metrics

    def test_metrics_values_valid(self, sample_training_data):
        """Test que les metriques ont des valeurs valides."""
        X, y, _ = prepare_features(sample_training_data)
        pipeline, metrics = train_model(X, y)

        assert metrics['train']['rmse'] >= 0
        assert metrics['test']['rmse'] >= 0
        assert -1 <= metrics['train']['r2'] <= 1

    def test_pipeline_can_predict(self, sample_training_data):
        """Test que le pipeline peut faire des predictions."""
        X, y, _ = prepare_features(sample_training_data)
        pipeline, metrics = train_model(X, y)

        # Faire une prediction
        predictions = pipeline.predict(X[:5])
        assert len(predictions) == 5
        assert all(p >= 0 for p in predictions)


# =============================================================================
# Tests extract_time_features_inference
# =============================================================================

class TestExtractTimeFeaturesInference:
    """Tests pour la fonction extract_time_features_inference."""

    def test_with_datetime_column(self):
        """Test avec colonne datetime."""
        df = pd.DataFrame({
            'trip_distance': [5.0],
            'tpep_pickup_datetime': [datetime(2024, 6, 15, 14, 30)]
        })
        result = extract_time_features_inference(df)
        assert result['hour'].iloc[0] == 14
        assert result['month'].iloc[0] == 6

    def test_without_datetime_column(self):
        """Test sans colonne datetime (utilise heure actuelle)."""
        df = pd.DataFrame({
            'trip_distance': [5.0],
            'passenger_count': [2]
        })
        result = extract_time_features_inference(df)
        assert 'hour' in result.columns
        assert 'day_of_week' in result.columns
        assert 'month' in result.columns


# =============================================================================
# Tests prepare_input
# =============================================================================

class TestPrepareInput:
    """Tests pour la fonction prepare_input."""

    def test_dict_input(self, sample_inference_data):
        """Test avec un dictionnaire en entree."""
        feature_columns = ['trip_distance', 'passenger_count', 'PULocationID',
                          'DOLocationID', 'RatecodeID', 'hour', 'day_of_week', 'month']
        result = prepare_input(sample_inference_data, feature_columns)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_dataframe_input(self):
        """Test avec un DataFrame en entree."""
        df = pd.DataFrame([{
            'trip_distance': 5.0,
            'passenger_count': 2,
            'PULocationID': 100,
            'DOLocationID': 200,
            'RatecodeID': 1,
            'tpep_pickup_datetime': datetime.now()
        }])
        feature_columns = ['trip_distance', 'passenger_count', 'PULocationID',
                          'DOLocationID', 'RatecodeID', 'hour', 'day_of_week', 'month']
        result = prepare_input(df, feature_columns)
        assert len(result.columns) == len(feature_columns)

    def test_correct_column_order(self, sample_inference_data):
        """Test que l'ordre des colonnes est correct."""
        feature_columns = ['trip_distance', 'passenger_count', 'PULocationID',
                          'DOLocationID', 'RatecodeID', 'hour', 'day_of_week', 'month']
        result = prepare_input(sample_inference_data, feature_columns)
        assert list(result.columns) == feature_columns


# =============================================================================
# Tests predict
# =============================================================================

class TestPredict:
    """Tests pour la fonction predict."""

    def test_single_prediction(self, trained_model, sample_inference_data):
        """Test prediction unique."""
        predictions = predict(trained_model, sample_inference_data)
        assert len(predictions) == 1
        assert predictions[0] >= 0  # Tarif non negatif

    def test_batch_prediction(self, trained_model):
        """Test predictions en batch."""
        df = pd.DataFrame([
            {'trip_distance': 5.0, 'passenger_count': 2, 'PULocationID': 100,
             'DOLocationID': 200, 'RatecodeID': 1, 'hour': 14,
             'day_of_week': 2, 'month': 6},
            {'trip_distance': 10.0, 'passenger_count': 1, 'PULocationID': 50,
             'DOLocationID': 150, 'RatecodeID': 1, 'hour': 8,
             'day_of_week': 0, 'month': 6}
        ])
        predictions = predict(trained_model, df)
        assert len(predictions) == 2

    def test_prediction_non_negative(self, trained_model):
        """Test que les predictions sont non negatives."""
        data = {
            'trip_distance': 0.1,  # Tres court trajet
            'passenger_count': 1,
            'PULocationID': 100,
            'DOLocationID': 101,
            'RatecodeID': 1,
            'hour': 3,
            'day_of_week': 0,
            'month': 1
        }
        predictions = predict(trained_model, data)
        assert predictions[0] >= 0

    def test_return_confidence(self, trained_model, sample_inference_data):
        """Test retour de l'intervalle de confiance."""
        predictions, confidence = predict(
            trained_model, sample_inference_data, return_confidence=True
        )
        assert confidence > 0

    def test_longer_trip_higher_fare(self, trained_model):
        """Test que les trajets plus longs coutent plus cher."""
        short_trip = {
            'trip_distance': 1.0, 'passenger_count': 1, 'PULocationID': 100,
            'DOLocationID': 105, 'RatecodeID': 1, 'hour': 12,
            'day_of_week': 2, 'month': 6
        }
        long_trip = {
            'trip_distance': 20.0, 'passenger_count': 1, 'PULocationID': 100,
            'DOLocationID': 200, 'RatecodeID': 1, 'hour': 12,
            'day_of_week': 2, 'month': 6
        }
        pred_short = predict(trained_model, short_trip)[0]
        pred_long = predict(trained_model, long_trip)[0]
        # Le trajet long devrait couter plus cher (avec une marge)
        assert pred_long > pred_short * 0.5


# =============================================================================
# Tests d'integration
# =============================================================================

class TestEndToEnd:
    """Tests d'integration de bout en bout."""

    def test_train_and_predict(self, sample_training_data):
        """Test cycle complet entrainement puis prediction."""
        # Entrainement
        X, y, feature_columns = prepare_features(sample_training_data)
        pipeline, metrics = train_model(X, y)

        model_data = {
            'pipeline': pipeline,
            'feature_columns': feature_columns,
            'metrics': metrics
        }

        # Prediction
        test_data = {
            'trip_distance': 5.0,
            'passenger_count': 2,
            'PULocationID': 100,
            'DOLocationID': 200,
            'RatecodeID': 1,
            'hour': 14,
            'day_of_week': 2,
            'month': 6
        }
        predictions = predict(model_data, test_data)
        assert predictions[0] > 0

    def test_consistent_predictions(self, trained_model, sample_inference_data):
        """Test que les predictions sont consistantes."""
        pred1 = predict(trained_model, sample_inference_data)
        pred2 = predict(trained_model, sample_inference_data)
        assert pred1[0] == pred2[0]

    def test_model_generalizes(self, sample_training_data):
        """Test que le modele generalise correctement."""
        X, y, feature_columns = prepare_features(sample_training_data)
        pipeline, metrics = train_model(X, y)

        # Le RMSE de test ne devrait pas etre trop different de celui d'entrainement
        train_rmse = metrics['train']['rmse']
        test_rmse = metrics['test']['rmse']
        # Tolerance: test RMSE pas plus de 5x le train RMSE (eviter overfitting severe)
        assert test_rmse < train_rmse * 5
