"""
Tests unitaires pour le module de validation des donnees.

Ce module teste les fonctions de validation utilisees lors de
l'entrainement et de l'inference du modele ML.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime

import sys
from pathlib import Path

# Ajouter le repertoire src au path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.data_validation import (
    validate_columns,
    validate_data_types,
    validate_value_ranges,
    validate_no_nulls,
    validate_dataframe,
    clean_dataframe,
    REQUIRED_COLUMNS,
    TARGET_COLUMN,
    VALIDATION_RULES
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def valid_dataframe():
    """Cree un DataFrame valide pour les tests."""
    return pd.DataFrame({
        'trip_distance': [2.5, 3.0, 5.2, 1.0],
        'passenger_count': [1, 2, 3, 1],
        'PULocationID': [100, 150, 200, 50],
        'DOLocationID': [200, 180, 100, 150],
        'RatecodeID': [1, 1, 2, 1],
        'tpep_pickup_datetime': pd.to_datetime([
            '2024-01-01 10:00:00',
            '2024-01-01 14:30:00',
            '2024-01-02 08:15:00',
            '2024-01-02 22:00:00'
        ]),
        'total_amount': [15.0, 20.0, 35.0, 12.0]
    })


@pytest.fixture
def dataframe_missing_columns():
    """Cree un DataFrame avec des colonnes manquantes."""
    return pd.DataFrame({
        'trip_distance': [2.5, 3.0],
        'passenger_count': [1, 2]
    })


@pytest.fixture
def dataframe_invalid_types():
    """Cree un DataFrame avec des types incorrects."""
    return pd.DataFrame({
        'trip_distance': ['abc', 'def'],  # Devrait etre numerique
        'passenger_count': [1, 2],
        'PULocationID': [100, 150],
        'DOLocationID': [200, 180],
        'RatecodeID': [1, 1],
        'tpep_pickup_datetime': ['2024-01-01', '2024-01-02'],
        'total_amount': [15.0, 20.0]
    })


@pytest.fixture
def dataframe_out_of_range():
    """Cree un DataFrame avec des valeurs hors plage."""
    return pd.DataFrame({
        'trip_distance': [2.5, 150.0],  # 150 > max 100
        'passenger_count': [1, 15],  # 15 > max 9
        'PULocationID': [100, 300],  # 300 > max 265
        'DOLocationID': [200, 0],  # 0 < min 1
        'RatecodeID': [1, 10],  # 10 > max 6
        'tpep_pickup_datetime': pd.to_datetime(['2024-01-01', '2024-01-02']),
        'total_amount': [15.0, 600.0]  # 600 > max 500
    })


@pytest.fixture
def dataframe_with_nulls():
    """Cree un DataFrame avec des valeurs nulles."""
    return pd.DataFrame({
        'trip_distance': [2.5, None, 5.2],
        'passenger_count': [1, 2, None],
        'PULocationID': [100, 150, 200],
        'DOLocationID': [200, None, 100],
        'RatecodeID': [1, 1, 2],
        'tpep_pickup_datetime': pd.to_datetime([
            '2024-01-01', '2024-01-02', '2024-01-03'
        ]),
        'total_amount': [15.0, 20.0, 35.0]
    })


# =============================================================================
# Tests validate_columns
# =============================================================================

class TestValidateColumns:
    """Tests pour la fonction validate_columns."""

    def test_valid_dataframe(self, valid_dataframe):
        """Test avec un DataFrame valide."""
        is_valid, missing = validate_columns(valid_dataframe, include_target=True)
        assert is_valid is True
        assert missing == []

    def test_missing_columns(self, dataframe_missing_columns):
        """Test avec des colonnes manquantes."""
        is_valid, missing = validate_columns(
            dataframe_missing_columns, include_target=True
        )
        assert is_valid is False
        assert 'PULocationID' in missing
        assert 'total_amount' in missing

    def test_without_target(self, valid_dataframe):
        """Test sans la colonne cible."""
        df = valid_dataframe.drop(columns=['total_amount'])
        is_valid, missing = validate_columns(df, include_target=False)
        assert is_valid is True
        assert missing == []

    def test_empty_dataframe(self):
        """Test avec un DataFrame vide."""
        df = pd.DataFrame()
        is_valid, missing = validate_columns(df, include_target=True)
        assert is_valid is False
        assert len(missing) > 0


# =============================================================================
# Tests validate_data_types
# =============================================================================

class TestValidateDataTypes:
    """Tests pour la fonction validate_data_types."""

    def test_valid_types(self, valid_dataframe):
        """Test avec des types valides."""
        is_valid, errors = validate_data_types(valid_dataframe)
        assert is_valid is True
        assert errors == {}

    def test_invalid_types(self, dataframe_invalid_types):
        """Test avec des types invalides."""
        is_valid, errors = validate_data_types(dataframe_invalid_types)
        assert is_valid is False
        assert 'trip_distance' in errors


# =============================================================================
# Tests validate_value_ranges
# =============================================================================

class TestValidateValueRanges:
    """Tests pour la fonction validate_value_ranges."""

    def test_valid_ranges(self, valid_dataframe):
        """Test avec des valeurs dans les plages valides."""
        is_valid, violations = validate_value_ranges(valid_dataframe)
        assert is_valid is True
        assert violations == {}

    def test_out_of_range_values(self, dataframe_out_of_range):
        """Test avec des valeurs hors plage."""
        is_valid, violations = validate_value_ranges(dataframe_out_of_range)
        assert is_valid is False
        assert 'trip_distance' in violations
        assert 'passenger_count' in violations
        assert 'PULocationID' in violations

    def test_negative_distance(self):
        """Test avec une distance negative."""
        df = pd.DataFrame({
            'trip_distance': [-1.0, 5.0],
            'passenger_count': [1, 2],
            'PULocationID': [100, 150],
            'DOLocationID': [200, 180],
            'RatecodeID': [1, 1],
            'tpep_pickup_datetime': pd.to_datetime(['2024-01-01', '2024-01-02']),
            'total_amount': [15.0, 20.0]
        })
        is_valid, violations = validate_value_ranges(df)
        assert is_valid is False
        assert 'trip_distance' in violations


# =============================================================================
# Tests validate_no_nulls
# =============================================================================

class TestValidateNoNulls:
    """Tests pour la fonction validate_no_nulls."""

    def test_no_nulls(self, valid_dataframe):
        """Test sans valeurs nulles."""
        is_valid, null_counts = validate_no_nulls(valid_dataframe)
        assert is_valid is True
        assert null_counts == {}

    def test_with_nulls(self, dataframe_with_nulls):
        """Test avec des valeurs nulles."""
        is_valid, null_counts = validate_no_nulls(dataframe_with_nulls)
        assert is_valid is False
        assert 'trip_distance' in null_counts
        assert null_counts['trip_distance'] == 1

    def test_specific_columns(self, dataframe_with_nulls):
        """Test avec des colonnes specifiques."""
        is_valid, null_counts = validate_no_nulls(
            dataframe_with_nulls,
            columns=['PULocationID']
        )
        assert is_valid is True


# =============================================================================
# Tests validate_dataframe
# =============================================================================

class TestValidateDataframe:
    """Tests pour la fonction validate_dataframe."""

    def test_valid_dataframe(self, valid_dataframe):
        """Test avec un DataFrame completement valide."""
        is_valid, report = validate_dataframe(valid_dataframe)
        assert is_valid is True
        assert report['columns']['valid'] is True
        assert report['types']['valid'] is True
        assert report['ranges']['valid'] is True

    def test_strict_mode_columns(self, dataframe_missing_columns):
        """Test du mode strict avec colonnes manquantes."""
        with pytest.raises(ValueError) as excinfo:
            validate_dataframe(dataframe_missing_columns, strict=True)
        assert "Colonnes manquantes" in str(excinfo.value)

    def test_report_structure(self, valid_dataframe):
        """Test de la structure du rapport."""
        is_valid, report = validate_dataframe(valid_dataframe)
        assert 'is_valid' in report
        assert 'columns' in report
        assert 'types' in report
        assert 'ranges' in report
        assert 'nulls' in report
        assert 'row_count' in report
        assert 'warnings' in report


# =============================================================================
# Tests clean_dataframe
# =============================================================================

class TestCleanDataframe:
    """Tests pour la fonction clean_dataframe."""

    def test_clean_valid_dataframe(self, valid_dataframe):
        """Test nettoyage d'un DataFrame deja valide."""
        df_clean = clean_dataframe(valid_dataframe)
        assert len(df_clean) == len(valid_dataframe)

    def test_remove_nulls(self, dataframe_with_nulls):
        """Test suppression des valeurs nulles."""
        df_clean = clean_dataframe(dataframe_with_nulls)
        assert df_clean['trip_distance'].isnull().sum() == 0
        assert len(df_clean) < len(dataframe_with_nulls)

    def test_filter_out_of_range(self, dataframe_out_of_range):
        """Test filtrage des valeurs hors plage."""
        df_clean = clean_dataframe(dataframe_out_of_range)
        assert len(df_clean) < len(dataframe_out_of_range)
        # Verifier que les valeurs restantes sont dans les plages
        if len(df_clean) > 0:
            assert df_clean['trip_distance'].max() <= 100.0

    def test_preserve_valid_rows(self, valid_dataframe):
        """Test preservation des lignes valides."""
        df_clean = clean_dataframe(valid_dataframe)
        assert len(df_clean) == len(valid_dataframe)
        # Verifier que les valeurs sont preservees
        assert df_clean['trip_distance'].iloc[0] == valid_dataframe['trip_distance'].iloc[0]


# =============================================================================
# Tests d'integration pour l'inference
# =============================================================================

class TestInferenceValidation:
    """Tests de validation pour les donnees d'inference."""

    def test_inference_without_target(self, valid_dataframe):
        """Test validation pour inference (sans colonne cible)."""
        df = valid_dataframe.drop(columns=['total_amount'])
        is_valid, report = validate_dataframe(df, include_target=False)
        assert is_valid is True

    def test_single_row_validation(self):
        """Test validation d'une seule ligne (cas d'inference)."""
        df = pd.DataFrame([{
            'trip_distance': 5.0,
            'passenger_count': 2,
            'PULocationID': 100,
            'DOLocationID': 200,
            'RatecodeID': 1,
            'tpep_pickup_datetime': datetime.now()
        }])
        is_valid, report = validate_dataframe(df, include_target=False)
        assert is_valid is True

    def test_edge_case_values(self):
        """Test avec des valeurs limites."""
        df = pd.DataFrame({
            'trip_distance': [0.0, 100.0],  # Valeurs limites
            'passenger_count': [0, 9],
            'PULocationID': [1, 265],
            'DOLocationID': [1, 265],
            'RatecodeID': [1, 6],
            'tpep_pickup_datetime': pd.to_datetime(['2024-01-01', '2024-01-02']),
            'total_amount': [0.0, 500.0]
        })
        is_valid, report = validate_dataframe(df)
        assert is_valid is True


# =============================================================================
# Tests de robustesse
# =============================================================================

class TestRobustness:
    """Tests de robustesse du module de validation."""

    def test_large_dataframe(self):
        """Test avec un grand DataFrame."""
        n = 100000
        df = pd.DataFrame({
            'trip_distance': np.random.uniform(0, 50, n),
            'passenger_count': np.random.randint(1, 6, n),
            'PULocationID': np.random.randint(1, 265, n),
            'DOLocationID': np.random.randint(1, 265, n),
            'RatecodeID': np.random.randint(1, 6, n),
            'tpep_pickup_datetime': pd.date_range('2024-01-01', periods=n, freq='min'),
            'total_amount': np.random.uniform(5, 100, n)
        })
        is_valid, report = validate_dataframe(df)
        assert report['row_count'] == n

    def test_unicode_in_dataframe(self, valid_dataframe):
        """Test avec des caracteres unicode (ne devrait pas affecter)."""
        df = valid_dataframe.copy()
        df['note'] = ['Test unicode: cafe', 'Another: resume', 'More', 'Data']
        is_valid, report = validate_dataframe(df)
        assert is_valid is True

    def test_mixed_dtypes_handling(self):
        """Test avec des types mixtes dans les colonnes."""
        df = pd.DataFrame({
            'trip_distance': [2.5, 3, '4.5', 5.0],  # Mix de types
            'passenger_count': [1, 2, 3, 1],
            'PULocationID': [100, 150, 200, 50],
            'DOLocationID': [200, 180, 100, 150],
            'RatecodeID': [1, 1, 2, 1],
            'tpep_pickup_datetime': pd.to_datetime([
                '2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04'
            ]),
            'total_amount': [15.0, 20.0, 35.0, 12.0]
        })
        # La validation devrait detecter le probleme de type
        is_valid, type_errors = validate_data_types(df)
        # Le type sera 'object' a cause du string '4.5'
        assert 'trip_distance' in type_errors or is_valid is False
