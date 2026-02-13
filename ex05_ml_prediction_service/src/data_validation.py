"""
Module de validation des données pour le modèle ML NYC Taxi.

Ce module contient les fonctions de validation des données d'entrée
pour l'entrainement et l'inférence du modèle de prediction des tarifs.

Notes
-----
Les règles de validation sont basées sur le contrat de donnees NYC TLC.
"""

import pandas as pd
from typing import Tuple, List, Dict, Any


# Constantes de validation
REQUIRED_COLUMNS = [
    'trip_distance',
    'passenger_count',
    'PULocationID',
    'DOLocationID',
    'RatecodeID',
    'tpep_pickup_datetime',
]

TARGET_COLUMN = 'total_amount'

# Plages de valeurs valides selon NYC TLC
VALIDATION_RULES = {
    'trip_distance': {'min': 0.0, 'max': 100.0},
    'passenger_count': {'min': 0, 'max': 9},
    'PULocationID': {'min': 1, 'max': 265},
    'DOLocationID': {'min': 1, 'max': 265},
    'RatecodeID': {'min': 1, 'max': 6},
    'total_amount': {'min': 0.0, 'max': 500.0},
}


def validate_columns(df: pd.DataFrame, include_target: bool = True) -> Tuple[bool, List[str]]:
    """
    Valide la presence des colonnes requises dans le DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame a valider.
    include_target : bool, optional
        Si True, vérifie aussi la presence de la colonne cible.
        Par défaut True.

    Returns
    -------
    Tuple[bool, List[str]]
        Un tuple contenant:
        - bool: True si toutes les colonnes sont présentes
        - List[str]: Liste des colonnes manquantes
    """
    required = REQUIRED_COLUMNS.copy()
    if include_target:
        required.append(TARGET_COLUMN)

    missing_columns = [col for col in required if col not in df.columns]
    is_valid = len(missing_columns) == 0

    return is_valid, missing_columns


def validate_data_types(df: pd.DataFrame) -> Tuple[bool, Dict[str, str]]:
    """
    Valide les types de données des colonnes.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame à valider.

    Returns
    -------
    Tuple[bool, Dict[str, str]]
        Un tuple contenant:
        - bool: True si tous les types sont corrects
        - Dict[str, str]: Dictionnaire des colonnes avec types incorrects
    """
    type_errors = {}

    numeric_columns = ['trip_distance', 'passenger_count', 'PULocationID',
                       'DOLocationID', 'RatecodeID']

    for col in numeric_columns:
        if col in df.columns:
            if not pd.api.types.is_numeric_dtype(df[col]):
                type_errors[col] = f"Expected numeric, got {df[col].dtype}"

    if TARGET_COLUMN in df.columns:
        if not pd.api.types.is_numeric_dtype(df[TARGET_COLUMN]):
            type_errors[TARGET_COLUMN] = f"Expected numeric, got {df[TARGET_COLUMN].dtype}"

    is_valid = len(type_errors) == 0
    return is_valid, type_errors


def validate_value_ranges(df: pd.DataFrame) -> Tuple[bool, Dict[str, Dict[str, Any]]]:
    """
    Valide que les valeurs sont dans les plages acceptables.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame à valider.

    Returns
    -------
    Tuple[bool, Dict[str, Dict[str, Any]]]
        Un tuple contenant:
        - bool: True si toutes les valeurs sont valides
        - Dict: Dictionnaire des violations par colonne
    """
    violations = {}

    for col, rules in VALIDATION_RULES.items():
        if col not in df.columns:
            continue

        col_data = df[col].dropna()
        if len(col_data) == 0:
            continue

        min_val = col_data.min()
        max_val = col_data.max()

        col_violations = {}
        if min_val < rules['min']:
            col_violations['below_min'] = {
                'expected_min': rules['min'],
                'actual_min': min_val,
                'count': (col_data < rules['min']).sum()
            }
        if max_val > rules['max']:
            col_violations['above_max'] = {
                'expected_max': rules['max'],
                'actual_max': max_val,
                'count': (col_data > rules['max']).sum()
            }

        if col_violations:
            violations[col] = col_violations

    is_valid = len(violations) == 0
    return is_valid, violations


def validate_no_nulls(df: pd.DataFrame, columns: List[str] = None) -> Tuple[bool, Dict[str, int]]:
    """
    Valide l'absence de valeurs nulles dans les colonnes specifiées.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame a valider.
    columns : List[str], optional
        Liste des colonnes a verifier. Si None, verifie toutes les colonnes
        requises. Par défaut None.

    Returns
    -------
    Tuple[bool, Dict[str, int]]
        Un tuple contenant:
        - bool: True si aucune valeur nulle
        - Dict[str, int]: Dictionnaire du nombre de nulls par colonne
    """
    if columns is None:
        columns = REQUIRED_COLUMNS

    null_counts = {}
    for col in columns:
        if col in df.columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                null_counts[col] = int(null_count)

    is_valid = len(null_counts) == 0
    return is_valid, null_counts


def validate_dataframe(
    df: pd.DataFrame,
    include_target: bool = True,
    strict: bool = False
) -> Tuple[bool, Dict[str, Any]]:
    """
    Valide complètement un DataFrame pour l'entrainement ou l'inférence.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame a valider.
    include_target : bool, optional
        Si True, verifie aussi la colonne cible. Par defaut True.
    strict : bool, optional
        Si True, echoue sur toute violation. Si False, genere des
        avertissements pour les violations de plage. Par defaut False.

    Returns
    -------
    Tuple[bool, Dict[str, Any]]
        Un tuple contenant:
        - bool: True si le DataFrame est valide
        - Dict: Rapport complet de validation

    Raises
    ------
    ValueError
        Si strict=True et que des colonnes sont manquantes ou types incorrects.

    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'trip_distance': [2.5, 3.0],
    ...     'passenger_count': [1, 2],
    ...     'PULocationID': [100, 150],
    ...     'DOLocationID': [200, 180],
    ...     'RatecodeID': [1, 1],
    ...     'tpep_pickup_datetime': pd.to_datetime(['2024-01-01', '2024-01-02']),
    ...     'total_amount': [15.0, 20.0]
    ... })
    >>> is_valid, report = validate_dataframe(df)
    >>> print(is_valid)
    True
    """
    report = {
        'is_valid': True,
        'columns': {},
        'types': {},
        'ranges': {},
        'nulls': {},
        'row_count': len(df),
        'warnings': []
    }

    # Validation des colonnes
    cols_valid, missing_cols = validate_columns(df, include_target)
    report['columns'] = {'valid': cols_valid, 'missing': missing_cols}
    if not cols_valid:
        report['is_valid'] = False
        if strict:
            raise ValueError(f"Colonnes manquantes: {missing_cols}")

    # Validation des types
    types_valid, type_errors = validate_data_types(df)
    report['types'] = {'valid': types_valid, 'errors': type_errors}
    if not types_valid:
        report['is_valid'] = False
        if strict:
            raise ValueError(f"Types incorrects: {type_errors}")

    # Validation des plages
    ranges_valid, range_violations = validate_value_ranges(df)
    report['ranges'] = {'valid': ranges_valid, 'violations': range_violations}
    if not ranges_valid:
        if strict:
            report['is_valid'] = False
        else:
            report['warnings'].append(
                f"Valeurs hors plage detectees: {list(range_violations.keys())}"
            )

    # Validation des nulls
    nulls_valid, null_counts = validate_no_nulls(df)
    report['nulls'] = {'valid': nulls_valid, 'counts': null_counts}
    if not nulls_valid:
        report['warnings'].append(f"Valeurs nulles detectees: {null_counts}")

    return report['is_valid'], report


def clean_dataframe(df: pd.DataFrame, include_target: bool = True) -> pd.DataFrame:
    """
    Nettoie un DataFrame en appliquant les regles de validation.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame a nettoyer.
    include_target : bool, optional
        Si True, filtre aussi sur la colonne cible. Par defaut True.

    Returns
    -------
    pd.DataFrame
        DataFrame nettoye.

    Notes
    -----
    Cette fonction:
    - Supprime les lignes avec des valeurs nulles dans les colonnes requises
    - Filtre les valeurs hors plage
    - Convertit les types si necessaire
    """
    df_clean = df.copy()

    # Colonnes a verifier
    columns_to_check = REQUIRED_COLUMNS.copy()
    if include_target:
        columns_to_check.append(TARGET_COLUMN)

    # Supprimer les lignes avec des nulls dans les colonnes importantes
    for col in columns_to_check:
        if col in df_clean.columns:
            df_clean = df_clean.dropna(subset=[col])

    # Filtrer les valeurs hors plage
    for col, rules in VALIDATION_RULES.items():
        if col not in df_clean.columns:
            continue
        if col == TARGET_COLUMN and not include_target:
            continue

        df_clean = df_clean[
            (df_clean[col] >= rules['min']) &
            (df_clean[col] <= rules['max'])
        ]

    return df_clean.reset_index(drop=True)
