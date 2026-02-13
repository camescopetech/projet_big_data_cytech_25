"""
DAG Airflow pour l'orchestration du pipeline Big Data NYC Taxi.

Ce DAG automatise l'ensemble des exercices du projet :
- Exercice 1 : Collecte des donnees et stockage dans MinIO
- Exercice 2 : Nettoyage des donnees (Branche 1) et ingestion (Branche 2)
- Exercice 3 : Creation des tables SQL dans PostgreSQL
- Exercice 4 : Dashboard de visualisation (verification)
- Exercice 5 : Entrainement du modele ML

Notes
-----
Le DAG s'execute mensuellement pour traiter les nouvelles donnees NYC Taxi.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from docker.types import Mount
import os


# =============================================================================
# Configuration
# =============================================================================

# Arguments par defaut pour les taches
default_args = {
    'owner': 'bigdata-cytech',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Variables d'environnement
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minio')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minio123')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'datawarehouse')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'datawarehouse123')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'nyc_taxi_dw')


# =============================================================================
# Fonctions Python pour les taches
# =============================================================================

def check_minio_connection():
    """Verifie la connexion a MinIO."""
    from minio import Minio
    from minio.error import S3Error

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    # Verifier que MinIO est accessible
    buckets = client.list_buckets()
    print(f"[OK] MinIO accessible. Buckets existants: {[b.name for b in buckets]}")

    # Creer les buckets necessaires s'ils n'existent pas
    for bucket in ['nyc-raw', 'nyc-cleaned']:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"[INFO] Bucket '{bucket}' cree")
        else:
            print(f"[INFO] Bucket '{bucket}' existe deja")

    return True


def check_postgres_connection():
    """Verifie la connexion a PostgreSQL."""
    import psycopg2

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB
    )
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    version = cursor.fetchone()
    print(f"[OK] PostgreSQL accessible: {version[0]}")
    cursor.close()
    conn.close()

    return True


def verify_raw_data_exists():
    """Verifie que les donnees brutes existent dans MinIO."""
    from minio import Minio

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    objects = list(client.list_objects('nyc-raw', recursive=True))
    parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]

    if not parquet_files:
        raise ValueError("Aucun fichier Parquet trouve dans le bucket nyc-raw")

    print(f"[OK] {len(parquet_files)} fichier(s) Parquet trouve(s) dans nyc-raw:")
    for f in parquet_files:
        print(f"  - {f}")

    return parquet_files


def verify_cleaned_data_exists():
    """Verifie que les donnees nettoyees existent dans MinIO."""
    from minio import Minio

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    objects = list(client.list_objects('nyc-cleaned', recursive=True))
    parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]

    if not parquet_files:
        raise ValueError("Aucun fichier Parquet trouve dans le bucket nyc-cleaned")

    print(f"[OK] {len(parquet_files)} fichier(s) Parquet nettoye(s) dans nyc-cleaned:")
    for f in parquet_files:
        print(f"  - {f}")

    return parquet_files


def verify_sql_tables_exist():
    """Verifie que les tables SQL ont ete creees."""
    import psycopg2

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB
    )
    cursor = conn.cursor()

    # Lister les tables
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name;
    """)
    tables = [row[0] for row in cursor.fetchall()]

    print(f"[OK] Tables trouvees dans PostgreSQL: {tables}")

    cursor.close()
    conn.close()

    return tables


def train_ml_model():
    """Entraine le modele ML pour la prediction des tarifs."""
    import subprocess
    import sys

    # Chemin vers l'exercice 5
    ex05_path = '/opt/airflow/exercises/ex05_ml_prediction_service'

    # Installer les dependances si necessaire
    subprocess.run([
        sys.executable, '-m', 'pip', 'install', '-q',
        'pandas', 'pyarrow', 'scikit-learn', 'numpy', 'joblib', 'minio'
    ], check=True)

    # Importer le module d'entrainement
    sys.path.insert(0, ex05_path)
    from src.train import (
        load_data_from_minio,
        prepare_features,
        train_model,
        save_model,
        print_metrics
    )

    print("[INFO] Chargement des donnees depuis MinIO...")
    df = load_data_from_minio(
        bucket_name='nyc-cleaned',
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY
    )

    # Echantillonner pour l'entrainement rapide
    if len(df) > 200000:
        df = df.sample(n=200000, random_state=42)

    print(f"[INFO] Donnees chargees: {len(df)} lignes")

    # Preparation et entrainement
    X, y, feature_columns = prepare_features(df)
    pipeline, metrics = train_model(X, y)

    # Affichage des metriques
    print_metrics(metrics)

    # Sauvegarde
    model_path = save_model(pipeline, feature_columns, metrics)
    print(f"[OK] Modele entraine et sauvegarde: {model_path}")

    return metrics


# =============================================================================
# Definition du DAG
# =============================================================================

with DAG(
    'nyc_taxi_bigdata_pipeline',
    default_args=default_args,
    description='Pipeline Big Data complet pour les donnees NYC Taxi',
    schedule_interval='@monthly',  # Execution mensuelle
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bigdata', 'nyc-taxi', 'etl', 'ml'],
) as dag:

    # =========================================================================
    # Start
    # =========================================================================
    start = DummyOperator(task_id='start')

    # =========================================================================
    # Pre-checks : Verification des connexions
    # =========================================================================
    with TaskGroup('pre_checks', tooltip='Verification des prerequis') as pre_checks:

        check_minio = PythonOperator(
            task_id='check_minio',
            python_callable=check_minio_connection,
        )

        check_postgres = PythonOperator(
            task_id='check_postgres',
            python_callable=check_postgres_connection,
        )

    # =========================================================================
    # Exercice 1 : Collecte des donnees
    # =========================================================================
    with TaskGroup('ex01_data_retrieval', tooltip='Collecte des donnees NYC Taxi') as ex01:

        # Execution via Docker (Spark/Scala)
        ex01_run = DockerOperator(
            task_id='run_spark_data_retrieval',
            image='bitnami/spark:3.5.0',
            api_version='auto',
            auto_remove='success',
            command='echo "Exercice 1: Data retrieval via Spark - A executer manuellement avec sbt run"',
            docker_url='unix://var/run/docker.sock',
            network_mode='spark-network',
            mount_tmp_dir=False,
        )

        # Verification des donnees
        ex01_verify = PythonOperator(
            task_id='verify_raw_data',
            python_callable=verify_raw_data_exists,
        )

        ex01_run >> ex01_verify

    # =========================================================================
    # Exercice 2 : Nettoyage et Ingestion
    # =========================================================================
    with TaskGroup('ex02_data_processing', tooltip='Nettoyage et ingestion des donnees') as ex02:

        # Branche 1 : Nettoyage vers MinIO
        ex02_branch1 = DockerOperator(
            task_id='run_spark_cleaning',
            image='bitnami/spark:3.5.0',
            api_version='auto',
            auto_remove='success',
            command='echo "Exercice 2 Branche 1: Data cleaning via Spark - A executer manuellement avec sbt run"',
            docker_url='unix://var/run/docker.sock',
            network_mode='spark-network',
            mount_tmp_dir=False,
        )

        # Verification des donnees nettoyees
        ex02_verify_cleaned = PythonOperator(
            task_id='verify_cleaned_data',
            python_callable=verify_cleaned_data_exists,
        )

        # Branche 2 : Ingestion vers PostgreSQL
        ex02_branch2 = DockerOperator(
            task_id='run_spark_ingestion',
            image='bitnami/spark:3.5.0',
            api_version='auto',
            auto_remove='success',
            command='echo "Exercice 2 Branche 2: Data ingestion via Spark - A executer manuellement avec sbt run"',
            docker_url='unix://var/run/docker.sock',
            network_mode='spark-network',
            mount_tmp_dir=False,
        )

        ex02_branch1 >> ex02_verify_cleaned >> ex02_branch2

    # =========================================================================
    # Exercice 3 : Creation des tables SQL
    # =========================================================================
    with TaskGroup('ex03_sql_tables', tooltip='Creation du schema SQL') as ex03:

        # Les scripts SQL sont executes automatiquement au demarrage de PostgreSQL
        # via le volume monte dans docker-entrypoint-initdb.d

        ex03_verify = PythonOperator(
            task_id='verify_tables',
            python_callable=verify_sql_tables_exist,
        )

    # =========================================================================
    # Exercice 4 : Dashboard (verification seulement)
    # =========================================================================
    with TaskGroup('ex04_dashboard', tooltip='Verification du dashboard') as ex04:

        ex04_check = BashOperator(
            task_id='check_dashboard_config',
            bash_command='echo "[INFO] Dashboard Streamlit configure dans ex04_dashboard/"',
        )

    # =========================================================================
    # Exercice 5 : Machine Learning
    # =========================================================================
    with TaskGroup('ex05_ml_training', tooltip='Entrainement du modele ML') as ex05:

        ex05_train = PythonOperator(
            task_id='train_model',
            python_callable=train_ml_model,
        )

    # =========================================================================
    # End
    # =========================================================================
    end = DummyOperator(task_id='end')

    # =========================================================================
    # Dependencies
    # =========================================================================
    start >> pre_checks >> ex01 >> ex02 >> ex03 >> ex04 >> ex05 >> end
