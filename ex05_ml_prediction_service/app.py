#!/usr/bin/env python3
"""
Application Streamlit pour la prediction des tarifs NYC Taxi.

Cette application fournit une interface web pour effectuer des predictions
de tarifs de taxi en utilisant le modele ML entraine.

Notes
-----
Lancez l'application avec: streamlit run app.py

Examples
--------
Avec uv:
    $ uv run streamlit run app.py --server.port 8502
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime, time
import joblib

# Configuration de la page
st.set_page_config(
    page_title="NYC Taxi Fare Predictor",
    page_icon="🚕",
    layout="wide"
)

# Chemins
MODEL_DIR = Path(__file__).parent / "models"
MODEL_PATH = MODEL_DIR / "latest_model.joblib"


@st.cache_resource
def load_model():
    """Charge le modele ML."""
    if not MODEL_PATH.exists():
        return None
    return joblib.load(MODEL_PATH)


def extract_time_features(hour, day_of_week, month):
    """Prepare les features temporelles."""
    return {
        'hour': hour,
        'day_of_week': day_of_week,
        'month': month
    }


def predict_fare(model_data, trip_data):
    """Effectue une prediction de tarif."""
    pipeline = model_data['pipeline']
    feature_columns = model_data['feature_columns']

    # Creer le DataFrame avec les features dans le bon ordre
    df = pd.DataFrame([trip_data])

    # Ajouter les colonnes manquantes
    for col in feature_columns:
        if col not in df.columns:
            df[col] = 0

    # Reordonner
    X = df[feature_columns]

    # Prediction
    prediction = pipeline.predict(X)[0]
    return max(0, prediction)


def main():
    """Point d'entree principal de l'application."""
    st.title("🚕 NYC Taxi Fare Predictor")
    st.markdown("---")

    # Charger le modele
    model_data = load_model()

    if model_data is None:
        st.error("⚠️ Modele non trouve. Veuillez d'abord entrainer le modele.")
        st.info("Executez: `uv run python src/train.py` pour entrainer le modele.")
        return

    # Afficher les informations du modele
    with st.sidebar:
        st.header("ℹ️ Informations du Modele")
        st.write(f"**Version:** {model_data.get('version', 'N/A')}")
        st.write(f"**Cree le:** {model_data.get('created_at', 'N/A')[:10]}")

        metrics = model_data.get('metrics', {})
        if metrics:
            st.subheader("Metriques")
            test_metrics = metrics.get('test', {})
            st.metric("RMSE (Test)", f"${test_metrics.get('rmse', 0):.2f}")
            st.metric("MAE (Test)", f"${test_metrics.get('mae', 0):.2f}")
            st.metric("R² (Test)", f"{test_metrics.get('r2', 0):.3f}")

    # Interface principale
    col1, col2 = st.columns([1, 1])

    with col1:
        st.header("📍 Details du Trajet")

        # Distance
        trip_distance = st.slider(
            "Distance du trajet (miles)",
            min_value=0.1,
            max_value=50.0,
            value=5.0,
            step=0.1
        )

        # Passagers
        passenger_count = st.selectbox(
            "Nombre de passagers",
            options=[1, 2, 3, 4, 5, 6],
            index=0
        )

        # Zones
        st.subheader("Zones")
        col_pu, col_do = st.columns(2)
        with col_pu:
            pu_location = st.number_input(
                "Zone de Pickup (1-265)",
                min_value=1,
                max_value=265,
                value=100
            )
        with col_do:
            do_location = st.number_input(
                "Zone de Dropoff (1-265)",
                min_value=1,
                max_value=265,
                value=200
            )

        # Type de tarif
        rate_codes = {
            1: "Standard",
            2: "JFK",
            3: "Newark",
            4: "Nassau/Westchester",
            5: "Prix negocie",
            6: "Groupe"
        }
        rate_code = st.selectbox(
            "Type de tarif",
            options=list(rate_codes.keys()),
            format_func=lambda x: f"{x} - {rate_codes[x]}"
        )

    with col2:
        st.header("🕐 Date et Heure")

        # Date et heure
        selected_date = st.date_input(
            "Date du trajet",
            value=datetime.now()
        )

        selected_time = st.time_input(
            "Heure du trajet",
            value=time(14, 0)
        )

        # Extraire les features temporelles
        hour = selected_time.hour
        day_of_week = selected_date.weekday()
        month = selected_date.month

        st.info(f"""
        **Features temporelles:**
        - Heure: {hour}h
        - Jour: {['Lundi', 'Mardi', 'Mercredi', 'Jeudi', 'Vendredi', 'Samedi', 'Dimanche'][day_of_week]}
        - Mois: {['Jan', 'Fev', 'Mar', 'Avr', 'Mai', 'Juin', 'Juil', 'Aout', 'Sep', 'Oct', 'Nov', 'Dec'][month-1]}
        """)

    # Prediction
    st.markdown("---")

    if st.button("🔮 Predire le Tarif", type="primary", use_container_width=True):
        trip_data = {
            'trip_distance': trip_distance,
            'passenger_count': passenger_count,
            'PULocationID': pu_location,
            'DOLocationID': do_location,
            'RatecodeID': rate_code,
            'hour': hour,
            'day_of_week': day_of_week,
            'month': month
        }

        with st.spinner("Calcul en cours..."):
            predicted_fare = predict_fare(model_data, trip_data)

            # Intervalle de confiance
            rmse = model_data.get('metrics', {}).get('test', {}).get('rmse', 5.0)
            confidence = 1.96 * rmse

        # Afficher le resultat
        col_result1, col_result2, col_result3 = st.columns(3)

        with col_result1:
            st.metric(
                "💰 Tarif Predit",
                f"${predicted_fare:.2f}"
            )

        with col_result2:
            st.metric(
                "📊 Fourchette Basse",
                f"${max(0, predicted_fare - confidence):.2f}"
            )

        with col_result3:
            st.metric(
                "📊 Fourchette Haute",
                f"${predicted_fare + confidence:.2f}"
            )

        # Graphique de decomposition
        st.subheader("📈 Analyse de la Prediction")

        # Estimation des composantes (approximation)
        base_fare = 3.0  # Tarif de base NYC
        per_mile = predicted_fare / max(trip_distance, 0.1) if trip_distance > 0 else 0
        estimated_components = {
            'Tarif de base': base_fare,
            'Distance': trip_distance * 2.5,
            'Supplements': max(0, predicted_fare - base_fare - trip_distance * 2.5)
        }

        fig = go.Figure(data=[
            go.Bar(
                x=list(estimated_components.keys()),
                y=list(estimated_components.values()),
                marker_color=['#1f77b4', '#ff7f0e', '#2ca02c']
            )
        ])
        fig.update_layout(
            title="Decomposition estimee du tarif",
            xaxis_title="Composante",
            yaxis_title="Montant ($)",
            height=300
        )
        st.plotly_chart(fig, use_container_width=True)

    # Section d'analyse
    st.markdown("---")
    st.header("📊 Analyse des Tarifs")

    tab1, tab2 = st.tabs(["Impact de la Distance", "Impact de l'Heure"])

    with tab1:
        # Graphique: tarif vs distance
        distances = np.linspace(0.5, 30, 30)
        fares = []
        for d in distances:
            trip_data = {
                'trip_distance': d,
                'passenger_count': passenger_count,
                'PULocationID': pu_location,
                'DOLocationID': do_location,
                'RatecodeID': rate_code,
                'hour': hour,
                'day_of_week': day_of_week,
                'month': month
            }
            fares.append(predict_fare(model_data, trip_data))

        fig = px.line(
            x=distances,
            y=fares,
            labels={'x': 'Distance (miles)', 'y': 'Tarif predit ($)'},
            title="Evolution du tarif selon la distance"
        )
        fig.add_scatter(
            x=[trip_distance],
            y=[predicted_fare if 'predicted_fare' in dir() else fares[int(trip_distance)]],
            mode='markers',
            marker=dict(size=15, color='red'),
            name='Votre trajet'
        )
        st.plotly_chart(fig, use_container_width=True)

    with tab2:
        # Graphique: tarif vs heure
        hours = list(range(24))
        hourly_fares = []
        for h in hours:
            trip_data = {
                'trip_distance': trip_distance,
                'passenger_count': passenger_count,
                'PULocationID': pu_location,
                'DOLocationID': do_location,
                'RatecodeID': rate_code,
                'hour': h,
                'day_of_week': day_of_week,
                'month': month
            }
            hourly_fares.append(predict_fare(model_data, trip_data))

        fig = px.bar(
            x=hours,
            y=hourly_fares,
            labels={'x': 'Heure de la journee', 'y': 'Tarif predit ($)'},
            title="Variation du tarif selon l'heure"
        )
        st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    main()
