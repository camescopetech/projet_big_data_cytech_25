"""
Exercice 4 : Dashboard de visualisation des données NYC Taxi.

Ce tableau de bord Streamlit permet de visualiser les données des taxis jaunes
de New York stockées dans PostgreSQL (Data Warehouse).

Periode : Juin - Aout 2025 (3 mois)

Author: Equipe Big Data CY Tech
Version: 1.1.0
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv

# =============================================================================
# CONFIGURATION
# =============================================================================

# Charger les variables d'environnement depuis .env
load_dotenv()

# Configuration PostgreSQL depuis les variables d'environnement
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "nyc_taxi_dw")
PG_USER = os.getenv("PG_USER", "datawarehouse")
PG_PASSWORD = os.getenv("PG_PASSWORD", "datawarehouse123")

# Connexion string
DATABASE_URL = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

# Configuration Streamlit
st.set_page_config(
    page_title="NYC Taxi Dashboard",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# CONNEXION BASE DE DONNEES
# =============================================================================

@st.cache_resource
def get_database_connection():
    """Cree une connexion a la base de donnees PostgreSQL."""
    try:
        engine = create_engine(DATABASE_URL)
        return engine
    except Exception as e:
        st.error(f"Erreur de connexion a PostgreSQL: {e}")
        return None


@st.cache_data(ttl=300)
def execute_query(query: str) -> pd.DataFrame:
    """Execute une requete SQL et retourne un DataFrame."""
    engine = get_database_connection()
    if engine is None:
        return pd.DataFrame()
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        st.error(f"Erreur lors de l'execution de la requete: {e}")
        return pd.DataFrame()


# =============================================================================
# REQUETES SQL
# =============================================================================

def get_total_trips():
    """Retourne le nombre total de courses."""
    query = "SELECT COUNT(*) as total FROM fact_trips;"
    df = execute_query(query)
    return df['total'].iloc[0] if not df.empty else 0


def get_total_revenue():
    """Retourne le revenu total."""
    query = "SELECT SUM(total_amount) as revenue FROM fact_trips;"
    df = execute_query(query)
    return df['revenue'].iloc[0] if not df.empty else 0


def get_avg_trip_distance():
    """Retourne la distance moyenne des courses."""
    query = "SELECT AVG(trip_distance) as avg_distance FROM fact_trips;"
    df = execute_query(query)
    return df['avg_distance'].iloc[0] if not df.empty else 0


def get_avg_trip_duration():
    """Retourne la duree moyenne des courses."""
    query = "SELECT AVG(trip_duration_minutes) as avg_duration FROM fact_trips;"
    df = execute_query(query)
    return df['avg_duration'].iloc[0] if not df.empty else 0


def get_trips_by_day_of_week():
    """Retourne le nombre de courses par jour de la semaine."""
    query = """
    SELECT
        d.day_name,
        d.day_of_week,
        COUNT(*) as nb_trips,
        ROUND(AVG(f.total_amount)::numeric, 2) as avg_amount,
        ROUND(AVG(f.trip_distance)::numeric, 2) as avg_distance
    FROM fact_trips f
    JOIN dim_date d ON f.pickup_date_id = d.date_id
    GROUP BY d.day_name, d.day_of_week
    ORDER BY d.day_of_week;
    """
    return execute_query(query)


def get_trips_by_hour():
    """Retourne le nombre de courses par heure."""
    query = """
    SELECT
        t.hour,
        t.period_of_day,
        COUNT(*) as nb_trips,
        ROUND(AVG(f.total_amount)::numeric, 2) as avg_amount
    FROM fact_trips f
    JOIN dim_time t ON f.pickup_time_id = t.time_id
    GROUP BY t.hour, t.period_of_day
    ORDER BY t.hour;
    """
    return execute_query(query)


def get_trips_by_payment_type():
    """Retourne la repartition par type de paiement."""
    query = """
    SELECT
        p.payment_name,
        COUNT(*) as nb_trips,
        ROUND(SUM(f.total_amount)::numeric, 2) as total_revenue,
        ROUND(AVG(f.tip_amount)::numeric, 2) as avg_tip
    FROM fact_trips f
    JOIN dim_payment_type p ON f.payment_type_id = p.payment_type_id
    GROUP BY p.payment_name
    ORDER BY nb_trips DESC;
    """
    return execute_query(query)


def get_trips_by_vendor():
    """Retourne la repartition par fournisseur."""
    query = """
    SELECT
        v.vendor_name,
        COUNT(*) as nb_trips,
        ROUND(AVG(f.total_amount)::numeric, 2) as avg_amount,
        ROUND(AVG(f.trip_distance)::numeric, 2) as avg_distance
    FROM fact_trips f
    JOIN dim_vendor v ON f.vendor_id = v.vendor_id
    GROUP BY v.vendor_name
    ORDER BY nb_trips DESC;
    """
    return execute_query(query)


def get_trips_by_borough():
    """Retourne le nombre de courses par arrondissement (pickup)."""
    query = """
    SELECT
        b.borough_name,
        COUNT(*) as nb_trips,
        ROUND(AVG(f.total_amount)::numeric, 2) as avg_amount,
        ROUND(AVG(f.trip_distance)::numeric, 2) as avg_distance
    FROM fact_trips f
    JOIN dim_location l ON f.pickup_location_id = l.location_id
    JOIN dim_borough b ON l.borough_id = b.borough_id
    GROUP BY b.borough_name
    ORDER BY nb_trips DESC;
    """
    return execute_query(query)


def get_daily_trips():
    """Retourne le nombre de courses par jour."""
    query = """
    SELECT
        d.full_date,
        COUNT(*) as nb_trips,
        ROUND(SUM(f.total_amount)::numeric, 2) as daily_revenue
    FROM fact_trips f
    JOIN dim_date d ON f.pickup_date_id = d.date_id
    GROUP BY d.full_date
    ORDER BY d.full_date;
    """
    return execute_query(query)


def get_rush_hour_analysis():
    """Analyse des heures de pointe."""
    query = """
    SELECT
        t.is_rush_hour,
        COUNT(*) as nb_trips,
        ROUND(AVG(f.total_amount)::numeric, 2) as avg_amount,
        ROUND(AVG(f.trip_duration_minutes)::numeric, 2) as avg_duration
    FROM fact_trips f
    JOIN dim_time t ON f.pickup_time_id = t.time_id
    GROUP BY t.is_rush_hour;
    """
    return execute_query(query)


def get_weekend_analysis():
    """Analyse weekend vs semaine."""
    query = """
    SELECT
        d.is_weekend,
        COUNT(*) as nb_trips,
        ROUND(AVG(f.total_amount)::numeric, 2) as avg_amount,
        ROUND(AVG(f.trip_distance)::numeric, 2) as avg_distance,
        ROUND(AVG(f.tip_amount)::numeric, 2) as avg_tip
    FROM fact_trips f
    JOIN dim_date d ON f.pickup_date_id = d.date_id
    GROUP BY d.is_weekend;
    """
    return execute_query(query)


# =============================================================================
# INTERFACE UTILISATEUR
# =============================================================================

def main():
    """Fonction principale du dashboard."""

    # Titre principal
    st.title("🚕 NYC Yellow Taxi Dashboard")
    st.markdown("**Periode : Juin - Aout 2025 (3 mois)**")
    st.markdown("---")

    # Verifier la connexion
    engine = get_database_connection()
    if engine is None:
        st.error("❌ Impossible de se connecter a PostgreSQL. Verifiez que Docker est en cours d'execution.")
        st.info("Lancez `docker compose up -d postgres` depuis la racine du projet.")
        return

    # Sidebar
    st.sidebar.title("Navigation")
    page = st.sidebar.radio(
        "Choisir une vue",
        ["Vue d'ensemble", "Analyse temporelle", "Analyse geographique", "Analyse financiere"]
    )

    st.sidebar.markdown("---")
    st.sidebar.markdown("### Connexion")
    st.sidebar.success("✅ Connecte a PostgreSQL")
    st.sidebar.text(f"Base: {PG_DATABASE}")

    # Afficher la page selectionnee
    if page == "Vue d'ensemble":
        show_overview()
    elif page == "Analyse temporelle":
        show_temporal_analysis()
    elif page == "Analyse geographique":
        show_geographic_analysis()
    elif page == "Analyse financiere":
        show_financial_analysis()


def show_overview():
    """Affiche la vue d'ensemble."""
    st.header("📊 Vue d'ensemble")

    # KPIs principaux
    col1, col2, col3, col4 = st.columns(4)

    total_trips = get_total_trips()
    total_revenue = get_total_revenue()
    avg_distance = get_avg_trip_distance()
    avg_duration = get_avg_trip_duration()

    with col1:
        st.metric("Total Courses", f"{total_trips:,.0f}")
    with col2:
        st.metric("Revenu Total", f"${total_revenue:,.2f}" if total_revenue else "$0")
    with col3:
        st.metric("Distance Moyenne", f"{avg_distance:.2f} mi" if avg_distance else "0 mi")
    with col4:
        st.metric("Duree Moyenne", f"{avg_duration:.1f} min" if avg_duration else "0 min")

    st.markdown("---")

    # Graphiques cote a cote
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Courses par jour de la semaine")
        df_days = get_trips_by_day_of_week()
        if not df_days.empty:
            fig = px.bar(
                df_days,
                x='day_name',
                y='nb_trips',
                color='avg_amount',
                color_continuous_scale='Blues',
                labels={'day_name': 'Jour', 'nb_trips': 'Nombre de courses', 'avg_amount': 'Montant moyen ($)'}
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Pas de donnees disponibles")

    with col2:
        st.subheader("Repartition par type de paiement")
        df_payment = get_trips_by_payment_type()
        if not df_payment.empty:
            fig = px.pie(
                df_payment,
                values='nb_trips',
                names='payment_name',
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Set2
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Pas de donnees disponibles")

    # Graphique fournisseurs
    st.subheader("Repartition par fournisseur (Vendor)")
    df_vendor = get_trips_by_vendor()
    if not df_vendor.empty:
        col1, col2 = st.columns([2, 1])
        with col1:
            fig = px.bar(
                df_vendor,
                x='vendor_name',
                y='nb_trips',
                color='vendor_name',
                labels={'vendor_name': 'Fournisseur', 'nb_trips': 'Nombre de courses'}
            )
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            st.dataframe(df_vendor, use_container_width=True)


def show_temporal_analysis():
    """Affiche l'analyse temporelle."""
    st.header("⏰ Analyse Temporelle")

    # Evolution journaliere
    st.subheader("Evolution quotidienne des courses")
    df_daily = get_daily_trips()
    if not df_daily.empty:
        fig = px.line(
            df_daily,
            x='full_date',
            y='nb_trips',
            labels={'full_date': 'Date', 'nb_trips': 'Nombre de courses'}
        )
        fig.update_traces(line_color='#1f77b4')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Pas de donnees disponibles")

    # Analyse par heure
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Distribution horaire des courses")
        df_hours = get_trips_by_hour()
        if not df_hours.empty:
            fig = px.bar(
                df_hours,
                x='hour',
                y='nb_trips',
                color='period_of_day',
                labels={'hour': 'Heure', 'nb_trips': 'Nombre de courses', 'period_of_day': 'Periode'}
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Pas de donnees disponibles")

    with col2:
        st.subheader("Heures de pointe vs Heures creuses")
        df_rush = get_rush_hour_analysis()
        if not df_rush.empty:
            df_rush['type'] = df_rush['is_rush_hour'].map({True: 'Heures de pointe', False: 'Heures creuses'})
            fig = px.bar(
                df_rush,
                x='type',
                y='nb_trips',
                color='type',
                labels={'type': '', 'nb_trips': 'Nombre de courses'}
            )
            st.plotly_chart(fig, use_container_width=True)

            # Metriques
            rush_data = df_rush[df_rush['is_rush_hour'] == True]
            non_rush_data = df_rush[df_rush['is_rush_hour'] == False]

            if not rush_data.empty and not non_rush_data.empty:
                col_a, col_b = st.columns(2)
                with col_a:
                    st.metric("Duree moy. (pointe)", f"{rush_data['avg_duration'].iloc[0]:.1f} min")
                with col_b:
                    st.metric("Duree moy. (creuse)", f"{non_rush_data['avg_duration'].iloc[0]:.1f} min")
        else:
            st.info("Pas de donnees disponibles")

    # Weekend vs Semaine
    st.subheader("Weekend vs Semaine")
    df_weekend = get_weekend_analysis()
    if not df_weekend.empty:
        df_weekend['type'] = df_weekend['is_weekend'].map({True: 'Weekend', False: 'Semaine'})

        col1, col2, col3 = st.columns(3)

        with col1:
            fig = px.bar(df_weekend, x='type', y='nb_trips', color='type',
                        labels={'type': '', 'nb_trips': 'Nombre de courses'})
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.bar(df_weekend, x='type', y='avg_amount', color='type',
                        labels={'type': '', 'avg_amount': 'Montant moyen ($)'})
            st.plotly_chart(fig, use_container_width=True)

        with col3:
            fig = px.bar(df_weekend, x='type', y='avg_tip', color='type',
                        labels={'type': '', 'avg_tip': 'Pourboire moyen ($)'})
            st.plotly_chart(fig, use_container_width=True)


def show_geographic_analysis():
    """Affiche l'analyse geographique."""
    st.header("🗺️ Analyse Geographique")

    # Par arrondissement
    st.subheader("Courses par arrondissement (Pickup)")
    df_borough = get_trips_by_borough()
    if not df_borough.empty:
        col1, col2 = st.columns([2, 1])

        with col1:
            fig = px.bar(
                df_borough,
                x='borough_name',
                y='nb_trips',
                color='avg_amount',
                color_continuous_scale='Viridis',
                labels={
                    'borough_name': 'Arrondissement',
                    'nb_trips': 'Nombre de courses',
                    'avg_amount': 'Montant moyen ($)'
                }
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.dataframe(
                df_borough[['borough_name', 'nb_trips', 'avg_amount', 'avg_distance']],
                use_container_width=True,
                column_config={
                    'borough_name': 'Arrondissement',
                    'nb_trips': st.column_config.NumberColumn('Courses', format="%d"),
                    'avg_amount': st.column_config.NumberColumn('$ Moyen', format="%.2f"),
                    'avg_distance': st.column_config.NumberColumn('Dist. Moy.', format="%.2f mi")
                }
            )

        # Graphique en treemap
        st.subheader("Treemap des arrondissements")
        fig = px.treemap(
            df_borough,
            path=['borough_name'],
            values='nb_trips',
            color='avg_distance',
            color_continuous_scale='RdYlGn_r',
            labels={'nb_trips': 'Courses', 'avg_distance': 'Distance moyenne'}
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Pas de donnees disponibles")


def show_financial_analysis():
    """Affiche l'analyse financiere."""
    st.header("💰 Analyse Financiere")

    # KPIs financiers
    total_revenue = get_total_revenue()
    total_trips = get_total_trips()

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Revenu Total", f"${total_revenue:,.2f}" if total_revenue else "$0")
    with col2:
        avg_per_trip = total_revenue / total_trips if total_trips > 0 else 0
        st.metric("Revenu Moyen par Course", f"${avg_per_trip:.2f}")
    with col3:
        st.metric("Total Courses", f"{total_trips:,}")

    st.markdown("---")

    # Revenus par jour
    st.subheader("Evolution des revenus quotidiens")
    df_daily = get_daily_trips()
    if not df_daily.empty:
        fig = px.area(
            df_daily,
            x='full_date',
            y='daily_revenue',
            labels={'full_date': 'Date', 'daily_revenue': 'Revenu ($)'}
        )
        fig.update_traces(fill='tozeroy', line_color='#2ecc71')
        st.plotly_chart(fig, use_container_width=True)

    # Analyse par type de paiement
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Revenus par type de paiement")
        df_payment = get_trips_by_payment_type()
        if not df_payment.empty:
            fig = px.bar(
                df_payment,
                x='payment_name',
                y='total_revenue',
                color='payment_name',
                labels={'payment_name': 'Type de paiement', 'total_revenue': 'Revenu total ($)'}
            )
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Pourboire moyen par type de paiement")
        if not df_payment.empty:
            fig = px.bar(
                df_payment,
                x='payment_name',
                y='avg_tip',
                color='payment_name',
                labels={'payment_name': 'Type de paiement', 'avg_tip': 'Pourboire moyen ($)'}
            )
            st.plotly_chart(fig, use_container_width=True)

    # Tableau recapitulatif
    st.subheader("Tableau recapitulatif par type de paiement")
    if not df_payment.empty:
        st.dataframe(
            df_payment,
            use_container_width=True,
            column_config={
                'payment_name': 'Type de paiement',
                'nb_trips': st.column_config.NumberColumn('Courses', format="%d"),
                'total_revenue': st.column_config.NumberColumn('Revenu Total', format="$%.2f"),
                'avg_tip': st.column_config.NumberColumn('Pourboire Moyen', format="$%.2f")
            }
        )


# =============================================================================
# POINT D'ENTREE
# =============================================================================

if __name__ == "__main__":
    main()
