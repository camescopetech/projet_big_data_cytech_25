# Exercice 4 : Visualisation des Données NYC Taxi

## Objectif

Créer un tableau de bord interactif pour visualiser les données des taxis jaunes de New York (Juin-Août 2025) stockées dans PostgreSQL.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    PostgreSQL                                │
│                   (nyc_taxi_dw)                              │
│                                                              │
│  fact_trips + dimensions (vendor, date, time, location...)  │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          │ SQL Queries
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                    Streamlit App                             │
│                                                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ Vue         │  │ Analyse     │  │ Analyse             │  │
│  │ d'ensemble  │  │ temporelle  │  │ géographique        │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │              Analyse financière                      │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
                    Navigateur Web
                  http://localhost:8501
```

## Prérequis

1. **Exercices 1, 2, 3 terminés** : Données dans PostgreSQL
2. **Docker** en cours d'exécution avec PostgreSQL
3. **Python 3.8+** installé

## Structure du Projet

```
ex04_dashboard/
├── app.py               # Application Streamlit principale
├── requirements.txt     # Dépendances Python
├── run_ex04.sh          # Script d'exécution
├── ex04-guide.md        # Ce guide
└── venv/                # Environnement virtuel (créé automatiquement)
```

## Installation et Exécution

### Méthode 1 : Script automatique (recommandé)

```bash
cd ex04_dashboard
chmod +x run_ex04.sh
./run_ex04.sh
```

Le script va :
1. Vérifier Python et Docker
2. Vérifier PostgreSQL (le démarre si nécessaire)
3. Créer un environnement virtuel
4. Installer les dépendances
5. Lancer le dashboard

### Méthode 2 : Exécution manuelle

```bash
cd ex04_dashboard

# Créer l'environnement virtuel
python3 -m venv venv
source venv/bin/activate

# Installer les dépendances
pip install -r requirements.txt

# Lancer Streamlit
streamlit run app.py
```

## Fonctionnalités du Dashboard

### 1. Vue d'ensemble

- **KPIs principaux** : Total courses, revenu total, distance moyenne, durée moyenne
- **Graphique** : Courses par jour de la semaine
- **Graphique** : Répartition par type de paiement (camembert)
- **Graphique** : Répartition par fournisseur (CMT vs VeriFone)

### 2. Analyse Temporelle

- **Évolution quotidienne** des courses
- **Distribution horaire** des courses (par période de la journée)
- **Heures de pointe vs heures creuses**
- **Weekend vs semaine** (comparaison)

### 3. Analyse Géographique

- **Courses par arrondissement** (Manhattan, Brooklyn, Queens, etc.)
- **Treemap** des arrondissements
- **Tableau** avec distance et montant moyen par zone

### 4. Analyse Financière

- **Revenus totaux** et moyens
- **Évolution des revenus quotidiens**
- **Revenus par type de paiement**
- **Analyse des pourboires**

## Connexion à PostgreSQL

Le dashboard se connecte automatiquement à PostgreSQL avec ces paramètres :

```
Host: localhost
Port: 5432
Database: nyc_taxi_dw
User: datawarehouse
Password: datawarehouse123
```

## Requêtes SQL Utilisées

### Exemple : Courses par jour de la semaine

```sql
SELECT
    d.day_name,
    d.day_of_week,
    COUNT(*) as nb_trips,
    ROUND(AVG(f.total_amount)::numeric, 2) as avg_amount
FROM fact_trips f
JOIN dim_date d ON f.pickup_date_id = d.date_id
GROUP BY d.day_name, d.day_of_week
ORDER BY d.day_of_week;
```

### Exemple : Distribution horaire

```sql
SELECT
    t.hour,
    t.period_of_day,
    COUNT(*) as nb_trips
FROM fact_trips f
JOIN dim_time t ON f.pickup_time_id = t.time_id
GROUP BY t.hour, t.period_of_day
ORDER BY t.hour;
```

## Captures d'écran

Le dashboard génère automatiquement des visualisations interactives :

1. **Graphiques en barres** pour les comparaisons
2. **Camemberts** pour les répartitions
3. **Graphiques en lignes** pour les évolutions temporelles
4. **Treemaps** pour les analyses hiérarchiques
5. **Tableaux interactifs** avec tri et filtrage

## Technologies Utilisées

| Technologie | Usage |
|-------------|-------|
| Streamlit | Framework web Python |
| Plotly | Graphiques interactifs |
| Pandas | Manipulation de données |
| SQLAlchemy | Connexion PostgreSQL |
| psycopg2 | Driver PostgreSQL |

## Résultat Attendu

```
==========================================
Exercice 4 : Dashboard NYC Taxi
Période : Juin - Août 2025 (3 mois)
==========================================

[1/5] Vérification de Python...
✓ Python 3.11.x

[2/5] Vérification de Docker...
✓ Docker est actif

[3/5] Vérification de PostgreSQL...
✓ PostgreSQL est actif

[4/5] Installation des dépendances...
✓ Dépendances installées

[5/5] Lancement du dashboard Streamlit...
==========================================

Le dashboard va s'ouvrir dans votre navigateur.

URL : http://localhost:8501

Appuyez sur Ctrl+C pour arrêter le serveur.
==========================================

  You can now view your Streamlit app in your browser.

  Local URL: http://localhost:8501
  Network URL: http://192.168.x.x:8501
```

## Problèmes Courants

### Erreur : "Connection refused" (PostgreSQL)

**Solution** : Lancez PostgreSQL avec :
```bash
docker compose up -d postgres
```

### Erreur : "No data available"

**Solution** : Vérifiez que les exercices 1, 2 et 3 ont été exécutés :
```bash
docker exec postgres psql -U datawarehouse -d nyc_taxi_dw -c "SELECT COUNT(*) FROM fact_trips;"
```

### Erreur : "Module not found"

**Solution** : Réinstallez les dépendances :
```bash
source venv/bin/activate
pip install -r requirements.txt
```
## Prochaines Étapes

1. **Exercice 5** : Machine Learning pour prédire le prix des courses
