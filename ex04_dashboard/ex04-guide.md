# Exercice 4 : Visualisation des Donnees NYC Taxi

## Objectif

Creer un tableau de bord interactif pour visualiser les donnees des taxis jaunes de New York (Juin-Aout 2025) stockees dans PostgreSQL.

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
│  │ d'ensemble  │  │ temporelle  │  │ geographique        │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │              Analyse financiere                      │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
                    Navigateur Web
                  http://localhost:8501
```

## Prerequis

1. **Exercices 1, 2, 3 termines** : Donnees dans PostgreSQL
2. **Docker** en cours d'execution avec PostgreSQL
3. **Python 3.8+** installe

## Structure du Projet

```
ex04_dashboard/
├── app.py               # Application Streamlit principale
├── requirements.txt     # Dependances Python
├── run_ex04.sh          # Script d'execution
├── ex04-guide.md        # Ce guide
└── venv/                # Environnement virtuel (cree automatiquement)
```

## Installation et Execution

### Methode 1 : Script automatique (recommande)

```bash
cd ex04_dashboard
chmod +x run_ex04.sh
./run_ex04.sh
```

Le script va :
1. Verifier Python et Docker
2. Verifier PostgreSQL (le demarre si necessaire)
3. Creer un environnement virtuel
4. Installer les dependances
5. Lancer le dashboard

### Methode 2 : Execution manuelle

```bash
cd ex04_dashboard

# Creer l'environnement virtuel
python3 -m venv venv
source venv/bin/activate

# Installer les dependances
pip install -r requirements.txt

# Lancer Streamlit
streamlit run app.py
```

## Fonctionnalites du Dashboard

### 1. Vue d'ensemble

- **KPIs principaux** : Total courses, revenu total, distance moyenne, duree moyenne
- **Graphique** : Courses par jour de la semaine
- **Graphique** : Repartition par type de paiement (camembert)
- **Graphique** : Repartition par fournisseur (CMT vs VeriFone)

### 2. Analyse Temporelle

- **Evolution quotidienne** des courses
- **Distribution horaire** des courses (par periode de la journee)
- **Heures de pointe vs heures creuses**
- **Weekend vs semaine** (comparaison)

### 3. Analyse Geographique

- **Courses par arrondissement** (Manhattan, Brooklyn, Queens, etc.)
- **Treemap** des arrondissements
- **Tableau** avec distance et montant moyen par zone

### 4. Analyse Financiere

- **Revenus totaux** et moyens
- **Evolution des revenus quotidiens**
- **Revenus par type de paiement**
- **Analyse des pourboires**

## Connexion a PostgreSQL

Le dashboard se connecte automatiquement a PostgreSQL avec ces parametres :

```
Host: localhost
Port: 5432
Database: nyc_taxi_dw
User: datawarehouse
Password: datawarehouse123
```

## Requetes SQL Utilisees

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

## Captures d'ecran

Le dashboard genere automatiquement des visualisations interactives :

1. **Graphiques en barres** pour les comparaisons
2. **Camemberts** pour les repartitions
3. **Graphiques en lignes** pour les evolutions temporelles
4. **Treemaps** pour les analyses hierarchiques
5. **Tableaux interactifs** avec tri et filtrage

## Technologies Utilisees

| Technologie | Usage |
|-------------|-------|
| Streamlit | Framework web Python |
| Plotly | Graphiques interactifs |
| Pandas | Manipulation de donnees |
| SQLAlchemy | Connexion PostgreSQL |
| psycopg2 | Driver PostgreSQL |

## Resultat Attendu

```
==========================================
Exercice 4 : Dashboard NYC Taxi
Periode : Juin - Aout 2025 (3 mois)
==========================================

[1/5] Verification de Python...
✓ Python 3.11.x

[2/5] Verification de Docker...
✓ Docker est actif

[3/5] Verification de PostgreSQL...
✓ PostgreSQL est actif

[4/5] Installation des dependances...
✓ Dependances installees

[5/5] Lancement du dashboard Streamlit...
==========================================

Le dashboard va s'ouvrir dans votre navigateur.

URL : http://localhost:8501

Appuyez sur Ctrl+C pour arreter le serveur.
==========================================

  You can now view your Streamlit app in your browser.

  Local URL: http://localhost:8501
  Network URL: http://192.168.x.x:8501
```

## Problemes Courants

### Erreur : "Connection refused" (PostgreSQL)

**Solution** : Lancez PostgreSQL avec :
```bash
docker compose up -d postgres
```

### Erreur : "No data available"

**Solution** : Verifiez que les exercices 1, 2 et 3 ont ete executes :
```bash
docker exec postgres psql -U datawarehouse -d nyc_taxi_dw -c "SELECT COUNT(*) FROM fact_trips;"
```

### Erreur : "Module not found"

**Solution** : Reinstallez les dependances :
```bash
source venv/bin/activate
pip install -r requirements.txt
```

## Alternative : Notebook Jupyter

Si vous preferez utiliser un notebook pour l'EDA, vous pouvez :

1. Installer Jupyter :
```bash
pip install jupyter
```

2. Lancer Jupyter :
```bash
jupyter notebook
```

3. Creer un notebook dans le dossier `notebooks/` a la racine du projet.

## Prochaines Etapes

1. **Exercice 5** : Machine Learning pour predire le prix des courses
