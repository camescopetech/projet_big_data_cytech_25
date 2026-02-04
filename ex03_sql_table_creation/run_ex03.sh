#!/bin/bash
# =============================================================================
# Exercice 3 : Création des tables SQL (Data Warehouse)
# =============================================================================

set -e

# Couleurs pour l'affichage
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "=========================================="
echo "Exercice 3 : Création du Data Warehouse"
echo "=========================================="

# Configuration PostgreSQL
PG_HOST="localhost"
PG_PORT="5432"
PG_USER="datawarehouse"
PG_PASSWORD="datawarehouse123"
PG_DB="nyc_taxi_dw"

export PGPASSWORD=$PG_PASSWORD

# -----------------------------------------------------------------------------
# Étape 1 : Vérifier Docker
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}[1/5]${NC} Vérification de Docker..."
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}✗${NC} Docker n'est pas actif. Lancez Docker Desktop."
    exit 1
fi
echo -e "${GREEN}✓${NC} Docker est actif"

# -----------------------------------------------------------------------------
# Étape 2 : Démarrer PostgreSQL
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}[2/5]${NC} Démarrage de PostgreSQL..."

cd "$(dirname "$0")/.."

# Vérifier si le conteneur existe déjà
if docker ps -a --format '{{.Names}}' | grep -q '^postgres$'; then
    if docker ps --format '{{.Names}}' | grep -q '^postgres$'; then
        echo -e "${BLUE}[INFO]${NC} PostgreSQL est déjà en cours d'exécution"
    else
        echo -e "${BLUE}[INFO]${NC} Démarrage du conteneur PostgreSQL existant..."
        docker start postgres
    fi
else
    echo -e "${BLUE}[INFO]${NC} Création et démarrage de PostgreSQL..."
    docker compose up -d postgres
fi

# Attendre que PostgreSQL soit prêt
echo -e "${BLUE}[INFO]${NC} Attente de PostgreSQL..."
for i in {1..30}; do
    if docker exec postgres pg_isready -U $PG_USER -d $PG_DB > /dev/null 2>&1; then
        break
    fi
    sleep 1
done

if ! docker exec postgres pg_isready -U $PG_USER -d $PG_DB > /dev/null 2>&1; then
    echo -e "${RED}✗${NC} PostgreSQL n'est pas prêt après 30 secondes"
    exit 1
fi

echo -e "${GREEN}✓${NC} PostgreSQL est prêt"

# -----------------------------------------------------------------------------
# Étape 3 : Exécuter le script de création des tables
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}[3/5]${NC} Création des tables (modèle flocon)..."

docker exec -i postgres psql -U $PG_USER -d $PG_DB < ex03_sql_table_creation/creation.sql

echo -e "${GREEN}✓${NC} Tables créées avec succès"

# -----------------------------------------------------------------------------
# Étape 4 : Insérer les données de référence
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}[4/5]${NC} Insertion des données de référence..."

docker exec -i postgres psql -U $PG_USER -d $PG_DB < ex03_sql_table_creation/insertion.sql

echo -e "${GREEN}✓${NC} Données de référence insérées"

# -----------------------------------------------------------------------------
# Étape 5 : Vérification
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}[5/5]${NC} Vérification des tables..."

echo -e "\n${BLUE}Tables créées :${NC}"
docker exec postgres psql -U $PG_USER -d $PG_DB -c "\dt"

echo -e "\n${BLUE}Comptage des données de référence :${NC}"
docker exec postgres psql -U $PG_USER -d $PG_DB -c "
SELECT 'dim_vendor' AS table_name, COUNT(*) AS row_count FROM dim_vendor
UNION ALL SELECT 'dim_rate_code', COUNT(*) FROM dim_rate_code
UNION ALL SELECT 'dim_payment_type', COUNT(*) FROM dim_payment_type
UNION ALL SELECT 'dim_borough', COUNT(*) FROM dim_borough
UNION ALL SELECT 'dim_location', COUNT(*) FROM dim_location
UNION ALL SELECT 'dim_time', COUNT(*) FROM dim_time
UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL SELECT 'fact_trips', COUNT(*) FROM fact_trips
ORDER BY table_name;
"

echo -e "\n=========================================="
echo -e "${GREEN}✓ EXERCICE 3 TERMINÉ !${NC}"
echo "=========================================="
echo ""
echo "Résultat :"
echo "  - Tables créées dans PostgreSQL"
echo "  - Données de référence insérées"
echo "  - Base de données : $PG_DB"
echo ""
echo "Connexion :"
echo "  Host: localhost"
echo "  Port: 5432"
echo "  User: $PG_USER"
echo "  Password: $PG_PASSWORD"
echo "  Database: $PG_DB"
echo ""
echo "Prochaine étape :"
echo "  - Exercice 2 Branche 2 : Ingestion des données nettoyées"
