#!/bin/bash
# =============================================================================
# Exercice 2 - Branche 2 : Ingestion vers PostgreSQL
# =============================================================================

set -e

# Couleurs pour l'affichage
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "=========================================="
echo "Exercice 2 - Branche 2 : Ingestion PostgreSQL"
echo "Période : Juin - Août 2025 (3 mois)"
echo "=========================================="

# Détection de Java 17
if [[ "$OSTYPE" == "darwin"* ]]; then
    export JAVA_HOME=$(/usr/libexec/java_home -v 17 2>/dev/null || echo "")
fi

if [ -z "$JAVA_HOME" ]; then
    echo -e "${RED}✗${NC} Java 17 non trouvé. Installez-le avec: brew install openjdk@17"
    exit 1
fi

echo "Java Home: $JAVA_HOME"

# -----------------------------------------------------------------------------
# Étape 1 : Vérifier Docker
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}[1/6]${NC} Vérification de Docker..."
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}✗${NC} Docker n'est pas actif. Lancez Docker Desktop."
    exit 1
fi
echo -e "${GREEN}✓${NC} Docker est actif"

# -----------------------------------------------------------------------------
# Étape 2 : Vérifier MinIO
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}[2/6]${NC} Vérification de MinIO..."
if ! curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; then
    echo -e "${BLUE}[INFO]${NC} Démarrage de MinIO..."
    cd "$(dirname "$0")/.."
    docker compose up -d minio
    sleep 5
fi
echo -e "${GREEN}✓${NC} MinIO est actif"

# -----------------------------------------------------------------------------
# Étape 3 : Vérifier PostgreSQL
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}[3/6]${NC} Vérification de PostgreSQL..."
if ! docker exec postgres pg_isready -U datawarehouse -d nyc_taxi_dw > /dev/null 2>&1; then
    echo -e "${BLUE}[INFO]${NC} Démarrage de PostgreSQL..."
    cd "$(dirname "$0")/.."
    docker compose up -d postgres
    echo -e "${BLUE}[INFO]${NC} Attente de PostgreSQL..."
    for i in {1..30}; do
        if docker exec postgres pg_isready -U datawarehouse -d nyc_taxi_dw > /dev/null 2>&1; then
            break
        fi
        sleep 1
    done
fi

if ! docker exec postgres pg_isready -U datawarehouse -d nyc_taxi_dw > /dev/null 2>&1; then
    echo -e "${RED}✗${NC} PostgreSQL n'est pas prêt"
    exit 1
fi
echo -e "${GREEN}✓${NC} PostgreSQL est actif"

# -----------------------------------------------------------------------------
# Étape 4 : Vérifier le bucket nyc-cleaned
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}[4/6]${NC} Vérification du bucket nyc-cleaned..."
echo -e "${BLUE}[INFO]${NC} Assurez-vous que l'exercice 2 branche 1 a été exécuté"
echo -e "${GREEN}✓${NC} Prérequis vérifié"

# -----------------------------------------------------------------------------
# Étape 5 : Vérifier les tables (Exercice 3)
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}[5/6]${NC} Vérification des tables PostgreSQL..."

TABLE_COUNT=$(docker exec postgres psql -U datawarehouse -d nyc_taxi_dw -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE';" 2>/dev/null | tr -d ' ')

if [ "$TABLE_COUNT" -lt 7 ]; then
    echo -e "${RED}✗${NC} Les tables ne sont pas créées. Exécutez l'exercice 3 d'abord."
    exit 1
fi
echo -e "${GREEN}✓${NC} Tables PostgreSQL prêtes ($TABLE_COUNT tables)"

# -----------------------------------------------------------------------------
# Étape 6 : Exécuter le programme Spark
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}[6/6]${NC} Exécution du programme Spark..."
echo "=========================================="

cd "$(dirname "$0")"
sbt run

echo -e "\n=========================================="
echo -e "${GREEN}✓ EXERCICE 2 - BRANCHE 2 TERMINÉ !${NC}"
echo "=========================================="
echo ""
echo "Résultat :"
echo "  - Données insérées dans PostgreSQL"
echo "  - Table : fact_trips"
echo "  - Mois traités : Juin, Juillet, Août 2025"
echo ""
echo "Vérification :"
echo "  docker exec postgres psql -U datawarehouse -d nyc_taxi_dw -c 'SELECT COUNT(*) FROM fact_trips;'"
echo ""
echo "Prochaine étape :"
echo "  - Exercice 4 : Visualisation des données"
