#!/bin/bash
# =============================================================================
# Exercice 4 : Dashboard de visualisation des donnees NYC Taxi
# =============================================================================
#
# Description:
#   Lance le tableau de bord Streamlit pour visualiser les donnees
#   des taxis jaunes de New York depuis PostgreSQL.
#
# Usage:
#   ./run_ex04.sh
#
# Prerequis:
#   - Python 3.8+ installe
#   - PostgreSQL en cours d'execution (docker compose up -d postgres)
#   - Exercices 1, 2, 3 termines (donnees dans PostgreSQL)
#
# Auteur: Camille Bezet, Brasa Franklin, Kenmogne Loic, Martin Soares Flavio
# Version: 1.0.0
# =============================================================================

set -e

# Couleurs pour l'affichage
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "=========================================="
echo "Exercice 4 : Dashboard NYC Taxi"
echo "Periode : Juin - Aout 2025 (3 mois)"
echo "=========================================="

# Se positionner dans le repertoire du script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# -----------------------------------------------------------------------------
# Etape 1 : Verifier Python
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}[1/5]${NC} Verification de Python..."
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}✗${NC} Python 3 n'est pas installe"
    exit 1
fi
PYTHON_VERSION=$(python3 --version)
echo -e "${GREEN}✓${NC} $PYTHON_VERSION"

# -----------------------------------------------------------------------------
# Etape 2 : Verifier Docker
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}[2/5]${NC} Verification de Docker..."
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}✗${NC} Docker n'est pas actif. Lancez Docker Desktop."
    exit 1
fi
echo -e "${GREEN}✓${NC} Docker est actif"

# -----------------------------------------------------------------------------
# Etape 3 : Verifier PostgreSQL
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}[3/5]${NC} Verification de PostgreSQL..."
if ! docker exec postgres pg_isready -U datawarehouse -d nyc_taxi_dw > /dev/null 2>&1; then
    echo -e "${BLUE}[INFO]${NC} Demarrage de PostgreSQL..."
    cd "$SCRIPT_DIR/.."
    docker compose up -d postgres
    echo -e "${BLUE}[INFO]${NC} Attente de PostgreSQL..."
    sleep 5
fi

if ! docker exec postgres pg_isready -U datawarehouse -d nyc_taxi_dw > /dev/null 2>&1; then
    echo -e "${RED}✗${NC} PostgreSQL n'est pas pret"
    exit 1
fi
echo -e "${GREEN}✓${NC} PostgreSQL est actif"

# -----------------------------------------------------------------------------
# Etape 4 : Installer les dependances
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}[4/5]${NC} Installation des dependances..."

# Creer un environnement virtuel si necessaire
if [ ! -d "venv" ]; then
    echo -e "${BLUE}[INFO]${NC} Creation de l'environnement virtuel..."
    python3 -m venv venv
fi

# Activer l'environnement virtuel
source venv/bin/activate

# Installer les dependances
pip install -q -r requirements.txt
echo -e "${GREEN}✓${NC} Dependances installees"

# -----------------------------------------------------------------------------
# Etape 5 : Lancer Streamlit
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}[5/5]${NC} Lancement du dashboard Streamlit..."
echo "=========================================="
echo ""
echo -e "${GREEN}Le dashboard va s'ouvrir dans votre navigateur.${NC}"
echo ""
echo "URL : http://localhost:8501"
echo ""
echo "Appuyez sur Ctrl+C pour arreter le serveur."
echo "=========================================="

streamlit run app.py --server.port 8501 --server.headless false
