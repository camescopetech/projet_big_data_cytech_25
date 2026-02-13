#!/bin/bash
# =============================================================================
# Exercice 5 : Service de Prediction ML pour NYC Taxi
# =============================================================================
#
# Description:
#   Entraine un modele de Machine Learning pour predire les tarifs des taxis
#   jaunes de New York, puis lance une application Streamlit de demonstration.
#
# Usage:
#   ./run_ex05.sh [--train-only] [--app-only] [--test]
#
# Options:
#   --train-only  : Effectue uniquement l'entrainement du modele
#   --app-only    : Lance uniquement l'application Streamlit (necessite modele)
#   --test        : Execute les tests unitaires
#
# Prerequis:
#   - uv installe (gestionnaire d'environnement Python)
#   - Donnees Parquet disponibles (ex01_data_retrieval/data/raw/)
#
# IMPORTANT:
#   L'utilisation de Python natif est strictement interdite.
#   Vous devez utiliser les environnements virtuels geres par uv.
#
# Auteur: Camille Bezet, Brasa Franklin, Kenmogne Loic, Martin Soares Flavio
# Version: 1.0.0
# =============================================================================

set -e

# =============================================================================
# CONFIGURATION
# =============================================================================

readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly BLUE='\033[0;34m'
readonly CYAN='\033[0;36m'
readonly NC='\033[0m' # No Color

# Repertoire du script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Options par defaut
TRAIN_ONLY=false
APP_ONLY=false
RUN_TESTS=false

# =============================================================================
# FONCTIONS
# =============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[OK]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERREUR]${NC} $1" >&2
    exit 1
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_step() {
    echo -e "\n${CYAN}[$1/$2]${NC} $3"
}

print_header() {
    echo ""
    echo "=========================================="
    echo "$1"
    echo "=========================================="
}

show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --train-only    Effectue uniquement l'entrainement du modele"
    echo "  --app-only      Lance uniquement l'application Streamlit"
    echo "  --test          Execute les tests unitaires"
    echo "  -h, --help      Affiche cette aide"
    echo ""
    echo "Exemples:"
    echo "  $0              # Entraine puis lance l'app"
    echo "  $0 --train-only # Entraine le modele uniquement"
    echo "  $0 --app-only   # Lance l'app (modele doit exister)"
    echo "  $0 --test       # Execute les tests"
}

# =============================================================================
# PARSING DES ARGUMENTS
# =============================================================================

while [[ $# -gt 0 ]]; do
    case $1 in
        --train-only)
            TRAIN_ONLY=true
            shift
            ;;
        --app-only)
            APP_ONLY=true
            shift
            ;;
        --test)
            RUN_TESTS=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo "Option inconnue: $1"
            show_help
            exit 1
            ;;
    esac
done

# =============================================================================
# MAIN
# =============================================================================

print_header "Exercice 5 : ML Prediction Service NYC Taxi"
echo "Periode : Juin - Aout 2025 (3 mois)"
echo ""
echo -e "${YELLOW}RAPPEL: L'utilisation de Python natif est strictement interdite.${NC}"
echo -e "${YELLOW}        Utilisation obligatoire de uv pour l'environnement virtuel.${NC}"
echo ""

TOTAL_STEPS=5
if $RUN_TESTS; then
    TOTAL_STEPS=3
elif $TRAIN_ONLY; then
    TOTAL_STEPS=4
elif $APP_ONLY; then
    TOTAL_STEPS=3
fi

# -----------------------------------------------------------------------------
# Etape 1 : Verification de uv
# -----------------------------------------------------------------------------
print_step 1 $TOTAL_STEPS "Verification de uv..."

if ! command -v uv &> /dev/null; then
    log_warn "uv n'est pas installe. Installation en cours..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.cargo/bin:$PATH"

    if ! command -v uv &> /dev/null; then
        log_error "Impossible d'installer uv. Installez-le manuellement: https://github.com/astral-sh/uv"
    fi
fi

UV_VERSION=$(uv --version 2>&1 || echo "version inconnue")
log_success "uv disponible: $UV_VERSION"

# -----------------------------------------------------------------------------
# Etape 2 : Installation des dependances avec uv
# -----------------------------------------------------------------------------
print_step 2 $TOTAL_STEPS "Installation des dependances avec uv..."

log_info "Synchronisation de l'environnement virtuel..."
uv sync

log_success "Dependances installees"

# -----------------------------------------------------------------------------
# Mode Tests
# -----------------------------------------------------------------------------
if $RUN_TESTS; then
    print_step 3 $TOTAL_STEPS "Execution des tests unitaires..."

    echo ""
    log_info "Verification PEP 8 avec flake8..."
    uv run flake8 src/ --max-line-length=100 --ignore=E501,W503 || log_warn "Quelques violations PEP 8 detectees"

    echo ""
    log_info "Execution des tests pytest..."
    uv run pytest tests/ -v --tb=short

    print_header "Tests termines"
    log_success "Tous les tests ont passe!"
    exit 0
fi

# -----------------------------------------------------------------------------
# Mode App Only
# -----------------------------------------------------------------------------
if $APP_ONLY; then
    print_step 3 $TOTAL_STEPS "Verification du modele..."

    if [ ! -f "models/latest_model.joblib" ]; then
        log_error "Modele non trouve. Executez d'abord: ./run_ex05.sh --train-only"
    fi
    log_success "Modele trouve"

    print_header "Lancement de l'application Streamlit"
    echo ""
    echo -e "${GREEN}L'application va s'ouvrir dans votre navigateur.${NC}"
    echo ""
    echo "URL : http://localhost:8502"
    echo ""
    echo "Appuyez sur Ctrl+C pour arreter le serveur."
    echo "=========================================="

    uv run streamlit run app.py --server.port 8502 --server.headless false
    exit 0
fi

# -----------------------------------------------------------------------------
# Etape 3 : Verification des donnees
# -----------------------------------------------------------------------------
print_step 3 $TOTAL_STEPS "Verification des donnees..."

DATA_PATH="../ex01_data_retrieval/data/raw"
if [ ! -d "$DATA_PATH" ]; then
    log_error "Repertoire de donnees non trouve: $DATA_PATH"
fi

PARQUET_COUNT=$(ls -1 "$DATA_PATH"/*.parquet 2>/dev/null | wc -l | tr -d ' ')
if [ "$PARQUET_COUNT" -eq 0 ]; then
    log_error "Aucun fichier Parquet trouve dans $DATA_PATH"
fi

log_success "$PARQUET_COUNT fichier(s) Parquet trouve(s)"

# -----------------------------------------------------------------------------
# Etape 4 : Entrainement du modele
# -----------------------------------------------------------------------------
print_step 4 $TOTAL_STEPS "Entrainement du modele ML..."

echo ""
log_info "Demarrage de l'entrainement..."
log_info "Modele: Gradient Boosting Regressor"
log_info "Target: total_amount (tarif total)"
echo ""

uv run python src/train.py --source local --data-path "$DATA_PATH" --model-type gradient_boosting

if [ ! -f "models/latest_model.joblib" ]; then
    log_error "L'entrainement a echoue - modele non genere"
fi

log_success "Modele entraine et sauvegarde"

if $TRAIN_ONLY; then
    print_header "Entrainement termine"
    echo ""
    echo "Modele sauvegarde dans: models/latest_model.joblib"
    echo ""
    echo "Prochaines etapes:"
    echo "  - Lancer l'app: ./run_ex05.sh --app-only"
    echo "  - Executer les tests: ./run_ex05.sh --test"
    echo ""
    log_success "EXERCICE 5 - ENTRAINEMENT TERMINE!"
    exit 0
fi

# -----------------------------------------------------------------------------
# Etape 5 : Lancement de l'application Streamlit
# -----------------------------------------------------------------------------
print_step 5 $TOTAL_STEPS "Lancement de l'application Streamlit..."

print_header "Application de Demonstration"
echo ""
echo -e "${GREEN}L'application va s'ouvrir dans votre navigateur.${NC}"
echo ""
echo "URL : http://localhost:8502"
echo ""
echo "Fonctionnalites:"
echo "  - Prediction de tarif en temps reel"
echo "  - Analyse de l'impact de la distance"
echo "  - Analyse de l'impact de l'heure"
echo ""
echo "Appuyez sur Ctrl+C pour arreter le serveur."
echo "=========================================="

uv run streamlit run app.py --server.port 8502 --server.headless false
