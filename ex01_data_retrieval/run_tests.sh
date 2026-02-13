#!/bin/bash
# =============================================================================
# run_tests.sh - Script d'exécution des tests unitaires pour l'Exercice 1
# =============================================================================
#
# Description:
#   Exécute la suite de tests unitaires MainSpec avec ScalaTest.
#   Configure automatiquement Java 17 et les options JVM nécessaires.
#
# Usage:
#   ./run_tests.sh              # Exécute tous les tests
#   ./run_tests.sh --verbose    # Mode verbose avec détails
#   ./run_tests.sh --watch      # Mode watch (relance à chaque modification)
#
# Auteur: Camille Bezet, Brasa Franklin, Kenmogne Loic, Martin Soares Flavio
# Version: 1.0.0
# =============================================================================

set -e

# =============================================================================
# CONFIGURATION
# =============================================================================

# Force Java 17 (requis pour Spark 3.5.0)
export JAVA_HOME=$(/usr/libexec/java_home -v 17)

# Codes couleur ANSI
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly BLUE='\033[0;34m'
readonly NC='\033[0m'

# =============================================================================
# FONCTIONS
# =============================================================================

##
# Affiche l'aide
##
show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --help, -h      Affiche cette aide"
    echo "  --verbose, -v   Mode verbose avec détails des tests"
    echo "  --watch, -w     Mode watch (relance automatique)"
    echo "  --coverage, -c  Génère un rapport de couverture"
    echo ""
    echo "Exemples:"
    echo "  $0              Exécute tous les tests"
    echo "  $0 --verbose    Affiche les détails de chaque test"
    echo "  $0 --watch      Relance les tests à chaque modification"
}

##
# Vérifie les prérequis
##
check_prerequisites() {
    echo -e "${BLUE}[INFO]${NC} Vérification des prérequis..."

    # Vérifier Java
    if ! java -version &> /dev/null; then
        echo -e "${RED}✗ Java n'est pas installé${NC}"
        exit 1
    fi

    # Vérifier SBT
    if ! command -v sbt &> /dev/null; then
        echo -e "${RED}✗ SBT n'est pas installé${NC}"
        exit 1
    fi

    echo -e "${GREEN}✓${NC} Prérequis OK (Java $(java -version 2>&1 | head -1 | cut -d'"' -f2))"
}

# =============================================================================
# MAIN
# =============================================================================

echo "=========================================="
echo "Tests Unitaires - Exercice 1"
echo "Collecte des données NYC Taxi (Juin-Août 2025)"
echo "=========================================="
echo "Java Home: $JAVA_HOME"
echo ""

check_prerequisites

# Parse arguments
VERBOSE=""
WATCH=""
COVERAGE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            show_help
            exit 0
            ;;
        --verbose|-v)
            VERBOSE="true"
            shift
            ;;
        --watch|-w)
            WATCH="true"
            shift
            ;;
        --coverage|-c)
            COVERAGE="true"
            shift
            ;;
        *)
            echo -e "${RED}Option inconnue: $1${NC}"
            show_help
            exit 1
            ;;
    esac
done

# Exécution des tests
echo ""
echo -e "${YELLOW}[TEST]${NC} Lancement des tests..."
echo "=========================================="

if [[ "$WATCH" == "true" ]]; then
    echo -e "${BLUE}[INFO]${NC} Mode watch activé. Ctrl+C pour arrêter."
    sbt "~test"
elif [[ "$COVERAGE" == "true" ]]; then
    echo -e "${BLUE}[INFO]${NC} Génération du rapport de couverture..."
    sbt clean coverage test coverageReport
    echo ""
    echo -e "${GREEN}✓${NC} Rapport généré dans target/scala-2.12/scoverage-report/"
else
    sbt test
fi

EXIT_CODE=$?

# Résultat
echo ""
echo "=========================================="
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✓ TOUS LES TESTS SONT PASSÉS !${NC}"
else
    echo -e "${RED}✗ CERTAINS TESTS ONT ÉCHOUÉ${NC}"
fi
echo "=========================================="

exit $EXIT_CODE
