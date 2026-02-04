#!/bin/bash
# =============================================================================
# Exercice 2 - Branche 2 : Exécution des tests unitaires
# =============================================================================

set -e

# Couleurs pour l'affichage
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "=========================================="
echo "Exercice 2 - Branche 2 : Tests Unitaires"
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
# Exécution des tests
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}[1/1]${NC} Exécution des tests..."
echo "=========================================="

cd "$(dirname "$0")"
sbt test

echo -e "\n=========================================="
echo -e "${GREEN}✓ TESTS TERMINÉS !${NC}"
echo "=========================================="
