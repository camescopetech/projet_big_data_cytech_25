#!/bin/bash
# =============================================================================
# run_tests.sh - Tests unitaires pour l'Exercice 2
# =============================================================================

set -e

export JAVA_HOME=$(/usr/libexec/java_home -v 17)

echo "=========================================="
echo "Tests Unitaires - Exercice 2"
echo "Nettoyage des données NYC Taxi (Juin-Août 2025)"
echo "=========================================="

# Se positionner dans le répertoire du script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

sbt test
EXIT_CODE=$?

echo ""
echo "=========================================="
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "\033[0;32m✓ TOUS LES TESTS SONT PASSÉS !\033[0m"
else
    echo -e "\033[0;31m✗ CERTAINS TESTS ONT ÉCHOUÉ\033[0m"
fi
echo "=========================================="

exit $EXIT_CODE
