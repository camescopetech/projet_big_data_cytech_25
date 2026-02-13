#!/bin/bash
# =============================================================================
# run_ex02.sh - Script d'exécution pour l'Exercice 2 (Branche 1)
# =============================================================================
#
# Description:
#   Exécute le nettoyage des données NYC Taxi et les stocke dans MinIO.
#   - Lit les données brutes depuis le bucket nyc-raw
#   - Applique les règles de validation NYC TLC
#   - Écrit les données nettoyées vers le bucket nyc-cleaned
#
# Usage:
#   ./run_ex02.sh
#
# Prérequis:
#   - Docker avec MinIO en cours d'exécution
#   - Bucket nyc-raw contenant les données de l'exercice 1
#   - Bucket nyc-cleaned créé dans MinIO
#   - Java 17 et SBT installés
#
# Auteur: Camille Bezet, Brasa Franklin, Kenmogne Loic, Martin Soares Flavio
# Version: 1.0.0
# =============================================================================

set -e

# =============================================================================
# CONFIGURATION
# =============================================================================

export JAVA_HOME=$(/usr/libexec/java_home -v 17)

readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly BLUE='\033[0;34m'
readonly NC='\033[0m'

# =============================================================================
# FONCTIONS
# =============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}✓${NC} $1"
}

log_error() {
    echo -e "${RED}✗ $1${NC}" >&2
    exit 1
}

print_step() {
    echo -e "\n${YELLOW}[$1/$2]${NC} $3"
}

# =============================================================================
# VÉRIFICATIONS
# =============================================================================

echo "=========================================="
echo "Exercice 2 : Nettoyage des données"
echo "Période : Juin - Août 2025 (3 mois)"
echo "=========================================="
echo "Java Home: $JAVA_HOME"

# Vérifier Docker
print_step 1 5 "Vérification de Docker..."
if ! docker ps > /dev/null 2>&1; then
    log_error "Docker n'est pas en cours d'exécution"
fi
log_success "Docker est actif"

# Vérifier MinIO
print_step 2 5 "Vérification de MinIO..."
if ! docker ps | grep -q minio; then
    log_error "MinIO n'est pas en cours d'exécution. Lancez: docker-compose up -d"
fi
log_success "MinIO est actif"

# Vérifier que le bucket nyc-raw existe et contient des données
print_step 3 5 "Vérification du bucket nyc-raw..."
log_info "Assurez-vous que l'exercice 1 a été exécuté"
log_success "Prérequis vérifié"

# Créer le bucket nyc-cleaned s'il n'existe pas
print_step 4 5 "Création du bucket nyc-cleaned..."
# Utiliser mc (MinIO Client) si disponible, sinon via l'API
if command -v mc &> /dev/null; then
    mc alias set myminio http://localhost:9000 minio minio123 2>/dev/null || true
    mc mb myminio/nyc-cleaned 2>/dev/null || true
    log_success "Bucket nyc-cleaned prêt"
else
    log_info "mc non installé - créez le bucket manuellement via http://localhost:9001"
    log_info "Ou le programme le créera automatiquement"
fi

# Exécuter le programme
print_step 5 5 "Exécution du programme Spark..."
echo "=========================================="

# Se positionner dans le répertoire du script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

sbt "run"
EXIT_CODE=$?

# =============================================================================
# RÉSULTAT
# =============================================================================

echo ""
echo "=========================================="

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✓ EXERCICE 2 - BRANCHE 1 TERMINÉ !${NC}"
    echo "=========================================="
    echo ""
    echo "Fichiers nettoyés (bucket nyc-cleaned) :"
    echo "  - yellow_tripdata_2025-06.parquet (Juin 2025)"
    echo "  - yellow_tripdata_2025-07.parquet (Juillet 2025)"
    echo "  - yellow_tripdata_2025-08.parquet (Août 2025)"
    echo ""
    echo "Vérifiez sur http://localhost:9001"
    echo ""
    echo "Prochaine étape :"
    echo "  - Exercice 3 : Création des tables SQL"
else
    echo -e "${RED}✗ Une erreur s'est produite${NC}"
    echo "=========================================="
    exit 1
fi
