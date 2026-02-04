#!/bin/bash
# =============================================================================
# run_ex01.sh - Script d'exécution pour l'Exercice 1
# =============================================================================
#
# Description:
#   Ce script automatise l'exécution de l'exercice 1 du projet Big Data.
#   Il vérifie les prérequis (Docker, MinIO, SBT) avant de lancer l'application
#   Spark qui télécharge et stocke les données NYC Taxi dans MinIO.
#
# Usage:
#   ./run_ex01.sh
#
# Prérequis:
#   - Docker Desktop en cours d'exécution
#   - MinIO démarré via docker-compose
#   - SBT (Scala Build Tool) installé
#   - Java 8, 11 ou 17 installé
#
# Auteur: Équipe Big Data CY Tech
# Version: 1.0.0
# =============================================================================

set -e  # Exit on error

# =============================================================================
# CONFIGURATION
# =============================================================================

# Force l'utilisation de Java 17 (requis pour Spark 3.5.0)
# Java 21+ n'est pas compatible avec Spark à cause de javax.security.auth.Subject
export JAVA_HOME=$(/usr/libexec/java_home -v 17)

# Codes couleur ANSI pour l'affichage
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly BLUE='\033[0;34m'
readonly NC='\033[0m'  # No Color

# =============================================================================
# FONCTIONS UTILITAIRES
# =============================================================================

##
# Affiche un message d'information
# @param $1 Message à afficher
##
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

##
# Affiche un message de succès
# @param $1 Message à afficher
##
log_success() {
    echo -e "${GREEN}✓${NC} $1"
}

##
# Affiche un message d'erreur et quitte
# @param $1 Message d'erreur
##
log_error() {
    echo -e "${RED}✗ $1${NC}" >&2
    exit 1
}

##
# Affiche un en-tête de section
# @param $1 Numéro de l'étape
# @param $2 Nombre total d'étapes
# @param $3 Description de l'étape
##
print_step() {
    echo -e "\n${YELLOW}[$1/$2]${NC} $3"
}

# =============================================================================
# VÉRIFICATIONS DES PRÉREQUIS
# =============================================================================

echo "=========================================="
echo "Exercice 1 : NYC Taxi Data → MinIO"
echo "Période : Juin - Août 2025 (3 mois)"
echo "=========================================="
echo "Java Home: $JAVA_HOME"

# Étape 1: Vérification de Docker
print_step 1 5 "Vérification de Docker..."
if ! docker ps > /dev/null 2>&1; then
    log_error "Docker n'est pas en cours d'exécution. Démarrez Docker Desktop et réessayez."
fi
log_success "Docker est actif"

# Étape 2: Vérification de MinIO
print_step 2 5 "Vérification de MinIO..."
if ! docker ps | grep -q minio; then
    log_error "MinIO n'est pas en cours d'exécution. Lancez: docker-compose up -d"
fi
log_success "MinIO est actif"

# Étape 3: Création du répertoire de données
print_step 3 5 "Création du dossier data/raw..."
mkdir -p data/raw
log_success "Dossier créé"

# Étape 4: Vérification de SBT
print_step 4 5 "Vérification de SBT..."
if ! command -v sbt &> /dev/null; then
    log_error "SBT n'est pas installé. Installation: https://www.scala-sbt.org/download.html"
fi
log_success "SBT est installé ($(sbt --version 2>&1 | head -1))"

# =============================================================================
# EXÉCUTION DU PROGRAMME
# =============================================================================

print_step 5 5 "Exécution du programme Spark..."
echo "=========================================="

# Se positionner dans le répertoire du script (ex01_data_retrieval)
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
    echo -e "${GREEN}✓ EXERCICE 1 TERMINÉ AVEC SUCCÈS !${NC}"
    echo "=========================================="
    echo ""
    echo "Fichiers collectés :"
    echo "  - yellow_tripdata_2025-06.parquet (Juin 2025)"
    echo "  - yellow_tripdata_2025-07.parquet (Juillet 2025)"
    echo "  - yellow_tripdata_2025-08.parquet (Août 2025)"
    echo ""
    echo "Prochaines étapes :"
    echo "  1. Vérifiez MinIO        : http://localhost:9001"
    echo "  2. Identifiants          : minio / minio123"
    echo "  3. Bucket                : nyc-raw"
    echo "  4. Passez à l'exercice 2"
else
    echo -e "${RED}✗ Une erreur s'est produite${NC}"
    echo "=========================================="
    echo ""
    echo "Dépannage :"
    echo "  - Vérifiez que MinIO est accessible: curl http://localhost:9000/minio/health/live"
    echo "  - Consultez les logs: docker logs minio"
    echo "  - Vérifiez Java: java -version (doit être 8, 11 ou 17)"
    exit 1
fi
