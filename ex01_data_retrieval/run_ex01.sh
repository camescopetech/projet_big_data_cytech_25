#!/bin/bash

# Script d'exécution automatique pour l'exercice 1
# Usage: ./run_ex01.sh

# Force Java 17 (required for Spark 3.5.0)
export JAVA_HOME=$(/usr/libexec/java_home -v 17)

echo "=========================================="
echo "Exercice 1 : NYC Taxi Data → MinIO"
echo "=========================================="

# Couleurs pour l'affichage
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Vérifier que Docker est en cours d'exécution
echo -e "\n${YELLOW}[1/5]${NC} Vérification de Docker..."
if ! docker ps > /dev/null 2>&1; then
    echo -e "${RED}✗ Docker n'est pas en cours d'exécution${NC}"
    echo "Démarrez Docker et réessayez"
    exit 1
fi
echo -e "${GREEN}✓${NC} Docker est actif"

# Vérifier que MinIO est en cours d'exécution
echo -e "\n${YELLOW}[2/5]${NC} Vérification de MinIO..."
if ! docker ps | grep minio > /dev/null; then
    echo -e "${RED}✗ MinIO n'est pas en cours d'exécution${NC}"
    echo "Lancez : docker-compose up -d"
    exit 1
fi
echo -e "${GREEN}✓${NC} MinIO est actif"

# Créer le dossier data/raw s'il n'existe pas
echo -e "\n${YELLOW}[3/5]${NC} Création du dossier data/raw..."
mkdir -p data/raw
echo -e "${GREEN}✓${NC} Dossier créé"

# Vérifier que SBT est installé
echo -e "\n${YELLOW}[4/5]${NC} Vérification de SBT..."
if ! command -v sbt &> /dev/null; then
    echo -e "${RED}✗ SBT n'est pas installé${NC}"
    echo "Installez SBT : https://www.scala-sbt.org/download.html"
    exit 1
fi
echo -e "${GREEN}✓${NC} SBT est installé"

# Exécuter le programme
echo -e "\n${YELLOW}[5/5]${NC} Exécution du programme Spark..."
echo "=========================================="
cd ex01_data_retrieval
sbt "run"

# Vérifier le résultat
if [ $? -eq 0 ]; then
    echo ""
    echo "=========================================="
    echo -e "${GREEN}✓ EXERCICE 1 TERMINÉ AVEC SUCCÈS !${NC}"
    echo "=========================================="
    echo ""
    echo "Prochaines étapes :"
    echo "1. Vérifiez MinIO : http://localhost:9001"
    echo "2. Bucket : nyc-raw"
    echo "3. Passez à l'exercice 2"
else
    echo ""
    echo "=========================================="
    echo -e "${RED}✗ Une erreur s'est produite${NC}"
    echo "=========================================="
    exit 1
fi