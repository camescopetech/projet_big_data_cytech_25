#!/bin/bash
# =============================================================================
# Exercice 6 : Orchestration avec Apache Airflow
# =============================================================================
#
# Description:
#   Lance l'infrastructure Airflow pour orchestrer automatiquement
#   l'ensemble des exercices du projet Big Data NYC Taxi.
#
# Usage:
#   ./run_ex06.sh [start|stop|restart|status|logs]
#
# Prerequis:
#   - Docker et Docker Compose installes
#   - Les services de base (MinIO, PostgreSQL) doivent etre demarres
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
readonly NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

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
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_header() {
    echo ""
    echo "=========================================="
    echo "$1"
    echo "=========================================="
}

show_help() {
    echo "Usage: $0 [COMMANDE]"
    echo ""
    echo "Commandes disponibles:"
    echo "  start     Demarre Airflow et les services de base"
    echo "  stop      Arrete tous les services"
    echo "  restart   Redemarre tous les services"
    echo "  status    Affiche le statut des services"
    echo "  logs      Affiche les logs d'Airflow"
    echo "  init      Initialise Airflow (premiere utilisation)"
    echo "  help      Affiche cette aide"
    echo ""
    echo "Exemples:"
    echo "  $0 start   # Demarre tous les services"
    echo "  $0 logs    # Voir les logs"
}

check_docker() {
    if ! docker info > /dev/null 2>&1; then
        log_error "Docker n'est pas en cours d'execution"
        exit 1
    fi
    log_success "Docker est actif"
}

create_network() {
    if ! docker network inspect spark-network > /dev/null 2>&1; then
        log_info "Creation du reseau Docker spark-network..."
        docker network create spark-network
        log_success "Reseau cree"
    else
        log_info "Reseau spark-network existe deja"
    fi
}

start_base_services() {
    log_info "Demarrage des services de base (MinIO, PostgreSQL, Spark)..."
    cd "$PROJECT_ROOT"
    docker compose up -d minio postgres
    log_success "Services de base demarres"

    # Attendre que les services soient prets
    log_info "Attente de la disponibilite des services..."
    sleep 10
}

start_airflow() {
    log_info "Demarrage d'Airflow..."
    cd "$PROJECT_ROOT"
    docker compose -f docker-compose.yml -f ex06_airflow/docker-compose.airflow.yml up -d
    log_success "Airflow demarre"
}

stop_all() {
    log_info "Arret de tous les services..."
    cd "$PROJECT_ROOT"
    docker compose -f docker-compose.yml -f ex06_airflow/docker-compose.airflow.yml down
    log_success "Services arretes"
}

show_status() {
    print_header "Statut des Services"
    docker compose -f "$PROJECT_ROOT/docker-compose.yml" \
        -f "$PROJECT_ROOT/ex06_airflow/docker-compose.airflow.yml" ps
}

show_logs() {
    docker compose -f "$PROJECT_ROOT/docker-compose.yml" \
        -f "$PROJECT_ROOT/ex06_airflow/docker-compose.airflow.yml" \
        logs -f airflow-webserver airflow-scheduler
}

init_airflow() {
    print_header "Initialisation d'Airflow"

    # Creer les repertoires
    log_info "Creation des repertoires..."
    mkdir -p "$SCRIPT_DIR"/{dags,logs,plugins,config,scripts}
    chmod -R 777 "$SCRIPT_DIR"/{logs,plugins}

    # Creer le reseau
    create_network

    # Demarrer les services de base
    start_base_services

    # Demarrer Airflow
    log_info "Demarrage d'Airflow (premiere fois - peut prendre quelques minutes)..."
    cd "$PROJECT_ROOT"
    docker compose -f docker-compose.yml -f ex06_airflow/docker-compose.airflow.yml up -d

    log_info "Attente de l'initialisation d'Airflow..."
    sleep 30

    print_header "Airflow Initialise"
    echo ""
    echo "Interface Web Airflow: http://localhost:8080"
    echo "Identifiants: admin / admin"
    echo ""
    echo "Interface MinIO: http://localhost:9001"
    echo "Identifiants: minio / minio123"
    echo ""
}

# =============================================================================
# MAIN
# =============================================================================

print_header "Exercice 6 : Orchestration Airflow"

# Verifier Docker
check_docker

# Parser les arguments
case "${1:-start}" in
    start)
        create_network
        start_base_services
        start_airflow
        print_header "Services Demarres"
        echo ""
        echo -e "${GREEN}Airflow Web UI:${NC} http://localhost:8080"
        echo -e "${GREEN}Identifiants:${NC}   admin / admin"
        echo ""
        echo -e "${GREEN}MinIO Console:${NC}  http://localhost:9001"
        echo -e "${GREEN}Identifiants:${NC}   minio / minio123"
        echo ""
        echo -e "${GREEN}Spark Master:${NC}   http://localhost:8081"
        echo ""
        echo "Utilisez '$0 logs' pour voir les logs"
        ;;
    stop)
        stop_all
        ;;
    restart)
        stop_all
        sleep 5
        create_network
        start_base_services
        start_airflow
        log_success "Services redemarres"
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs
        ;;
    init)
        init_airflow
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        log_error "Commande inconnue: $1"
        show_help
        exit 1
        ;;
esac
