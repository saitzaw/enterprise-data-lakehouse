#!/bin/bash

# docker_setup.sh - Complete Docker environment setup
# Initializes environment files, databases, and Airflow configuration

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# =========================
# 1. Environment Files Setup
# =========================
setup_env_files() {
    print_info "Setting up environment files..."
    
    local env_files=(".env" ".env.spark" ".env.airflow" ".env.minio")
    
    for env_file in "${env_files[@]}"; do
        if [ -f "$env_file" ]; then
            print_warn "$env_file already exists, skipping..."
        else
            local sample_file="env/sample.$env_file"
            if [ ! -f "$sample_file" ]; then
                sample_file="env/sample.env${env_file:4}"  # Handle .env vs sample.env format
            fi
            
            if [ -f "$sample_file" ]; then
                cp "$sample_file" "$env_file"
                print_success "Created $env_file"
            else
                print_error "Sample file not found: $sample_file"
            fi
        fi
    done
}

# =========================
# 2. Docker GID Setup
# =========================
setup_docker_gid() {
    print_info "Setting up Docker GID..."
    
    local docker_gid=$(getent group docker | cut -d: -f3)
    
    if [ -z "$docker_gid" ]; then
        print_warn "Docker group not found, using default GID 999"
        docker_gid=999
    fi
    
    # Add to .env.airflow if not already present
    if ! grep -q "DOCKER_GID" .env.airflow; then
        echo "DOCKER_GID=$docker_gid" >> .env.airflow
        print_success "Added DOCKER_GID=$docker_gid to .env.airflow"
    else
        print_warn ".env.airflow already has DOCKER_GID"
    fi
}

# =========================
# 3. Build Docker Images
# =========================
build_images() {
    print_info "Building Docker images..."
    
    docker compose build --no-cache
    print_success "Docker images built successfully"
}

# =========================
# 4. Start Core Services
# =========================
start_core_services() {
    print_info "Starting core services (PostgreSQL, Redis, Spark)..."
    
    docker compose up -d postgres redis spark-master spark-worker
    
    # Wait for PostgreSQL to be ready
    print_info "Waiting for PostgreSQL to be ready..."
    sleep 5
    
    local retries=30
    while [ $retries -gt 0 ]; do
        if docker exec postgres pg_isready -U sparkuser >/dev/null 2>&1; then
            print_success "PostgreSQL is ready"
            break
        fi
        retries=$((retries - 1))
        sleep 2
    done
    
    if [ $retries -eq 0 ]; then
        print_error "PostgreSQL failed to start"
        return 1
    fi
}

# =========================
# 5. Initialize PostgreSQL
# =========================
init_postgres() {
    print_info "Initializing PostgreSQL databases..."
    
    # Create Airflow database
    print_info "Creating Airflow database..."
    docker exec -i postgres psql -U sparkuser -d sparkdb << EOF
CREATE DATABASE IF NOT EXISTS airflowdb;
CREATE USER IF NOT EXISTS airflow WITH PASSWORD 'airflowpass';
GRANT ALL PRIVILEGES ON DATABASE airflowdb TO airflow;
\c airflowdb
GRANT ALL ON SCHEMA public TO airflow;
EOF
    
    print_success "Airflow database created"
    
    # Load CRM seed data (optional)
    if [ -f "init-sql/SEED_CRM/01_dml_seed_crm_users_data.sql" ]; then
        print_info "Loading CRM seed data..."
        docker exec -i postgres psql -U sparkuser -d sparkdb < init-sql/SEED_CRM/01_dml_seed_crm_users_data.sql
        print_success "CRM seed data loaded"
    fi
}

# =========================
# 6. Create Airflow Config
# =========================
create_airflow_config() {
    print_info "Creating airflow.cfg..."
    
    if [ -f "airflow.cfg" ]; then
        print_warn "airflow.cfg already exists"
        return 0
    fi
    
    cat > airflow.cfg << 'EOF'
[core]
dags_folder = /opt/airflow/dags
base_log_folder = /opt/airflow/logs
base_cachedir = /opt/airflow/.cache

# SQLAlchemy connection string
sql_alchemy_conn = postgresql://airflow:airflowpass@postgres:5432/airflowdb
sql_engine_encoding = utf-8

# Celery configuration
broker_url = redis://redis:6379/0
celery_result_backend = redis://redis:6379/0

# Task execution
parallelism = 32
max_active_tasks_per_dag = 16
max_active_runs_per_dag = 16
load_examples = False
load_default_connections = False
unit_test_mode = False

# Executor
executor = CeleryExecutor

# Logging
log_level = INFO
fab_logging_level = WARNING

# Security
expose_config = False
hide_sensitive_var_conn_fields = True

[webserver]
expose_config = False
enable_proxy_fix = True
default_ui_timezone = UTC

[scheduler]
dag_dir_list_interval = 300
max_dagruns_to_create_per_loop = 10
max_tis_per_query = 512
catchup_by_default = False

[api]
auth_backends = airflow.api.auth.backend.default

[triggerer]
default_capacity = 1000
EOF
    
    print_success "Created airflow.cfg"
}

# =========================
# 7. Initialize Airflow DB
# =========================
init_airflow_db() {
    print_info "Initializing Airflow database..."
    
    docker compose run --rm airflow-webserver db init
    print_success "Airflow database initialized"
}

# =========================
# 8. Create Airflow Admin User
# =========================
create_airflow_admin() {
    print_info "Creating Airflow admin user..."
    
    docker compose run --rm airflow-webserver users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin123
    
    print_success "Airflow admin user created (username: admin, password: admin123)"
}

# =========================
# 9. Start All Services
# =========================
start_all_services() {
    print_info "Starting all services..."
    
    docker compose up -d
    
    print_info "Waiting for services to be ready..."
    sleep 10
    
    print_info "Service status:"
    docker compose ps
}

# =========================
# 10. Verification
# =========================
verify_services() {
    print_info "Verifying services..."
    
    local services=("postgres" "redis" "spark-master" "spark-worker" "airflow-webserver" "airflow-scheduler")
    local failed=0
    
    for service in "${services[@]}"; do
        if docker compose ps "$service" | grep -q "Up"; then
            print_success "$service is running"
        else
            print_error "$service is NOT running"
            failed=$((failed + 1))
        fi
    done
    
    if [ $failed -eq 0 ]; then
        print_success "All services are running!"
    else
        print_error "$failed service(s) failed to start"
        return 1
    fi
}

# =========================
# Main Flow
# =========================
main() {
    print_info "Starting Docker environment setup..."
    
    # Check if we're in the right directory
    if [ ! -f "docker-compose.yml" ]; then
        print_error "docker-compose.yml not found. Run this script from project root."
        exit 1
    fi
    
    echo ""
    setup_env_files
    echo ""
    setup_docker_gid
    echo ""
    
    read -p "Build Docker images? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        build_images
        echo ""
    fi
    
    start_core_services
    echo ""
    init_postgres
    echo ""
    create_airflow_config
    echo ""
    init_airflow_db
    echo ""
    create_airflow_admin
    echo ""
    start_all_services
    echo ""
    verify_services
    
    echo ""
    print_success "Setup complete!"
    echo ""
    echo -e "${BLUE}=== Access Points ===${NC}"
    echo "Airflow UI:     http://localhost:8088  (admin / admin123)"
    echo "Spark Master:   http://localhost:8080"
    echo "Spark History:  http://localhost:18080"
    echo "Jupyter:        http://localhost:8888  (check logs for token)"
    echo "MinIO:          http://localhost:9000"
    echo "Kafka Control:  http://localhost:9021"
    echo ""
    echo -e "${BLUE}=== Next Steps ===${NC}"
    echo "1. Access Airflow UI and verify DAGs are loading"
    echo "2. Check logs: make dev-airflow-logs"
    echo "3. Trigger a DAG from Airflow UI or CLI"
    echo ""
}

main "$@"
