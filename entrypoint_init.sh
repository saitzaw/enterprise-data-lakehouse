#!/bin/bash

# entrypoint_init.sh - Initialize Airflow on first run
# This ensures Airflow database is properly set up

set -e

AIRFLOW_HOME=${AIRFLOW_HOME:-/opt/airflow}
export AIRFLOW_HOME

echo "=========================================="
echo "Airflow Initialization"
echo "=========================================="
echo "AIRFLOW_HOME: $AIRFLOW_HOME"
echo "AIRFLOW_DB: ${AIRFLOW__CORE__SQL_ALCHEMY_CONN}"

# Function to wait for database
wait_for_postgres() {
    local retries=30
    local host=${1:-postgres}
    local port=${2:-5432}
    
    echo "Waiting for PostgreSQL at $host:$port..."
    while [ $retries -gt 0 ]; do
        if pg_isready -h "$host" -p "$port" -U sparkuser >/dev/null 2>&1; then
            echo "PostgreSQL is ready!"
            return 0
        fi
        retries=$((retries - 1))
        sleep 2
    done
    
    echo "ERROR: PostgreSQL failed to respond after 60 seconds"
    return 1
}

# Wait for PostgreSQL
wait_for_postgres

# Initialize database
echo ""
echo "Initializing Airflow database..."
airflow db init

# Upgrade database (in case of migrations)
echo "Applying database migrations..."
airflow db upgrade

# Check if admin user exists, if not create it
echo ""
echo "Checking admin user..."
if ! airflow users list | grep -q "^admin"; then
    echo "Creating admin user..."
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin123 || true
    echo "Admin user created (password: admin123)"
else
    echo "Admin user already exists"
fi

# Validate configuration
echo ""
echo "Validating Airflow configuration..."
airflow config validate

echo ""
echo "=========================================="
echo "Airflow initialization complete!"
echo "=========================================="
