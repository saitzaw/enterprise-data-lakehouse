# Quick Start Guide - Docker Setup

## ⚡ TL;DR - Quick Start (5 minutes)

```bash
# 1. Setup environment (one-time)
bash docker_setup.sh

# 2. Access services
# Airflow:     http://localhost:8088  (admin / admin123)
# Spark:       http://localhost:8080
# Jupyter:     http://localhost:8888
```

---

## 📋 Prerequisites

- Docker and Docker Compose installed
- 16GB RAM minimum
- Linux or WSL 2

---

## 🚀 Full Setup Steps

### Step 1: Initial Setup (First Time Only)

```bash
# Go to project directory
cd /home/alex/MyProjects/enterprise-data-lakehouse

# Run automated setup
bash docker_setup.sh
```

The script will:
- ✅ Copy environment files from samples
- ✅ Set Docker GID for Airflow
- ✅ Build Docker images
- ✅ Initialize PostgreSQL databases
- ✅ Create Airflow configuration
- ✅ Start all services

### Step 2: Verify Services are Running

```bash
# Check all containers
docker compose ps

# Expected output: All services should show "Up"
```

### Step 3: Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow UI** | http://localhost:8088 | `admin` / `admin123` |
| **Spark Master** | http://localhost:8080 | - |
| **Spark History** | http://localhost:18080 | - |
| **Jupyter** | http://localhost:8888 | See logs for token |
| **MinIO** | http://localhost:9000 | `admin` / `password123` |
| **Kafka Control** | http://localhost:9021 | - |

---

## 📝 Common Commands

### Service Management

```bash
# Start all services
docker compose up -d

# Stop all services
docker compose down

# View service status
docker compose ps

# View logs for specific service
docker logs airflow-webserver -f
docker logs spark-master -f
```

### Using Makefile (Shortcuts)

```bash
# View all Makefile targets
make help

# Start scheduler setup (Airflow + Spark + PostgreSQL)
make dev-scheduler-up

# Start all services except Elasticsearch
make dev-up-all-services

# Stop all services
make dev-down-all-services

# Access shells
make dev-spark-shell          # Access Spark master
make dev-postgres-shell       # Access PostgreSQL
make dev-airflow-shell        # Access Airflow webserver
```

### Airflow DAG Management

```bash
# List all DAGs
docker compose run --rm airflow-webserver dags list

# Show DAG details
docker compose run --rm airflow-webserver dags show <dag_id>

# Trigger a DAG
docker compose run --rm airflow-webserver dags trigger <dag_id>

# View DAG parsing errors
docker compose run --rm airflow-webserver dags list-import-errors
```

### Database Management

```bash
# Access PostgreSQL shell
make dev-postgres-shell

# Inside PostgreSQL:
psql -U sparkuser -d sparkdb

# List databases
\l

# List tables
\dt

# List users
\du

# Exit
\q
```

---

## 🔧 Troubleshooting

### Issue: "postgres not found" / "connection refused"

```bash
# Start PostgreSQL first
docker compose up -d postgres

# Wait a moment and try again
docker compose up -d
```

### Issue: Airflow UI shows "No DAGs"

```bash
# Check for DAG parsing errors
docker logs airflow-scheduler | grep -i error

# Force re-parsing
docker compose run --rm airflow-webserver dags reserialize
```

### Issue: "DOCKER_GID" environment variable warning

```bash
# The docker_setup.sh script handles this automatically
# Or manually add to .env.airflow:
echo "DOCKER_GID=$(getent group docker | cut -d: -f3)" >> .env.airflow
```

### For more issues, see [DOCKER_TROUBLESHOOTING.md](DOCKER_TROUBLESHOOTING.md)

---

## 📊 Testing the Setup

### Test Spark

```bash
# Access Spark shell
make dev-spark-shell

# Run a test command
spark-submit --version
```

### Test Airflow

```bash
# Open browser to http://localhost:8088
# Login with admin / admin123
# You should see DAGs in the list

# Or check via CLI
docker compose run --rm airflow-webserver dags list
```

### Test PostgreSQL

```bash
# Access PostgreSQL
make dev-postgres-shell

# Inside postgres shell:
psql -U sparkuser -d sparkdb
SELECT version();
\q
```

### Test Jupyter

```bash
# Get Jupyter token
docker logs spark-jupyter 2>&1 | grep token

# Open browser to http://localhost:8888
# Paste the token
```

---

## 📁 Project Structure

```
enterprise-data-lakehouse/
├── docker-compose.yml          # Docker service definitions
├── airflow.cfg                 # Airflow configuration
├── docker_setup.sh             # Automated setup script
├── .env                        # Environment variables
├── .env.spark                  # Spark-specific env vars
├── .env.airflow                # Airflow-specific env vars
├── dags/                       # Airflow DAGs
│   ├── bronze/                 # Raw data ingestion
│   ├── silver/                 # Data cleaning & transformation
│   └── gold/                   # Business-ready tables
├── spark-apps/                 # Spark applications
│   └── pyspark/src/           # Python Spark jobs
├── notebooks/                  # Jupyter notebooks
└── init-sql/                   # SQL initialization scripts
```

---

## 🔐 Security Notes

### Default Credentials (Development Only)

**⚠️ DO NOT USE IN PRODUCTION**

| Service | Username | Password |
|---------|----------|----------|
| PostgreSQL | `sparkuser` | `s3cureP@ssw0rd` |
| PostgreSQL (Airflow) | `airflow` | `airflowpass` |
| MinIO | `admin` | `password123` |
| Airflow | `admin` | `admin123` |

### For Production

1. Change all default passwords in `.env` files
2. Use secrets management (Vault, AWS Secrets Manager, etc.)
3. Enable SSL/TLS for services
4. Restrict network access
5. Use private container registries
6. Enable authentication for all services

---

## 📚 Next Steps

1. **Explore Airflow DAGs**
   - Navigate to http://localhost:8088
   - Click on a DAG to see its structure
   - Trigger a DAG to test

2. **Test Spark Jobs**
   - Submit a sample job: `make dev-spark-shell`
   - Run: `spark-submit --version`

3. **Explore Data**
   - Use Jupyter: http://localhost:8888
   - Access PostgreSQL: `make dev-postgres-shell`
   - Check MinIO: http://localhost:9000

4. **Read Documentation**
   - [Main README](README.md)
   - [Copilot Instructions](.github/copilot-instructions.md)
   - [Kafka Setup Guide](spark-apps/pyspark/src/KAFKA_SETUP.md)
   - [Docker Troubleshooting](DOCKER_TROUBLESHOOTING.md)

---

## 💡 Tips

- Use `make dev-scheduler-up` for typical development (Airflow + Spark)
- Use `make dev-jupyter-up` for data analysis (Jupyter + Spark)
- Use `make dev-streaming-up` for Kafka testing
- Keep environment files (`.env*`) out of git - they contain credentials

---

## 🆘 Still Having Issues?

1. Check [DOCKER_TROUBLESHOOTING.md](DOCKER_TROUBLESHOOTING.md)
2. View logs: `docker compose logs <service>`
3. Rebuild: `docker compose down && docker compose build --no-cache && docker compose up`
4. Complete reset: `bash docker_setup.sh` (removes and recreates everything)
