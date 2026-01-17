# Docker & Airflow Troubleshooting Guide

## Quick Start

If you're experiencing Docker/Airflow issues, run the setup script:

```bash
bash docker_setup.sh
```

This will:
1. ✅ Copy environment files from samples
2. ✅ Set up Docker GID for Airflow
3. ✅ Build Docker images
4. ✅ Initialize PostgreSQL
5. ✅ Create Airflow config
6. ✅ Start all services

## Common Issues & Solutions

### Issue 1: DOCKER_GID Variable Not Set

**Error:**
```
WARNING: The "DOCKER_GID" variable is not set
```

**Solution:**
```bash
# Add to .env.airflow
echo "DOCKER_GID=$(getent group docker | cut -d: -f3)" >> .env.airflow

# Or run the setup script
bash docker_setup.sh
```

---

### Issue 2: Environment Files Missing

**Error:**
```
ERROR: .env.airflow not found
```

**Solution:**
```bash
# Copy sample files
cp env/sample.env .env
cp env/sample.env.spark .env.spark
cp env/sample.env.airflow .env.airflow
cp env/sample.env.minio .env.minio
```

---

### Issue 3: airflow.cfg Missing

**Error:**
```
FileNotFoundError: /opt/airflow/airflow.cfg
```

**Solution:**
The project now includes `airflow.cfg` at the root. If you need to recreate it:

```bash
# The file is provided in the repository
# If missing, copy it from the Docker image:
docker cp airflow-webserver:/opt/airflow/airflow.cfg ./airflow.cfg
```

---

### Issue 4: PostgreSQL Connection Error

**Error:**
```
sqlalchemy.exc.OperationalError: (psycopg2.OperationalError) could not translate host name "postgres" to address
```

**Solution:**
1. Ensure PostgreSQL is running:
   ```bash
   docker compose up -d postgres
   ```

2. Wait for PostgreSQL to be ready:
   ```bash
   docker exec postgres pg_isready -U sparkuser
   ```

3. Verify database and user exist:
   ```bash
   make dev-postgres-shell
   # Inside postgres:
   psql -U sparkuser -d sparkdb
   \l  # List databases
   \du # List users
   ```

4. Create Airflow database if missing:
   ```bash
   docker exec -i postgres psql -U sparkuser -d sparkdb << EOF
   CREATE DATABASE IF NOT EXISTS airflowdb;
   CREATE USER IF NOT EXISTS airflow WITH PASSWORD 'airflowpass';
   GRANT ALL PRIVILEGES ON DATABASE airflowdb TO airflow;
   EOF
   ```

---

### Issue 5: Airflow DAGs Not Showing

**Error:**
```
No DAGs in Airflow UI
```

**Solution:**

1. Check DAG parsing errors:
   ```bash
   make dev-airflow-logs | grep -i "error\|failed"
   ```

2. Verify DAG files exist:
   ```bash
   ls -la dags/bronze/
   ls -la dags/silver/
   ls -la dags/gold/
   ```

3. Check DAG syntax:
   ```bash
   docker compose run --rm airflow-webserver dags list
   ```

4. Force DAG parsing:
   ```bash
   docker compose run --rm airflow-webserver dags reserialize
   ```

---

### Issue 6: Airflow Webserver/Scheduler Stuck

**Error:**
```
Container keeps restarting or exits with code 1
```

**Solution:**

1. Check logs:
   ```bash
   docker logs airflow-webserver --tail 50
   docker logs airflow-scheduler --tail 50
   ```

2. Check database initialization:
   ```bash
   docker compose run --rm airflow-webserver db upgrade
   ```

3. Create admin user:
   ```bash
   docker compose run --rm airflow-webserver users create \
       --username admin \
       --firstname Admin \
       --lastname User \
       --role Admin \
       --email admin@example.com \
       --password admin123
   ```

4. Reset Airflow:
   ```bash
   # Clean state
   docker compose down
   rm -rf ./logs/*
   docker volume rm enterprise-data-lakehouse_airflow_logs
   docker compose up -d postgres redis
   docker compose run --rm airflow-webserver db reset --yes
   docker compose up -d
   ```

---

### Issue 7: Docker Socket Permission Denied

**Error:**
```
permission denied while trying to connect to Docker daemon socket
```

**Solution:**

1. Check Docker GID:
   ```bash
   getent group docker | cut -d: -f3
   ```

2. Update .env.airflow:
   ```bash
   echo "DOCKER_GID=<GID_FROM_ABOVE>" >> .env.airflow
   ```

3. Recreate containers:
   ```bash
   docker compose down
   docker compose up -d airflow-webserver airflow-scheduler
   ```

---

### Issue 8: Spark Master Connection Error

**Error:**
```
Failed to connect to spark://spark-master:7077
```

**Solution:**

1. Ensure Spark is running:
   ```bash
   docker compose up -d spark-master spark-worker
   ```

2. Verify connection:
   ```bash
   docker exec spark-master curl http://localhost:8080
   ```

3. Check environment variable:
   ```bash
   cat .env.spark | grep SPARK_MASTER
   # Should be: SPARK_MASTER=spark://spark-master:7077
   ```

---

### Issue 9: Redis Connection Error

**Error:**
```
ConnectionRefusedError: connection refused (redis:6379)
```

**Solution:**

1. Start Redis:
   ```bash
   docker compose up -d redis
   ```

2. Verify connection:
   ```bash
   docker compose run --rm redis redis-cli -h redis ping
   ```

---

### Issue 10: Insufficient Disk Space

**Error:**
```
no space left on device
```

**Solution:**

1. Clean up Docker resources:
   ```bash
   docker system prune -a --volumes
   ```

2. Remove volumes:
   ```bash
   docker compose down -v
   ```

3. Check disk space:
   ```bash
   df -h
   ```

---

## Complete Reset Procedure

If nothing works, do a complete reset:

```bash
# 1. Stop all containers
docker compose down

# 2. Remove volumes and images
docker volume rm $(docker volume ls -q | grep enterprise-data-lakehouse)
docker compose down --rmi all

# 3. Clean local data
rm -rf ./logs/* ./spark-logs/* ./kafka-data/* ./pg_data/*

# 4. Rebuild
docker compose build --no-cache

# 5. Run setup
bash docker_setup.sh
```

---

## Verification Checklist

After setup, verify everything is working:

```bash
# 1. Check all containers are running
docker compose ps
# All should show "Up"

# 2. Verify Airflow UI
curl http://localhost:8088
# Should return HTML

# 3. Verify Spark Master
curl http://localhost:8080
# Should return Spark UI HTML

# 4. Verify PostgreSQL
docker exec postgres pg_isready -U sparkuser
# Should respond: accepting connections

# 5. Verify Redis
docker compose run --rm redis redis-cli -h redis ping
# Should respond: PONG

# 6. Check Airflow logs
make dev-airflow-logs | tail -20

# 7. List DAGs
docker compose run --rm airflow-webserver dags list
```

---

## Performance Tuning

### For Local Development

```bash
# .env.airflow - Reduce resource usage
AIRFLOW__CORE__PARALLELISM=8
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=4
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=2
```

### For Production

```bash
# .env.airflow - Increase for production
AIRFLOW__CORE__PARALLELISM=32
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=16
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=8
```

---

## Getting Help

Check these locations for more information:

1. **Logs**:
   ```bash
   make dev-airflow-logs
   make dev-spark-logs
   docker logs postgres
   docker logs redis
   ```

2. **DAG Parsing**:
   ```bash
   docker compose run --rm airflow-webserver dags list-import-errors
   ```

3. **Database State**:
   ```bash
   make dev-postgres-shell
   # Inside: \d # show tables
   ```

4. **README**:
   - [Main README](../README.md)
   - [Copilot Instructions](.github/copilot-instructions.md)
