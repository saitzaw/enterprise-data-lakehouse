# ClickHouse Gold Layer - Execution Guide

## Quick Start

### 1. Start ClickHouse Service
```bash
# Option A: Using Makefile (recommended)
make dev-clickhouse-up

# Option B: Using docker-compose directly
docker compose up -d clickhouse

# Verify ClickHouse is running
docker compose ps | grep clickhouse
```

### 2. Verify ClickHouse Connectivity
```bash
# Access ClickHouse CLI
make dev-clickhouse-shell

# Once inside, test connection
SELECT 1;
SHOW DATABASES;
SHOW TABLES;
```

Expected output: Tables exist but are empty initially.

### 3. Run dbt Gold Transformations
```bash
# Option A: Using Makefile (recommended)
make dev-dbt-run-gold

# Option B: Using docker-compose directly
docker exec -it dbt bash
cd /opt/airflow/sap_dbt
dbt run --select tag:gold --target clickhouse

# Option C: With additional logging
docker exec -it dbt dbt run --select tag:gold --target clickhouse --debug
```

**Expected output:**
```
Running with dbt=1.x.x on ...
Found 3 models, X tests
...
Completed successfully
```

### 4. Run Data Quality Tests
```bash
# Option A: Using Makefile
make dev-dbt-test-gold

# Option B: Direct execution
docker exec -it dbt bash -c "cd /opt/airflow/sap_dbt && dbt test --select tag:gold --target clickhouse"
```

### 5. Generate dbt Documentation
```bash
make dev-dbt-docs-gold

# Access documentation
open http://localhost:8000  # If you've started dbt docs serve
```

---

## Verify Data Population

### Check Row Counts
```bash
# From ClickHouse CLI
make dev-clickhouse-shell

# Inside ClickHouse:
SELECT COUNT(*) as fact_sales_count FROM fact_sales_orders_ch;
SELECT COUNT(*) as customers_count FROM dim_customers_ch;
SELECT COUNT(*) as dates_count FROM dim_date_ch;
```

### Run Sample Analytical Queries
```bash
# Top 10 customers by sales
SELECT 
    customer_id,
    customer_name,
    total_orders,
    customer_segment,
    total_sales_value
FROM dim_customers_ch
ORDER BY total_sales_value DESC
LIMIT 10;

# Sales by month
SELECT 
    order_year,
    order_month,
    COUNT(*) as order_count,
    SUM(order_amount) as total_amount
FROM fact_sales_orders_ch
GROUP BY order_year, order_month
ORDER BY order_year DESC, order_month DESC;

# Sales by customer segment
SELECT 
    customer_segment,
    COUNT(*) as orders,
    SUM(order_amount) as total_sales,
    AVG(order_amount) as avg_order_value
FROM fact_sales_orders_ch
GROUP BY customer_segment
ORDER BY total_sales DESC;
```

---

## Schedule dbt with Airflow

### Option 1: Using Existing DAG (Recommended)
The DAG `dags/gold/clickhouse_gold_transformation.py` is already created and will:
- Run dbt models daily at 02:00 UTC
- Execute data quality tests
- Generate documentation
- Validate table population

**To enable in Airflow:**
```bash
# DAG is auto-discovered by Airflow at startup
# Verify it appears in Airflow UI
make dev-airflow-dags-list

# Trigger manually in Airflow UI
http://localhost:8088
# Search for "clickhouse_gold_transformation"
# Click "Trigger DAG"
```

### Option 2: Manual Trigger
```bash
# From Airflow scheduler container
docker exec airflow-scheduler airflow dags trigger clickhouse_gold_transformation
```

---

## Architecture Overview

### Data Flow
```
Silver Zone (Delta)          →          dbt Models          →          Gold Layer (ClickHouse)
├── sap_silver_sales_header                                  ├── fact_sales_orders_ch
├── sap_silver_sales_lines      → Multi-table joins      →  ├── dim_customers_ch
├── sap_silver_customers                                     └── dim_date_ch
└── sap_silver_materials
```

### Tables and Engines

| Table | Engine | Purpose | Rows | Compression |
|-------|--------|---------|------|-------------|
| `fact_sales_orders_ch` | MergeTree | Sales transactions (OLAP optimized) | M | 2-5x |
| `dim_customers_ch` | ReplacingMergeTree | Customer master (SCD2 tracking) | K | 1-2x |
| `dim_date_ch` | MergeTree | Date dimension (2020-2025+) | K | 10-20x |

### Model Details

**fact_sales_orders_ch** (150+ lines)
- **Purpose**: Analytical sales fact table
- **Dimensions**: customers, materials, dates
- **Metrics**: order_amount, quantity, customer_segment
- **Partitioning**: order_month (YYYY-MM)
- **Order By**: order_key, order_date (for performance)
- **CTEs**: base_sales_header → base_sales_lines → customer_master → material_master → joined_data → final

**dim_customers_ch** (100+ lines)
- **Purpose**: Customer enrichment with aggregated metrics
- **Metrics**: total_orders, total_sales_value, tenure_days, customer_segment
- **SCD2**: ReplacingMergeTree tracks customer changes over time
- **Segmentation**: PREMIUM/GOLD/SILVER/STANDARD based on sales value

**dim_date_ch** (100+ lines)
- **Purpose**: Standard date dimension for time-based analytics
- **Range**: 2020-01-01 to 2026-12-31
- **Flags**: fiscal_quarter, week_start, month_start, is_weekend, is_holiday
- **Pre-aggregated**: Performance benefit vs. calculating in fact queries

---

## Troubleshooting

### Issue: ClickHouse Container Won't Start
```bash
# Check logs
docker logs clickhouse

# Common causes:
# 1. Port conflict (8123, 9000, 9009)
docker ps | grep -E "8123|9000|9009"

# 2. Config file error
docker logs clickhouse | grep "Error"

# 3. Permissions
sudo chown -R $USER:$USER ./clickhouse/data ./clickhouse/config
```

### Issue: dbt Run Fails - "Connection refused"
```bash
# Verify ClickHouse is running
docker compose ps | grep clickhouse

# Check ClickHouse logs
docker logs clickhouse

# Verify network connectivity
docker exec dbt ping clickhouse

# Re-run dbt debug
make dev-dbt-debug
```

### Issue: dbt Tests Fail
```bash
# Run tests with verbose output
docker exec -it dbt dbt test --select tag:gold --target clickhouse --debug

# Check specific test
docker exec -it dbt dbt test --select test_name --target clickhouse

# Common test failures:
# - Duplicate keys: Check surrogate key logic
# - Not null: Verify upstream data
# - Foreign key: Check dimension table joins
```

### Issue: Empty Tables After dbt Run
```bash
# Verify Silver tables exist and have data
docker exec postgres psql -U sparkuser -d sparkdb -c "
  SELECT table_name FROM information_schema.tables 
  WHERE table_schema = 'public' AND table_name LIKE 'sap_silver_%';
"

# Check dbt run logs
docker logs dbt | tail -100

# Verify dbt profiles configured correctly
docker exec -it dbt dbt debug --target clickhouse
```

---

## Performance Tuning

### Query Optimization

1. **Check partition pruning** (ClickHouse reads fewer data blocks)
```sql
SELECT
    table,
    partition,
    active,
    rows,
    bytes
FROM system.parts
WHERE database = 'default'
ORDER BY bytes DESC;
```

2. **Analyze query performance** (built-in query log)
```sql
SELECT
    query_start_time,
    user,
    query,
    query_duration_ms,
    read_rows,
    result_rows
FROM system.query_log
WHERE database = 'default'
ORDER BY query_start_time DESC
LIMIT 10;
```

3. **Enable compression for fact tables**
```sql
ALTER TABLE fact_sales_orders_ch
MODIFY SETTING codec = 'ZSTD(3)';
```

### Incremental Loads

Replace full dbt model `dbt run` with incremental loads:

```sql
-- Example: Incremental fact table (only new dates)
SELECT *
FROM fact_sales_orders_ch
WHERE order_date > (SELECT MAX(order_date) FROM fact_sales_orders_ch)
```

Configure in dbt model:
```yaml
{{ config(
    materialized='incremental',
    unique_key='order_key',
    order_by='order_date',
    engine='MergeTree()',
    incremental_strategy='delete+insert'
) }}
```

---

## Integration with BI Tools

### Grafana
```
Data Source Configuration:
- Type: ClickHouse
- Host: localhost
- Port: 8123
- Database: default
- User: default
- Password: clickhousepass
- HTTP: Enabled
```

### Metabase
```
Database Configuration:
- Database Type: ClickHouse
- Host: localhost
- Port: 8123
- Database: default
- Username: default
- Password: clickhousepass
```

### Python Client
```python
import clickhouse_driver

client = clickhouse_driver.Client('localhost:9000')

# Query
result = client.execute('SELECT COUNT(*) FROM fact_sales_orders_ch')
print(f"Total sales orders: {result[0][0]}")

# Batch insert (if needed)
client.execute('INSERT INTO fact_sales_orders_ch VALUES', data)
```

---

## Next Steps

1. ✅ Start ClickHouse: `make dev-clickhouse-up`
2. ✅ Run dbt transformations: `make dev-dbt-run-gold`
3. ✅ Verify data: `make dev-clickhouse-shell` → `SELECT COUNT(*) FROM fact_sales_orders_ch`
4. ✅ Monitor DAG: Airflow UI → clickhouse_gold_transformation
5. ✅ Build dashboards: Connect Grafana/Metabase to ClickHouse
6. ✅ Optimize queries: Use performance tuning commands above
