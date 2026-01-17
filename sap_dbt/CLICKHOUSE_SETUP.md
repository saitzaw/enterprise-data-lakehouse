# ClickHouse + dbt Gold Layer Setup Guide

## Overview

This guide explains how to use **dbt** to transform Silver zone Delta tables into **ClickHouse** gold layer tables for fast analytical queries.

**Data Flow:**
```
Bronze (Parquet/Delta) 
  ↓ (Spark)
Silver (Delta - SCD2) 
  ↓ (dbt)
Gold (ClickHouse - OLAP)
  ↓
Analytics/BI Tools
```

---

## Why ClickHouse for Gold?

| Feature | Delta/Parquet | ClickHouse |
|---------|---|---|
| **Query Speed** | Good (sec) | Excellent (ms) |
| **Aggregations** | Slow on large datasets | Optimized for analytics |
| **Compression** | Standard | 10-100x better |
| **Updates** | Expensive | Efficient (ReplacingMergeTree) |
| **Use Case** | Data lake storage | OLAP analytics |

---

## Architecture

### Layer 1: Bronze Zone
```
Raw SAP data (Parquet/Delta)
├─ VBAK (sales order header)
├─ VBAP (sales order items)
├─ KNA1 (customers)
└─ MARA/MAKT (materials)
```

### Layer 2: Silver Zone
```
Cleaned data (Delta with SCD Type 2)
├─ sap_silver_sales_orders_header
├─ sap_silver_sales_orders_lines
├─ sap_silver_customers
└─ sap_silver_materials
```

### Layer 3: Gold Zone
```
ClickHouse analytics tables
├─ fact_sales_orders_ch (complex aggregations)
├─ dim_customers_ch (enriched dimension)
└─ dim_date_ch (date dimension)
```

---

## Prerequisites

### 1. Start ClickHouse

```bash
# If ClickHouse container isn't running
docker compose up -d clickhouse

# Verify it's running
docker exec clickhouse clickhouse-client -u default -p clickhousepass --query "SELECT 1"
```

### 2. Install dbt ClickHouse Adapter

```bash
# Inside your Python environment
pip install dbt-clickhouse

# Or in Docker:
make dev-dbt-shell
pip install dbt-clickhouse
```

### 3. Configure Profiles

The profiles are already created at:
- **Location**: `sap_dbt/profiles/profiles.yml`
- **Default target**: `clickhouse`
- **Credentials**: See file for host/user/password

---

## dbt Models in Gold Layer

### 1. Fact Table: Sales Orders (`fact_sales_orders_ch.sql`)

**Complex transformation that:**
- ✓ Joins sales header + line items + customer + material
- ✓ Aggregates totals and calculations
- ✓ Adds time dimensions (year/month/quarter)
- ✓ Creates dimensional keys
- ✓ Optimized for BI queries

**Engine**: `MergeTree()` (immutable, excellent for analytics)

**Partitioned by**: `order_month` (fast time-based filtering)

**Queries it enables:**
```sql
-- Sales by month
SELECT order_month, sum(total_amount) FROM fact_sales_orders_ch GROUP BY order_month

-- Customer analysis
SELECT customer_name, COUNT(*) as order_count, SUM(total_amount) 
FROM fact_sales_orders_ch 
GROUP BY customer_name ORDER BY order_count DESC
LIMIT 10

-- Product performance
SELECT material_desc, COUNT(*), AVG(order_qty)
FROM fact_sales_orders_ch
GROUP BY material_desc
```

### 2. Dimension Table: Customers (`dim_customers_ch.sql`)

**Features:**
- ✓ Enriched with order metrics (lifetime value, order count, tenure)
- ✓ Customer segmentation (PREMIUM/GOLD/SILVER/STANDARD)
- ✓ Uses `ReplacingMergeTree` for SCD Type 2 (tracks changes over time)

**Engine**: `ReplacingMergeTree()` (allows updates/replacements)

**Queries:**
```sql
-- Customer segments
SELECT customer_segment, COUNT(*), AVG(lifetime_order_value)
FROM dim_customers_ch
WHERE mandt = '100'
GROUP BY customer_segment

-- Top customers
SELECT customer_name, lifetime_order_value
FROM dim_customers_ch
ORDER BY lifetime_order_value DESC
LIMIT 20
```

### 3. Dimension Table: Date (`dim_date_ch.sql`)

**Features:**
- ✓ Standard date dimension (2020-2025+)
- ✓ Fiscal quarters
- ✓ Holiday flags
- ✓ Week/month boundaries

**Engine**: `MergeTree()` (immutable dimension)

**Usage in joins:**
```sql
SELECT d.month_name, COUNT(*) as order_count
FROM fact_sales_orders_ch f
JOIN dim_date_ch d ON f.order_date_key = d.date_key
GROUP BY d.month_name
```

---

## Running dbt Transformations

### 1. Develop & Test (Locally)

```bash
# Access dbt shell
make dev-dbt-shell

# Inside container:
cd /usr/local/dbt-project  # or /opt/airflow/sap_dbt

# Preview transformation (dry run)
dbt parse
dbt compile

# Build all gold models
dbt run --select tag:gold

# Build specific model
dbt run --select fact_sales_orders_ch
```

### 2. dbt Commands Reference

```bash
# Parse project and validate DAG
dbt parse

# Compile SQL (generate)
dbt compile --select gold

# Execute transformations
dbt run --select tag:gold

# Run with debug logging
dbt run --debug --select fact_sales_orders_ch

# Test data quality
dbt test --select tag:gold

# Clean artifacts
dbt clean

# Snapshot (track dimension changes)
dbt snapshot

# Full pipeline
dbt run && dbt test && dbt docs generate
```

### 3. Run via Airflow DAG

**Create DAG** (example):

```python
# dags/gold/clickhouse_transformations.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="clickhouse_gold_transformation",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 2 * * *",  # Daily at 2 AM UTC
    default_args=default_args,
    tags=["gold", "dbt", "clickhouse"]
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command='''
            bash -lc "
            cd /opt/airflow/sap_dbt && 
            dbt run --select tag:gold --target clickhouse --profiles-dir /opt/airflow/sap_dbt/profiles
            "
        '''
    )
    
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command='''
            bash -lc "
            cd /opt/airflow/sap_dbt && 
            dbt test --select tag:gold --target clickhouse --profiles-dir /opt/airflow/sap_dbt/profiles
            "
        '''
    )

    dbt_run >> dbt_test
```

---

## Querying ClickHouse

### Via CLI

```bash
# Access ClickHouse shell
docker exec -it clickhouse clickhouse-client -u default -p clickhousepass

# List tables
SHOW TABLES;

# Query a table
SELECT COUNT(*) FROM fact_sales_orders_ch;

# Sample data
SELECT * FROM dim_customers_ch LIMIT 5;

# Aggregation example
SELECT order_year, order_month, SUM(total_amount) as monthly_sales
FROM fact_sales_orders_ch
GROUP BY order_year, order_month
ORDER BY order_year, order_month;
```

### Via Python

```python
from clickhouse_driver import Client

client = Client('clickhouse', user='default', password='clickhousepass')

# Query
result = client.execute(
    'SELECT customer_segment, COUNT(*) FROM dim_customers_ch GROUP BY customer_segment'
)

for row in result:
    print(row)
```

### Via HTTP API

```bash
curl -X POST 'http://localhost:8123/' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'query=SELECT COUNT(*) FROM fact_sales_orders_ch' \
  -d 'user=default' \
  -d 'password=clickhousepass'
```

---

## Performance Tuning

### 1. ClickHouse Query Optimization

```sql
-- Check table structure
DESCRIBE TABLE fact_sales_orders_ch;

-- View system metrics
SELECT table, sum(bytes) FROM system.parts GROUP BY table;

-- Merge small parts
OPTIMIZE TABLE fact_sales_orders_ch;

-- Monitor slow queries
SELECT * FROM system.query_log WHERE user = 'default' LIMIT 10;
```

### 2. dbt Configuration

```yaml
# dbt_project.yml
models:
  sap_landing:
    gold:
      +materialized: table
      +engine: MergeTree()
      +order_by: ['date_key']
      +partition_by: ['toYYYYMM(date_key)']
      +strategy: append  # Don't drop/recreate
```

### 3. Compression

```sql
-- Check compression ratio
SELECT 
    table,
    sum(bytes) as original_bytes,
    sum(bytes_on_disk) as compressed_bytes,
    round(100.0 * sum(bytes_on_disk) / sum(bytes), 1) as compression_ratio
FROM system.parts
WHERE active = 1
GROUP BY table;
```

---

## Troubleshooting

### Issue 1: dbt-clickhouse Not Found

```bash
# Install inside dbt container
make dev-dbt-shell
pip install dbt-clickhouse

# Verify
dbt --version
```

### Issue 2: ClickHouse Connection Error

```bash
# Check ClickHouse is running
docker compose ps clickhouse

# Test connection
docker exec clickhouse clickhouse-client -u default -p clickhousepass --query "SELECT 1"

# Check logs
docker logs clickhouse | tail -20
```

### Issue 3: dbt Parse Errors

```bash
# Check profiles.yml syntax
dbt debug --target clickhouse

# Validate sources
dbt parse

# Check for Python errors
dbt run --debug
```

### Issue 4: Slow Queries

```sql
-- Check table statistics
SELECT table, sum(bytes) FROM system.parts GROUP BY table;

-- Force optimization
OPTIMIZE TABLE fact_sales_orders_ch;

-- Check query execution plan
EXPLAIN SELECT COUNT(*) FROM fact_sales_orders_ch;
```

---

## Advanced Topics

### Incremental Loads

```yaml
# dbt_project.yml config
models:
  gold:
    fact_sales_orders_ch:
      +materialized: incremental
      +unique_key: order_sk
      +on_schema_change: fail
```

### Snapshots (Track Changes)

```sql
-- In dbt snapshot YAML
snapshots:
  snap_customers:
    unique_key: customer_id
    updated_at: updated_at
    target_schema: snapshots
```

### Materialized Views

```sql
-- Alternative to tables (for live data)
CREATE MATERIALIZED VIEW sales_summary_mv AS
SELECT order_year, order_month, SUM(total_amount)
FROM fact_sales_orders_ch
GROUP BY order_year, order_month;
```

---

## Migration from Delta to ClickHouse

```bash
# 1. Run dbt to create ClickHouse tables
dbt run --select tag:gold

# 2. Verify data loads
dbt test --select tag:gold

# 3. Monitor ClickHouse size
docker exec clickhouse clickhouse-client -q "
  SELECT table, formatReadableSize(sum(bytes)) 
  FROM system.parts 
  GROUP BY table;"

# 4. Switch queries to ClickHouse
# Update your BI tools to query ClickHouse instead of Delta
```

---

## Next Steps

1. ✅ dbt models created (`fact_sales_orders_ch`, `dim_customers_ch`, `dim_date_ch`)
2. ⏳ Run dbt to populate ClickHouse: `dbt run --select tag:gold`
3. ⏳ Create Airflow DAG for daily refreshes
4. ⏳ Connect BI tool (Grafana, Metabase, etc.) to ClickHouse
5. ⏳ Monitor performance and optimize partitions

---

## References

- [dbt ClickHouse Adapter](https://github.com/ClickHouse/dbt-clickhouse)
- [ClickHouse SQL Reference](https://clickhouse.com/docs/en/sql-reference/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Medallion Architecture](https://www.databricks.com/blog/2022/06/24/five-simple-steps-improve-data-quality-dbt-and-delta-lake.html)
