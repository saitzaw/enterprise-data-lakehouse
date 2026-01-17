# Simplified Financial Reporting Data Lakehouse

## Objective
**Clean, validated financial and sales reporting data pipeline with strict data quality controls at each stage.**

### What We Keep ✅
- **Bronze Zone**: Raw SAP sales/customer data (Parquet)
- **Silver Zone**: Cleaned, deduplicated data with quality validation (Delta Lake)
- **Gold Zone**: ClickHouse for fast financial reporting queries
- **dbt**: SQL transformations with data quality tests
- **Airflow**: Orchestration of data pipeline
- **PostgreSQL**: Metadata and reference data
- **Data Quality**: dbt tests at each stage (Bronze → Silver → Gold)

### What We Remove ❌
- **MongoDB**: Not needed for financial reporting
- **Elasticsearch**: Not needed for structured financial data
- **Kafka/Debezium**: Too complex for reporting-focused pipeline
- **Jupyter Notebooks**: Development tool, not production pipeline
- **Extra SAP modules**: Focus only on Sales (SD) + minimal master data

---

## Architecture: 3-Zone Medallion + Quality Gates

```
┌─────────────────────────────────────────────────────────────────┐
│                     SAP Source Systems                           │
│               (VBAK, VBAP, KNA1, MARA Tables)                    │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ↓
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE ZONE: Raw Ingestion (Parquet)                           │
│  ├─ Spark jobs (spark_submit via BashOperator)                  │
│  ├─ Table: sap_bronze_sales_header, sap_bronze_sales_lines      │
│  ├─ Table: sap_bronze_customers, sap_bronze_materials           │
│  └─ Quality Tests: Duplicates, nulls, freshness, row counts     │
│     Tests: test_bronze_data_quality.sql (6 tests)               │
└────────────┬──────────────────────────────────────────────────┬─┘
             │ FAIL: Alert & Stop                  │ PASS: Proceed
             │                                      │
        [Investigation]                             ↓
                                     ┌──────────────────────────────┐
                                     │ SILVER ZONE: Cleaned Data     │
                                     │ (Delta Lake + SCD Type 2)     │
                                     │ ├─ dbt transforms + tests     │
                                     │ ├─ Deduplication             │
                                     │ ├─ Dimension lookups         │
                                     │ ├─ Amount validation         │
                                     │ └─ Date/time consistency     │
                                     │ Tests: test_silver_quality    │
                                     │ (10 tests)                    │
                                     └────────┬──────────┬───────────┘
                                              │ FAIL     │ PASS
                                         [Investigation]  │
                                                          ↓
                                     ┌──────────────────────────────┐
                                     │ GOLD ZONE: Financial Reports │
                                     │ (ClickHouse OLAP)             │
                                     │ ├─ Fact: sales_orders         │
                                     │ ├─ Dim: customers, dates      │
                                     │ ├─ dbt materializes tables    │
                                     │ └─ Reconciliation tests       │
                                     │ Tests: test_gold_quality      │
                                     │ (10 tests)                    │
                                     └────────┬──────────┬───────────┘
                                              │ FAIL     │ PASS
                                         [Investigation] │
                                                          ↓
                                     ┌──────────────────────────────┐
                                     │ BI LAYER: Reporting          │
                                     │ (Grafana/Metabase)           │
                                     │ └─ Financial dashboards      │
                                     └──────────────────────────────┘
```

---

## Data Quality Framework

### Bronze Layer Quality (Raw Data Validation)
| Test | Purpose | Failure Action |
|------|---------|-----------------|
| Duplicate Check | No duplicate source records | Stop ingestion, investigate SAP |
| Null Check | Core fields populated | Alert, manual review |
| Freshness Check | Data < 90 days old | Warning log |
| Numeric Range | Qty/Amount within bounds | Alert on outliers |
| Date Validity | No future-dated records | Stop, fix timestamp |
| Row Count Threshold | Min 100 rows per table | Alert if too few |

**Tests File**: `sap_dbt/tests/bronze/test_bronze_data_quality.sql`

### Silver Layer Quality (Cleaned Data Validation)
| Test | Purpose | Failure Action |
|------|---------|-----------------|
| No Duplicates | Surrogate keys unique | Investigate dedup logic |
| Header-Line Match | All orders have lines | Alert, investigate cancellations |
| Amount Reconciliation | Header = sum of lines | Fix rounding, investigate discrepancies |
| Customer Valid | All customers in master | Stop, require master data first |
| Currency Valid | Expected SAP codes only | Stop, investigate data |
| Date Consistency | No future dates | Stop, fix source data |
| SCD2 Tracking | Effective dates sequential | Audit, fix versioning |
| No Nulls in Amounts | Financial metrics complete | Stop, investigate source |
| Material Master Match | Materials in master | Alert, add to material master |
| Data Freshness | < 24 hours old | Alert, check source refresh |

**Tests File**: `sap_dbt/tests/silver/test_silver_data_quality.sql`

### Gold Layer Quality (Reporting Data Validation)
| Test | Purpose | Failure Action |
|------|---------|-----------------|
| Fact Reconciliation | Count/Amount match source | Alert, audit reconciliation |
| Revenue Totals | Sum matches Silver source | Stop, investigate transform |
| No Nulls in Metrics | All financial fields valid | Stop, fix transform |
| Dimension Completeness | No orphaned fact records | Stop, check dimension keys |
| No Neg Amounts | Except documented returns | Alert if > 5% negative |
| Segment Distribution | All customers segmented | Alert, check segmentation logic |
| Time Consistency | Dates match dimension | Stop, fix date transform |
| Revenue Anomalies | Flag > 50% month variance | Alert for investigation |
| Dimension Metrics | LTV, tenure calculated | Stop, fix aggregation |
| No Fact Duplicates | Unique order keys | Stop, fix surrogate key logic |

**Tests File**: `sap_dbt/tests/gold/test_gold_data_quality.sql`

---

## Data Pipeline (Simplified)

### Daily Execution Schedule
```
02:00 UTC - Airflow Scheduler Starts
├─ 02:00 - Bronze Ingestion (Spark Job)
│  ├─ Read SAP tables (VBAK, VBAP, KNA1, MARA)
│  ├─ Write to Parquet (Bronze zone)
│  └─ Run: dbt test --select tag:bronze_quality
│     └─ FAIL → Stop pipeline, alert
│     └─ PASS → Continue
│
├─ 02:30 - Silver Transformation (dbt)
│  ├─ Deduplication
│  ├─ Dimensional lookups
│  ├─ Amount validation
│  ├─ Write to Delta (Silver zone, SCD2)
│  └─ Run: dbt test --select tag:silver_quality
│     └─ FAIL → Stop, investigation
│     └─ PASS → Continue
│
├─ 03:00 - Gold Analytics (dbt + ClickHouse)
│  ├─ Create fact_sales_orders_ch
│  ├─ Create dim_customers_ch
│  ├─ Create dim_date_ch
│  └─ Run: dbt test --select tag:gold_quality
│     └─ FAIL → Alert, BI dashboards stale
│     └─ PASS → Dashboards refreshed
│
└─ 03:30 - Pipeline Complete
```

---

## Data Cleaning Procedures

### Bronze → Silver: Deduplication & Cleaning

**Remove Duplicate Orders**
```python
# In dbt Silver model
SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY document_number, document_date ORDER BY ingestion_timestamp DESC) as rn
FROM {{ source('bronze', 'sap_bronze_sales_header') }}
WHERE rn = 1  -- Keep only latest version
```

**Clean Invalid Records**
```sql
-- Exclude cancelled orders if not needed for reporting
WHERE order_status NOT IN ('CANCELLED', 'REJECTED', 'VOID')
  AND document_date <= CURRENT_DATE  -- No future dates
  AND company_code IS NOT NULL  -- Required field
```

**Standardize Data**
```sql
-- Trim whitespace
SELECT TRIM(customer_name) as customer_name

-- Uppercase currency codes
SELECT UPPER(currency_code) as currency_code

-- Round amounts to 2 decimals
SELECT ROUND(document_amount, 2) as document_amount
```

### Silver → Gold: Aggregation & Enrichment

**Calculate Metrics**
```sql
-- Customer Lifetime Value
SELECT
    customer_id,
    COUNT(*) as total_orders,
    SUM(order_amount) as total_sales_value,
    AVG(order_amount) as avg_order_value,
    MAX(order_date) - MIN(order_date) as tenure_days
FROM sales_orders
GROUP BY customer_id
```

**Segment Customers**
```sql
-- Create segments for reporting
CASE
    WHEN total_sales_value >= 100000 THEN 'PREMIUM'
    WHEN total_sales_value >= 50000 THEN 'GOLD'
    WHEN total_sales_value >= 10000 THEN 'SILVER'
    ELSE 'STANDARD'
END as customer_segment
```

**Add Time Dimensions**
```sql
-- Extract year/month for reporting
SELECT
    EXTRACT(YEAR FROM order_date) as order_year,
    EXTRACT(MONTH FROM order_date) as order_month,
    TO_CHAR(order_date, 'YYYY-MM') as order_year_month
```

---

## dbt Project Structure (Focused)

```
sap_dbt/
├── dbt_project.yml           # Project config
├── profiles.yml              # ClickHouse target only
├── models/
│   ├── bronze/               # Bronze layer staging
│   │   └── stg_*.sql         # Raw table definitions
│   ├── silver/               # Silver layer transformations
│   │   ├── sap_sales_order.sql       # Main sales order model
│   │   ├── sap_customer_master.sql   # Customer dimension
│   │   └── sap_material_master.sql   # Material dimension
│   └── gold/                 # Gold layer for reporting
│       ├── fact_sales_orders_ch.sql  # Sales fact (ClickHouse)
│       ├── dim_customers_ch.sql      # Customer dimension
│       ├── dim_date_ch.sql           # Date dimension
│       └── sources.yml               # Source definitions
├── tests/
│   ├── bronze/
│   │   └── test_bronze_data_quality.sql    # 6 tests
│   ├── silver/
│   │   └── test_silver_data_quality.sql    # 10 tests
│   └── gold/
│       └── test_gold_data_quality.sql      # 10 tests
└── macros/                   # Custom dbt macros for testing
    └── test_*.sql            # Shared test definitions
```

---

## Execution & Monitoring

### Run Full Data Quality Pipeline
```bash
# Bronze ingestion + quality tests
make dev-bronze-ingest

# Silver transformation + quality tests
make dev-silver-transform

# Gold analytics + quality tests
make dev-gold-analytics

# Or all at once (with dependency checks)
make dev-pipeline-full
```

### Monitor Data Quality
```bash
# View test results
dbt test --select tag:bronze_quality --target clickhouse

dbt test --select tag:silver_quality --target clickhouse

dbt test --select tag:gold_quality --target clickhouse

# Detailed report
dbt docs generate
open target/index.html
```

### Alert on Quality Failures
```bash
# Airflow DAG includes failure notifications
- Email alerts when tests fail
- Slack integration for team notification
- Automatic stop-and-investigate mode
```

---

## Removed Components

**Why Removed:**
| Component | Reason |
|-----------|--------|
| Elasticsearch | Not needed for financial reporting |
| MongoDB | Reference data in PostgreSQL sufficient |
| Kafka/Debezium CDC | Reporting doesn't need real-time, daily batch ok |
| Jupyter Notebooks | Development tool, remove from production |
| FICO/COPA modules | Extra SAP modules not in scope for reporting |
| MinIO S3 | Use Delta Lake only for simplicity |

---

## Next Steps

1. ✅ Create dbt test files (Bronze, Silver, Gold)
2. ✅ Deploy simplified architecture
3. ⏳ Run Bronze tests → Fix data quality issues
4. ⏳ Run Silver tests → Verify transformations
5. ⏳ Run Gold tests → Validate reporting tables
6. ⏳ Build BI dashboards on ClickHouse
7. ⏳ Schedule Airflow DAG with alert notifications

---

## Key Files
- Bronze quality tests: `sap_dbt/tests/bronze/test_bronze_data_quality.sql`
- Silver quality tests: `sap_dbt/tests/silver/test_silver_data_quality.sql`
- Gold quality tests: `sap_dbt/tests/gold/test_gold_data_quality.sql`
- Orchestration: `dags/gold/clickhouse_gold_transformation.py`
- Execution guide: `CLICKHOUSE_EXECUTION_GUIDE.md`
