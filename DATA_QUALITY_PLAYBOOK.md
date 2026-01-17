# Data Quality Control & Cleaning Playbook

**Target**: Clean, validated financial reporting data pipeline with NO unnecessary components

---

## Overview

This playbook defines **3-stage data quality gates** that ensure clean, accurate financial data at each pipeline stage:

1. **Bronze Stage**: Validate raw SAP data
2. **Silver Stage**: Clean and deduplicate
3. **Gold Stage**: Reconcile for reporting

---

## Stage 1: Bronze Layer Quality Control

### What Happens in Bronze
- Raw SAP data ingested as Parquet
- **No transformations yet** - just validation
- dbt tests run BEFORE data enters Silver

### Bronze Data Quality Tests

**Test 1: Duplicate Detection**
```sql
-- Check for exact duplicates in raw SAP data
SELECT document_number, COUNT(*) as occurrences
FROM sap_bronze_sales_header
GROUP BY document_number
HAVING COUNT(*) > 1;
-- PASS: No duplicates or only latest versions
-- FAIL: Investigate SAP export process
```

**Test 2: Required Field Validation**
```sql
-- Ensure core business identifiers present
SELECT COUNT(*) as missing_required_fields
FROM sap_bronze_sales_header
WHERE document_number IS NULL 
   OR document_date IS NULL 
   OR customer_number IS NULL;
-- PASS: Count = 0
-- FAIL: Stop ingestion, investigate SAP export
```

**Test 3: Data Freshness**
```sql
-- Ensure data is recent (not > 90 days old)
SELECT MAX(document_date) as latest_date
FROM sap_bronze_sales_header;
-- PASS: Latest date is today or yesterday
-- FAIL: Alert on data refresh issue
```

**Test 4: Numeric Boundary Check**
```sql
-- Detect extreme/impossible values
SELECT COUNT(*) as suspicious_records
FROM sap_bronze_sales_header
WHERE document_amount < -999999  -- Extreme negatives
   OR document_amount > 999999999;  -- Extreme positives
-- PASS: < 1% of records
-- FAIL: Investigate data, may need manual review
```

**Test 5: Date Logic**
```sql
-- No future-dated records (system time issues)
SELECT COUNT(*) as future_dated
FROM sap_bronze_sales_header
WHERE document_date > CURRENT_DATE();
-- PASS: Count = 0
-- FAIL: Fix system dates, re-export
```

**Test 6: Minimum Row Count**
```sql
-- Ensure complete export (not partial)
SELECT COUNT(*) as row_count
FROM sap_bronze_sales_header;
-- PASS: > 1000 rows (adjust threshold based on your volume)
-- FAIL: Alert on incomplete extraction
```

### Bronze Cleaning Actions
| Issue | Action | Owner |
|-------|--------|-------|
| Duplicates | Dedup in SQL (keep latest) | Data Engineer |
| Missing required fields | Filter out incomplete records | Data Engineer |
| Stale data | Escalate to SAP support | SAP Admin |
| Extreme values | Flag for manual audit | Business Analyst |
| Future dates | Reset system time, re-export | SAP Admin |
| Low row count | Verify SAP table, re-export | Data Engineer |

---

## Stage 2: Silver Layer Quality Control

### What Happens in Silver
- **Deduplication**: Remove duplicates (keep latest version)
- **Data enrichment**: Add lookups (customer master, material master)
- **Standardization**: Trim, uppercase, round numbers
- **Validation**: Check amount reconciliation, date logic
- **SCD Type 2**: Track customer/material changes over time

### Silver Data Quality Tests

**Test 1: No Duplicate Surrogate Keys**
```sql
SELECT COUNT(*) as total, COUNT(DISTINCT sales_order_key) as unique_keys
FROM sap_silver_sales_order;
-- PASS: total = unique_keys (no duplicates)
-- FAIL: Fix deduplication logic
```

**Test 2: Header-Line Amount Reconciliation**
```sql
SELECT h.document_number, 
       h.document_amount as header_total,
       SUM(l.line_amount) as lines_total,
       ABS(h.document_amount - SUM(l.line_amount)) as diff
FROM sap_silver_sales_header h
LEFT JOIN sap_silver_sales_lines l ON h.sales_order_key = l.sales_order_key
GROUP BY h.document_number, h.document_amount
HAVING ABS(h.document_amount - SUM(l.line_amount)) > 0.01;
-- PASS: Empty result (all amounts match)
-- FAIL: Investigate source data or transformation logic
```

**Test 3: All Orders Have Lines**
```sql
SELECT h.document_number
FROM sap_silver_sales_header h
LEFT JOIN sap_silver_sales_lines l ON h.sales_order_key = l.sales_order_key
WHERE l.sales_order_key IS NULL AND h.order_status NOT IN ('CANCELLED', 'REJECTED');
-- PASS: Empty result (all active orders have lines)
-- FAIL: Check data ingestion, may have missing line items
```

**Test 4: Customer Master Validation**
```sql
SELECT COUNT(*) as orphaned_records
FROM sap_silver_sales_order o
LEFT JOIN sap_silver_customer_master c ON o.customer_number = c.customer_number
WHERE c.customer_number IS NULL;
-- PASS: Count = 0 (all customers in master)
-- FAIL: Load customer master first, or flag missing customers
```

**Test 5: Material Master Validation**
```sql
SELECT COUNT(*) as missing_materials
FROM sap_silver_sales_lines l
LEFT JOIN sap_silver_material_master m ON l.material_number = m.material_number
WHERE m.material_number IS NULL AND l.material_number IS NOT NULL;
-- PASS: Count = 0 (all materials in master)
-- FAIL: Load material master, investigate missing materials
```

**Test 6: Currency Code Standardization**
```sql
SELECT DISTINCT currency_code
FROM sap_silver_sales_order
WHERE currency_code NOT IN ('USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD');
-- PASS: Empty result (only valid codes)
-- FAIL: Investigate unknown currencies, standardize
```

**Test 7: SCD Type 2 Date Sequence**
```sql
SELECT c1.customer_number, c1.version, c1.end_date, c2.start_date
FROM sap_silver_customer_master c1
JOIN sap_silver_customer_master c2 
  ON c1.customer_number = c2.customer_number 
  AND c1.version < c2.version
WHERE c2.start_date != DATE_ADD(c1.end_date, INTERVAL 1 DAY);
-- PASS: Empty result (dates are sequential)
-- FAIL: Fix SCD2 date logic
```

**Test 8: No Nulls in Financial Amounts**
```sql
SELECT COUNT(*) as null_amounts
FROM sap_silver_sales_order
WHERE document_amount IS NULL OR currency_code IS NULL;
-- PASS: Count = 0
-- FAIL: Fix null handling in transformation
```

**Test 9: No Negative Amounts (Except Returns)**
```sql
SELECT COUNT(*) as unexpected_negatives
FROM sap_silver_sales_order
WHERE document_amount < 0 AND order_type NOT IN ('RETURN', 'CREDIT_MEMO');
-- PASS: Count = 0
-- FAIL: Investigate source data, may indicate data quality issue
```

**Test 10: Data Freshness**
```sql
SELECT MAX(document_date) as latest_date,
       CURRENT_DATE - MAX(document_date) as days_old
FROM sap_silver_sales_order;
-- PASS: days_old <= 1 (less than 1 day old)
-- FAIL: Alert on stale data, check upstream ingestion
```

### Silver Cleaning Actions
| Issue | Cleaning Step | Example SQL |
|-------|---|---|
| Duplicates | Keep latest version | `ROW_NUMBER() OVER (PARTITION BY key ORDER BY timestamp DESC)` |
| Null amounts | Filter out records | `WHERE amount IS NOT NULL` |
| Whitespace | Trim all strings | `SELECT TRIM(field_name)` |
| Case mismatch | Standardize case | `SELECT UPPER(currency_code)` |
| Precision loss | Round to 2 decimals | `SELECT ROUND(amount, 2)` |
| Missing lookups | Require master first | Add data dependency in DAG |
| Future dates | Filter out | `WHERE document_date <= CURRENT_DATE()` |
| Invalid codes | Validate against list | `WHERE currency_code IN ('USD', 'EUR', ...)` |

---

## Stage 3: Gold Layer Quality Control

### What Happens in Gold
- **Fact table creation**: Materialized in ClickHouse for reporting
- **Dimension enrichment**: Add customer metrics (LTV, segment)
- **Aggregation**: Pre-calculate all metrics used in reports
- **Reconciliation**: Ensure totals match Silver source
- **Audit trail**: Track all transformations

### Gold Data Quality Tests

**Test 1: Fact-Source Reconciliation**
```sql
-- Record count should match source (minus filtered-out records)
SELECT 
    (SELECT COUNT(*) FROM fact_sales_orders_ch) as fact_count,
    (SELECT COUNT(*) FROM sap_silver_sales_order 
     WHERE order_status NOT IN ('CANCELLED', 'REJECTED')) as source_count;
-- PASS: Counts match (or difference is documented)
-- FAIL: Investigate transformation logic
```

**Test 2: Revenue Total Reconciliation**
```sql
-- Sum of amounts should match source exactly
SELECT 
    (SELECT SUM(order_amount) FROM fact_sales_orders_ch) as fact_revenue,
    (SELECT SUM(document_amount) FROM sap_silver_sales_order 
     WHERE order_status NOT IN ('CANCELLED', 'REJECTED')) as source_revenue;
-- PASS: Amounts match (or < 0.01 difference due to rounding)
-- FAIL: Investigate calculation or filter logic
```

**Test 3: No Nulls in Metrics**
```sql
SELECT COUNT(*) as null_metrics
FROM fact_sales_orders_ch
WHERE order_amount IS NULL 
   OR quantity IS NULL 
   OR customer_id IS NULL 
   OR order_date IS NULL;
-- PASS: Count = 0 (all metrics populated)
-- FAIL: Fix transformation, cannot have nulls in fact table
```

**Test 4: Dimension Completeness (No Orphans)**
```sql
-- All fact records must have dimension records
SELECT COUNT(*) as orphaned_customers
FROM fact_sales_orders_ch f
LEFT JOIN dim_customers_ch c ON f.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
-- PASS: Count = 0 (all customers exist in dimension)
-- FAIL: Load dimension first, or add missing records
```

**Test 5: No Negative Amounts (Except Returns)**
```sql
SELECT COUNT(*) as unexpected_negatives,
       COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_sales_orders_ch) as pct_negative
FROM fact_sales_orders_ch
WHERE order_amount < 0 AND order_type NOT IN ('RETURN', 'CREDIT_MEMO');
-- PASS: pct_negative < 1% (minimal returns)
-- FAIL: If > 5%, investigate data quality
```

**Test 6: Customer Segment Distribution**
```sql
SELECT customer_segment, COUNT(*) as count, SUM(order_amount) as revenue
FROM fact_sales_orders_ch
GROUP BY customer_segment
ORDER BY revenue DESC;
-- PASS: All segments populated, reasonable distribution
-- FAIL: Investigate segmentation logic if skewed
```

**Test 7: Time Dimension Consistency**
```sql
-- Year/month extracted should match date dimension
SELECT COUNT(*) as mismatches
FROM fact_sales_orders_ch f
WHERE EXTRACT(YEAR FROM f.order_date) != f.order_year
   OR EXTRACT(MONTH FROM f.order_date) != f.order_month;
-- PASS: Count = 0
-- FAIL: Fix date extraction logic
```

**Test 8: Year-Over-Year Anomalies**
```sql
-- Flag if monthly revenue changes > 50% (may indicate data issue)
SELECT 
    order_year, order_month, 
    SUM(order_amount) as monthly_revenue,
    LAG(SUM(order_amount)) OVER (ORDER BY order_year, order_month) as prev_month,
    ROUND(100.0 * (SUM(order_amount) - LAG(SUM(order_amount)) OVER (ORDER BY order_year, order_month)) 
          / LAG(SUM(order_amount)) OVER (ORDER BY order_year, order_month), 2) as pct_change
FROM fact_sales_orders_ch
GROUP BY order_year, order_month
HAVING ABS(pct_change) > 50;  -- Flag > 50% changes
-- PASS: Empty result or documented anomalies
-- FAIL: Investigate business or data quality issues
```

**Test 9: Dimension Metrics Validation**
```sql
-- Customer segment should be calculated correctly
SELECT customer_id, total_sales_value, customer_segment
FROM dim_customers_ch
WHERE (total_sales_value >= 100000 AND customer_segment != 'PREMIUM')
   OR (total_sales_value >= 50000 AND total_sales_value < 100000 AND customer_segment != 'GOLD')
   OR (total_sales_value >= 10000 AND total_sales_value < 50000 AND customer_segment != 'SILVER')
   OR (total_sales_value < 10000 AND customer_segment != 'STANDARD');
-- PASS: Empty result (segmentation correct)
-- FAIL: Fix segmentation logic
```

**Test 10: No Fact Duplicates**
```sql
SELECT COUNT(*) as total_records, COUNT(DISTINCT order_key) as unique_keys
FROM fact_sales_orders_ch;
-- PASS: total = unique_keys (no duplicates)
-- FAIL: Fix surrogate key logic, may have duplicates
```

### Gold Cleaning Actions
| Issue | Resolution |
|-------|-----------|
| Count mismatch | Re-run transformation, validate filters |
| Amount mismatch | Check rounding logic, verify sums |
| Null metrics | Verify data availability in source |
| Orphaned records | Load missing dimensions, re-run |
| Negative amounts | Verify if returns, or investigate |
| Skewed distribution | Check segmentation logic |
| Anomalies | Alert business users for investigation |

---

## Execution Pipeline

### Daily Schedule
```
02:00 UTC - Bronze Ingestion
├─ Run: dbt test --select tag:bronze_quality
├─ Result FAIL → STOP, Alert team
└─ Result PASS → Continue

02:30 UTC - Silver Transformation
├─ Run: dbt run --select tag:silver
├─ Run: dbt test --select tag:silver_quality
├─ Result FAIL → STOP, Alert team
└─ Result PASS → Continue

03:00 UTC - Gold Analytics
├─ Run: dbt run --select tag:gold
├─ Run: dbt test --select tag:gold_quality
├─ Result FAIL → Alert (dashboards may be stale)
└─ Result PASS → Dashboards ready for reporting

03:30 UTC - Generate Audit Report
└─ Store test results for compliance
```

### Manual Execution
```bash
# Run all quality tests
make dev-dbt-test-gold

# Run specific layer
dbt test --select tag:bronze_quality --target clickhouse
dbt test --select tag:silver_quality --target clickhouse
dbt test --select tag:gold_quality --target clickhouse

# View test results
dbt docs generate  # HTML report
open target/index/test_results.html
```

---

## Data Quality Scorecard

Monitor these metrics daily:

| Metric | Bronze | Silver | Gold |
|--------|--------|--------|------|
| **Completeness** | % non-null core fields | % reconciled amounts | % populated metrics |
| **Uniqueness** | % unique records | 0 duplicates | 0 fact duplicates |
| **Consistency** | Date validity | Amount match header | YoY anomalies |
| **Accuracy** | Row count threshold | Lookup match % | Reconciliation % |
| **Freshness** | < 90 days old | < 24 hours old | < 24 hours old |

---

## What Gets Removed (Simplified)

### ❌ Components NOT in financial reporting pipeline:
- Kafka/Debezium (reporting doesn't need real-time)
- MongoDB (use PostgreSQL for reference data)
- Elasticsearch (use dbt + ClickHouse for search)
- Jupyter notebooks (development only, not production)
- Extra SAP modules (FICO, COPA - only SD in scope)
- MinIO (use Delta Lake directly)

### ✅ What Stays (Essential for reporting):
- Bronze (raw data ingestion)
- Silver (cleaned Delta Lake)
- Gold (ClickHouse for analytics)
- dbt (transformations + tests)
- Airflow (orchestration)
- PostgreSQL (metadata)

---

## Next Steps

1. ✅ Copy `data_cleaning_procedures.sql` to your development environment
2. ✅ Run Bronze quality tests: `dbt test --select tag:bronze_quality`
3. ✅ Fix any Bronze failures (duplicate removal, null handling)
4. ✅ Run Silver quality tests: `dbt test --select tag:silver_quality`
5. ✅ Fix any Silver failures (amount reconciliation, lookups)
6. ✅ Run Gold quality tests: `dbt test --select tag:gold_quality`
7. ✅ Schedule daily Airflow DAG with quality gates
8. ✅ Build BI dashboards once data passes all tests

---

## References
- Data Cleaning SQL: `sap_dbt/sql_procedures/data_cleaning_procedures.sql`
- Quality Tests: `sap_dbt/tests/bronze/`, `silver/`, `gold/`
- Architecture: `SIMPLIFIED_ARCHITECTURE.md`
- Execution Guide: `CLICKHOUSE_EXECUTION_GUIDE.md`
