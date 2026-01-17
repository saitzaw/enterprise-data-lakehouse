-- Data Cleaning & Validation Procedures
-- Executed at each stage: Bronze → Silver → Gold

-- ============================================================================
-- STAGE 1: BRONZE LAYER - RAW DATA INGESTION CLEANING
-- ============================================================================

-- 1.1 Remove Duplicate Records
-- Problem: Same SAP record ingested multiple times
-- Solution: Keep only latest version per document
SELECT 
    * EXCEPT (row_num),
    ROW_NUMBER() OVER (
        PARTITION BY document_number, document_date 
        ORDER BY ingestion_timestamp DESC
    ) as row_num
FROM sap_bronze_sales_header
QUALIFY row_num = 1;  -- Keep only latest

-- 1.2 Validate Required Fields
-- Problem: Missing critical identifiers
-- Solution: Filter out incomplete records
SELECT *
FROM sap_bronze_sales_header
WHERE 
    document_number IS NOT NULL
    AND document_date IS NOT NULL
    AND customer_number IS NOT NULL
    AND company_code IS NOT NULL
    AND document_amount IS NOT NULL;

-- 1.3 Remove Future-Dated Records
-- Problem: Document dates in the future (system errors)
-- Solution: Keep only historical data
SELECT *
FROM sap_bronze_sales_header
WHERE document_date <= CURRENT_DATE();

-- 1.4 Trim Whitespace from String Fields
-- Problem: Extra spaces causing matching issues
-- Solution: Standardize text fields
SELECT 
    TRIM(document_number) as document_number,
    TRIM(customer_name) as customer_name,
    TRIM(company_code) as company_code,
    -- Keep other columns as-is
    *
FROM sap_bronze_sales_header
EXCEPT SELECT *
FROM sap_bronze_sales_header;  -- Replace in actual process

-- 1.5 Standardize Currency Codes
-- Problem: Mixed case currency codes
-- Solution: Uppercase all currency codes
SELECT 
    UPPER(currency_code) as currency_code,
    *
FROM sap_bronze_sales_header;

-- 1.6 Round Numeric Fields
-- Problem: Excess decimal places or precision loss
-- Solution: Round financial amounts to 2 decimals
SELECT 
    ROUND(document_amount, 2) as document_amount,
    ROUND(tax_amount, 2) as tax_amount,
    ROUND(net_amount, 2) as net_amount,
    *
FROM sap_bronze_sales_header;

-- ============================================================================
-- STAGE 2: SILVER LAYER - DATA DEDUPLICATION & ENRICHMENT
-- ============================================================================

-- 2.1 Deduplication with SCD Type 2 Tracking
-- Problem: Customer data changes (address, credit limits)
-- Solution: Track history with effective dates
WITH customer_changes AS (
    SELECT 
        customer_number,
        customer_name,
        country_code,
        credit_limit,
        CURRENT_TIMESTAMP as load_date,
        LAG(CONCAT(customer_name, '|', country_code, '|', credit_limit))
            OVER (PARTITION BY customer_number ORDER BY load_date) as prev_values
    FROM sap_bronze_customers
)
SELECT 
    customer_number,
    customer_name,
    country_code,
    credit_limit,
    load_date as start_date,
    LEAD(load_date) OVER (PARTITION BY customer_number ORDER BY load_date) as end_date,
    CASE 
        WHEN prev_values IS NULL THEN TRUE  -- First record
        WHEN CONCAT(customer_name, '|', country_code, '|', credit_limit) != prev_values THEN TRUE
        ELSE FALSE
    END as is_changed
FROM customer_changes;

-- 2.2 Validate Amount Reconciliation
-- Problem: Header total doesn't match sum of lines
-- Solution: Validate and flag discrepancies
SELECT 
    h.document_number,
    h.document_amount,
    SUM(l.line_amount) as lines_total,
    ABS(h.document_amount - SUM(l.line_amount)) as difference
FROM sap_sales_order_header h
LEFT JOIN sap_sales_order_lines l 
    ON h.sales_order_key = l.sales_order_key
GROUP BY h.document_number, h.document_amount
HAVING ABS(h.document_amount - SUM(l.line_amount)) > 0.01  -- More than 1 cent difference
ORDER BY difference DESC;

-- 2.3 Exclude Cancelled Orders (Optional - depends on reporting needs)
-- Problem: Cancelled orders inflate order counts and affect metrics
-- Solution: Filter cancelled orders OR track separately
SELECT *
FROM sap_sales_order_header
WHERE order_status NOT IN ('CANCELLED', 'REJECTED', 'VOID')
  AND document_date BETWEEN '2024-01-01' AND CURRENT_DATE();

-- 2.4 Clean Material Numbers
-- Problem: Leading/trailing spaces in material codes
-- Solution: Standardize material identifiers
SELECT 
    TRIM(material_number) as material_number,
    UPPER(material_type) as material_type,
    *
FROM sap_bronze_materials
WHERE material_number IS NOT NULL;

-- 2.5 Validate Customer-Material Combinations
-- Problem: Invalid or non-existent material-customer combinations
-- Solution: Join with master data and validate
SELECT 
    l.sales_order_key,
    l.material_number,
    l.line_amount
FROM sap_sales_order_lines l
INNER JOIN sap_material_master m
    ON l.material_number = m.material_number  -- Material must exist
WHERE m.material_number IS NOT NULL;

-- 2.6 Flag Suspicious Transactions
-- Problem: Negative amounts, extreme values that need investigation
-- Solution: Create data quality flags for audit trail
SELECT 
    document_number,
    line_amount,
    quantity,
    CASE 
        WHEN line_amount < 0 AND order_type NOT IN ('RETURN', 'CREDIT_MEMO') THEN 'SUSPICIOUS_NEGATIVE'
        WHEN ABS(line_amount) > 1000000 THEN 'EXTREME_AMOUNT'
        WHEN quantity > 100000 THEN 'EXTREME_QUANTITY'
        WHEN quantity < -999 THEN 'EXTREME_NEGATIVE_QTY'
        ELSE 'OK'
    END as data_quality_flag
FROM sap_sales_order_lines
WHERE data_quality_flag != 'OK';

-- 2.7 Standardize Date Formats
-- Problem: Inconsistent date formats
-- Solution: Convert to ISO format (YYYY-MM-DD)
SELECT 
    TO_DATE(document_date, 'YYYY-MM-DD') as document_date,
    TO_DATE(posting_date, 'YYYY-MM-DD') as posting_date,
    TO_DATE(delivery_date, 'YYYY-MM-DD') as delivery_date
FROM sap_sales_order_header
WHERE document_date IS NOT NULL;

-- ============================================================================
-- STAGE 3: GOLD LAYER - FINANCIAL REPORTING PREPARATION
-- ============================================================================

-- 3.1 Create Clean Fact Table with All Calculations
-- Problem: Raw data not suitable for reporting
-- Solution: Pre-calculate all metrics
CREATE OR REPLACE TABLE fact_sales_orders_clean AS
SELECT 
    -- Surrogate key
    MD5(CONCAT(h.sales_order_key, l.line_number)) as order_line_key,
    
    -- Business keys
    h.document_number as sales_order_number,
    l.line_number as line_item_number,
    
    -- Foreign keys
    h.customer_number as customer_id,
    l.material_number as material_id,
    h.company_code as company_id,
    
    -- Dates
    h.document_date as order_date,
    h.posting_date as posting_date,
    h.delivery_date as delivery_date,
    
    -- Amounts (rounded and validated)
    ROUND(l.line_amount, 2) as line_amount,
    ROUND(h.document_amount, 2) as order_total_amount,
    ROUND(l.unit_price, 2) as unit_price,
    ROUND(l.quantity, 2) as quantity,
    
    -- Calculated fields
    EXTRACT(YEAR FROM h.document_date) as order_year,
    EXTRACT(MONTH FROM h.document_date) as order_month,
    TO_CHAR(h.document_date, 'YYYY-MM') as order_year_month,
    EXTRACT(QUARTER FROM h.document_date) as order_quarter,
    
    -- Status
    h.order_status as order_status,
    l.line_status as line_status,
    
    -- Currency
    UPPER(h.currency_code) as currency_code,
    
    -- Audit fields
    CURRENT_TIMESTAMP as load_timestamp,
    h.last_updated as source_last_updated
FROM sap_sales_order_header h
INNER JOIN sap_sales_order_lines l 
    ON h.sales_order_key = l.sales_order_key
WHERE 
    -- Quality filters
    h.document_date <= CURRENT_DATE()
    AND h.order_status NOT IN ('CANCELLED', 'REJECTED', 'VOID')
    AND l.line_amount IS NOT NULL
    AND h.document_amount IS NOT NULL;

-- 3.2 Create Clean Customer Dimension
-- Problem: Customer data scattered, no aggregates
-- Solution: Build enriched customer dimension
CREATE OR REPLACE TABLE dim_customers_clean AS
SELECT 
    c.customer_number as customer_id,
    TRIM(c.customer_name) as customer_name,
    TRIM(c.country_code) as country_code,
    c.credit_limit as credit_limit,
    UPPER(c.currency_code) as currency_code,
    
    -- Aggregated metrics from sales
    COUNT(DISTINCT f.sales_order_number) as total_orders,
    SUM(f.line_amount) as total_sales_value,
    AVG(f.line_amount) as avg_order_value,
    MAX(f.order_date) as last_order_date,
    MIN(f.order_date) as first_order_date,
    DATEDIFF(DAY, MIN(f.order_date), MAX(f.order_date)) as tenure_days,
    
    -- Customer segmentation
    CASE 
        WHEN SUM(f.line_amount) >= 100000 THEN 'PREMIUM'
        WHEN SUM(f.line_amount) >= 50000 THEN 'GOLD'
        WHEN SUM(f.line_amount) >= 10000 THEN 'SILVER'
        ELSE 'STANDARD'
    END as customer_segment,
    
    -- Risk flag
    CASE 
        WHEN c.credit_limit < SUM(f.line_amount) THEN 'HIGH_RISK'
        WHEN c.credit_limit < SUM(f.line_amount) * 1.5 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END as credit_risk,
    
    CURRENT_TIMESTAMP as load_timestamp
FROM sap_customer_master c
LEFT JOIN fact_sales_orders_clean f 
    ON c.customer_number = f.customer_id
GROUP BY 
    c.customer_number, c.customer_name, c.country_code, 
    c.credit_limit, c.currency_code;

-- 3.3 Remove Outliers (Optional)
-- Problem: Extreme outliers affecting reporting accuracy
-- Solution: Identify and flag (don't delete, audit trail needed)
SELECT 
    document_number,
    line_amount,
    quantity,
    ROUND(line_amount / STDDEV(line_amount) OVER (), 2) as z_score
FROM fact_sales_orders_clean
WHERE ABS(line_amount / STDDEV(line_amount) OVER ()) > 3  -- Beyond 3 std dev
ORDER BY z_score DESC;

-- 3.4 Validate Currency Consistency
-- Problem: Mixed currencies in same order
-- Solution: Flag orders with multiple currencies
SELECT 
    document_number,
    COUNT(DISTINCT currency_code) as currency_count,
    STRING_AGG(DISTINCT currency_code, ',') as currencies
FROM fact_sales_orders_clean
GROUP BY document_number
HAVING COUNT(DISTINCT currency_code) > 1;

-- 3.5 Validate Date Sequence
-- Problem: Delivery date before order date
-- Solution: Flag data quality issues
SELECT 
    document_number,
    order_date,
    posting_date,
    delivery_date,
    CASE 
        WHEN posting_date < order_date THEN 'ERROR: Posting before order'
        WHEN delivery_date < order_date THEN 'ERROR: Delivery before order'
        WHEN delivery_date < posting_date THEN 'WARNING: Delivery before posting'
        ELSE 'OK'
    END as date_quality
FROM fact_sales_orders_clean
WHERE date_quality != 'OK';

-- 3.6 Final Cleanliness Check
-- Problem: Unknown data quality issues
-- Solution: Count records by quality status
SELECT 
    SUM(CASE WHEN line_amount IS NULL THEN 1 ELSE 0 END) as null_amounts,
    SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END) as null_quantities,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customers,
    SUM(CASE WHEN order_date > CURRENT_DATE() THEN 1 ELSE 0 END) as future_dates,
    SUM(CASE WHEN line_amount < 0 THEN 1 ELSE 0 END) as negative_amounts,
    COUNT(*) as total_records
FROM fact_sales_orders_clean;

-- ============================================================================
-- MONITORING & MAINTENANCE
-- ============================================================================

-- 4.1 Data Quality Scorecard
-- Problem: No visibility into overall data quality
-- Solution: Calculate quality metrics
SELECT 
    CURRENT_DATE as report_date,
    'Sales Orders' as table_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN line_amount IS NULL THEN 1 ELSE 0 END) as null_records,
    ROUND(100 - (100.0 * SUM(CASE WHEN line_amount IS NULL THEN 1 ELSE 0 END) / COUNT(*)), 2) as completeness_pct,
    COUNT(DISTINCT document_number) as unique_orders,
    SUM(line_amount) as total_revenue,
    MIN(order_date) as earliest_date,
    MAX(order_date) as latest_date
FROM fact_sales_orders_clean;

-- 4.2 Field-Level Completeness
-- Problem: Which fields have the most missing data?
-- Solution: Identify data quality issues by column
SELECT 
    'customer_id' as field_name,
    COUNT(*) as total,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_count,
    ROUND(100.0 * SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as null_pct
FROM fact_sales_orders_clean
UNION ALL
SELECT 
    'line_amount' as field_name,
    COUNT(*) as total,
    SUM(CASE WHEN line_amount IS NULL THEN 1 ELSE 0 END) as null_count,
    ROUND(100.0 * SUM(CASE WHEN line_amount IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as null_pct
FROM fact_sales_orders_clean
UNION ALL
SELECT 
    'order_date' as field_name,
    COUNT(*) as total,
    SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) as null_count,
    ROUND(100.0 * SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as null_pct
FROM fact_sales_orders_clean;
