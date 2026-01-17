-- Gold Layer Data Quality Tests
-- Tests for analytical tables - ensure business metrics are correct
-- Focus: Financial reporting accuracy and audit readiness

-- Test 1: Fact table reconciliation to source
-- Fact row count should match or be explainable (e.g., cancelled orders filtered)
SELECT
    'fact_sales_orders_ch' as table_name,
    (SELECT COUNT(*) FROM {{ ref('fact_sales_orders_ch') }}) as fact_count,
    (SELECT COUNT(*) FROM {{ ref('sap_sales_order') }} WHERE order_status != 'CANCELLED') as source_count,
    CASE 
        WHEN ABS(
            (SELECT COUNT(*) FROM {{ ref('fact_sales_orders_ch') }}) -
            (SELECT COUNT(*) FROM {{ ref('sap_sales_order') }} WHERE order_status != 'CANCELLED')
        ) > 10 THEN 'WARNING: Count mismatch'
        ELSE 'OK'
    END as reconciliation;

-- Test 2: Revenue totals match source
-- Sum of fact table amounts should equal source
SELECT
    (SELECT SUM(order_amount) FROM {{ ref('fact_sales_orders_ch') }}) as fact_revenue,
    (SELECT SUM(document_amount) FROM {{ ref('sap_sales_order') }} WHERE order_status != 'CANCELLED') as source_revenue,
    ABS(
        (SELECT SUM(order_amount) FROM {{ ref('fact_sales_orders_ch') }}) -
        (SELECT SUM(document_amount) FROM {{ ref('sap_sales_order') }} WHERE order_status != 'CANCELLED')
    ) as difference;

-- Test 3: No nulls in key metrics
-- Financial metrics cannot be null
SELECT *
FROM {{ ref('fact_sales_orders_ch') }}
WHERE
    order_amount IS NULL
    OR quantity IS NULL
    OR customer_id IS NULL
    OR order_date IS NULL;

-- Test 4: Dimension table completeness
-- All fact records must have matching dimension records
SELECT COUNT(*) as orphaned_customer_records
FROM {{ ref('fact_sales_orders_ch') }} f
LEFT JOIN {{ ref('dim_customers_ch') }} c
    ON f.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

SELECT COUNT(*) as orphaned_date_records
FROM {{ ref('fact_sales_orders_ch') }} f
LEFT JOIN {{ ref('dim_date_ch') }} d
    ON f.order_date = d.date_key
WHERE d.date_key IS NULL;

-- Test 5: No negative amounts (except for returns/credits)
-- Negative amounts should be rare and documented
SELECT
    COUNT(*) as negative_amount_records,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {{ ref('fact_sales_orders_ch') }}) as pct_negative
FROM {{ ref('fact_sales_orders_ch') }}
WHERE order_amount < 0
    AND order_type NOT IN ('RETURN', 'CREDIT_MEMO');

-- Test 6: Customer segment distribution is reasonable
-- All customers should be assigned to a segment
SELECT
    customer_segment,
    COUNT(*) as count,
    SUM(order_amount) as total_amount
FROM {{ ref('fact_sales_orders_ch') }}
GROUP BY customer_segment
ORDER BY total_amount DESC;

-- Test 7: Time dimension consistency
-- Order year/month should match date dimension
SELECT *
FROM {{ ref('fact_sales_orders_ch') }}
WHERE 
    CAST(SUBSTRING(order_date, 1, 4) AS INT) != order_year
    OR CAST(SUBSTRING(order_date, 6, 2) AS INT) != order_month;

-- Test 8: Year-over-year growth is reasonable
-- Flag if monthly revenue drops > 50% (may indicate data issue)
SELECT
    order_year,
    order_month,
    SUM(order_amount) as monthly_revenue,
    LAG(SUM(order_amount)) OVER (ORDER BY order_year, order_month) as prior_month_revenue,
    ROUND(
        (SUM(order_amount) - LAG(SUM(order_amount)) OVER (ORDER BY order_year, order_month)) /
        LAG(SUM(order_amount)) OVER (ORDER BY order_year, order_month) * 100,
        2
    ) as pct_change
FROM {{ ref('fact_sales_orders_ch') }}
GROUP BY order_year, order_month
HAVING ABS(
    (SUM(order_amount) - LAG(SUM(order_amount)) OVER (ORDER BY order_year, order_month)) /
    LAG(SUM(order_amount)) OVER (ORDER BY order_year, order_month)
) > 0.5;  -- Flag if > 50% change

-- Test 9: Customer lifetime value is populated correctly
-- Dimension should show aggregated metrics
SELECT
    'dim_customers_ch' as table_name,
    customer_id,
    total_orders,
    total_sales_value,
    tenure_days
FROM {{ ref('dim_customers_ch') }}
WHERE total_orders IS NULL OR total_sales_value IS NULL;

-- Test 10: No duplicate records in fact table
-- Surrogate key should be unique
SELECT
    COUNT(*) as total_records,
    COUNT(DISTINCT order_key) as unique_orders
FROM {{ ref('fact_sales_orders_ch') }}
HAVING COUNT(*) != COUNT(DISTINCT order_key);
