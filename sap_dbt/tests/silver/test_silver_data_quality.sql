-- Silver Layer Data Quality Tests
-- Tests to validate cleaned, deduplicated, and enriched data
-- Focus: Financial reporting accuracy and consistency

-- Test 1: No duplicates after deduplication
-- Surrogate keys should be unique
SELECT *
FROM {{ ref('sap_sales_order') }}
WHERE 
    sales_order_key IN (
        SELECT sales_order_key
        FROM {{ ref('sap_sales_order') }}
        GROUP BY sales_order_key
        HAVING COUNT(*) > 1
    );

-- Test 2: All header records have matching line items
-- Cannot have sales orders with no lines
SELECT h.sales_order_number
FROM {{ ref('sap_sales_order_header') }} h
LEFT JOIN {{ ref('sap_sales_order_lines') }} l
    ON h.sales_order_key = l.sales_order_key
WHERE l.sales_order_key IS NULL
    AND h.order_status NOT IN ('CANCELLED', 'REJECTED');

-- Test 3: Amounts are consistent between header and lines
-- Header total should equal sum of line amounts
SELECT
    h.sales_order_key,
    h.document_amount as header_amount,
    SUM(l.line_amount) as lines_sum,
    ABS(h.document_amount - SUM(l.line_amount)) as difference
FROM {{ ref('sap_sales_order_header') }} h
JOIN {{ ref('sap_sales_order_lines') }} l
    ON h.sales_order_key = l.sales_order_key
GROUP BY h.sales_order_key, h.document_amount
HAVING ABS(h.document_amount - SUM(l.line_amount)) > 0.01;  -- Allow 1 cent rounding difference

-- Test 4: Customer dimensions are valid
-- All orders must have valid customer records
SELECT o.sales_order_key, o.customer_number
FROM {{ ref('sap_sales_order') }} o
LEFT JOIN {{ ref('sap_customer_master') }} c
    ON o.customer_number = c.customer_number
WHERE c.customer_number IS NULL;

-- Test 5: Currency codes are valid
-- Only expected SAP currency codes
SELECT DISTINCT currency_code
FROM {{ ref('sap_sales_order') }}
WHERE currency_code NOT IN ('USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD');

-- Test 6: Order dates are valid
-- Document dates should not be in future
-- Posted dates should be after document dates
SELECT *
FROM {{ ref('sap_sales_order_header') }}
WHERE 
    document_date > CURRENT_DATE
    OR posting_date < document_date;

-- Test 7: SCD Type 2 tracking is correct
-- Effective dates should be sequential for customer changes
SELECT c1.customer_number, c1.end_date, c2.start_date
FROM {{ ref('sap_customer_master') }} c1
JOIN {{ ref('sap_customer_master') }} c2
    ON c1.customer_number = c2.customer_number
    AND c1.version < c2.version
WHERE c2.start_date != DATE_ADD(c1.end_date, INTERVAL 1 DAY);

-- Test 8: Required financial fields are populated
-- No nulls in amounts, quantities, or currency codes
SELECT *
FROM {{ ref('sap_sales_order') }}
WHERE
    document_amount IS NULL
    OR currency_code IS NULL
    OR line_amount IS NULL
    OR quantity IS NULL;

-- Test 9: Material master matches line items
-- All materials used should have master data
SELECT DISTINCT l.material_number
FROM {{ ref('sap_sales_order_lines') }} l
LEFT JOIN {{ ref('sap_material_master') }} m
    ON l.material_number = m.material_number
WHERE m.material_number IS NULL
    AND l.material_number IS NOT NULL;

-- Test 10: Data freshness in Silver
-- Most recent data should be less than 24 hours old
SELECT
    MAX(document_date) as latest_document_date,
    CURRENT_DATE - MAX(document_date) as days_since_latest,
    CASE 
        WHEN CURRENT_DATE - MAX(document_date) > 1 THEN 'STALE'
        ELSE 'FRESH'
    END as data_freshness
FROM {{ ref('sap_sales_order') }};
