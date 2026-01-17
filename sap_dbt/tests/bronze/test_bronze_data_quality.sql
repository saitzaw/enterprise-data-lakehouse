-- Bronze Layer Data Quality Tests
-- Tests to validate raw data freshness, nullability, and basic constraints

-- Test 1: No duplicate records in raw SAP tables
-- This prevents duplicate ingestion issues
SELECT
    COUNT(*) as total_records,
    COUNT(DISTINCT *) as unique_records,
    CASE WHEN COUNT(*) = COUNT(DISTINCT *) THEN 'PASS' ELSE 'FAIL' END as duplicate_check
FROM {{ source('bronze', 'sap_bronze_sales_header') }}
HAVING COUNT(*) != COUNT(DISTINCT *);

-- Test 2: Validate required fields are not null
-- Core business identifiers must always be present
SELECT *
FROM {{ source('bronze', 'sap_bronze_sales_header') }}
WHERE 
    document_number IS NULL
    OR document_date IS NULL
    OR customer_number IS NULL
    OR company_code IS NULL;

-- Test 3: Data freshness check (data not older than 90 days)
SELECT COUNT(*) as stale_records
FROM {{ source('bronze', 'sap_bronze_sales_header') }}
WHERE document_date < CURRENT_DATE - INTERVAL 90 DAY;

-- Test 4: Validate numeric fields have expected ranges
-- Negative amounts could indicate returns but extreme negatives may be errors
SELECT *
FROM {{ source('bronze', 'sap_bronze_sales_lines') }}
WHERE
    quantity < -999  -- Extreme negative quantities
    OR unit_price < 0  -- Negative prices (only valid for returns)
    OR line_amount < -999999;  -- Extreme amounts

-- Test 5: Date consistency check
-- Document date should not be in the future
SELECT COUNT(*) as future_dated_records
FROM {{ source('bronze', 'sap_bronze_sales_header') }}
WHERE document_date > CURRENT_DATE;

-- Test 6: Validate SAP table row counts meet minimum threshold
-- Alert if ingestion appears to have missed records
SELECT
    '{{ table_name }}' as table_name,
    COUNT(*) as row_count,
    CASE 
        WHEN COUNT(*) < 100 THEN 'WARNING: Low row count'
        ELSE 'OK'
    END as status
FROM {{ source('bronze', table_name) }};
