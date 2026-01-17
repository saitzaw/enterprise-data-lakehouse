-- Gold Layer: Customer Analytics Dimension (ClickHouse)
-- version: 1.00 Date: 2025-01-17
-- Enriched customer dimension with aggregated metrics

{{ config(
    materialized='table',
    engine='ReplacingMergeTree(inserted_at, customer_id)',
    order_by=['customer_id', 'mandt'],
    tags=['gold', 'dimension', 'clickhouse']
) }}

with customer_base as (
    select
        kna1_mandt as mandt,
        kna1_kunnr as customer_id,
        kna1_name1 as customer_name,
        kna1_ort01 as city,
        kna1_land1 as country,
        kna1_brsch as industry,
        kna1_akord as payment_terms,
        kna1_lifsp as delivery_priority,
        kna1_erdat as customer_created_date,
        is_current
    from {{ source('delta', 'sap_silver_customers') }}
    where is_current = true
),

customer_orders as (
    -- Aggregate order metrics per customer
    select
        vbak_mandt as mandt,
        vbak_kunnr as customer_id,
        count(distinct vbak_vbeln) as total_orders,
        sum(vbak_netwr) as total_order_value,
        avg(vbak_netwr) as avg_order_value,
        max(vbak_audat) as last_order_date,
        min(vbak_audat) as first_order_date,
        count(distinct year(vbak_audat)) as years_active
    from {{ source('delta', 'sap_silver_sales_orders_header') }}
    where is_current = true
    group by vbak_mandt, vbak_kunnr
),

customer_enriched as (
    select
        cb.mandt,
        cb.customer_id,
        cb.customer_name,
        cb.city,
        cb.country,
        cb.industry,
        cb.payment_terms,
        cb.delivery_priority,
        cb.customer_created_date,
        coalesce(co.total_orders, 0) as lifetime_order_count,
        coalesce(co.total_order_value, 0.0) as lifetime_order_value,
        coalesce(co.avg_order_value, 0.0) as avg_order_value,
        co.last_order_date,
        co.first_order_date,
        coalesce(co.years_active, 0) as customer_tenure_years,
        case
            when co.total_order_value >= 100000 then 'PREMIUM'
            when co.total_order_value >= 50000 then 'GOLD'
            when co.total_order_value >= 10000 then 'SILVER'
            else 'STANDARD'
        end as customer_segment,
        current_timestamp() as inserted_at
    from customer_base cb
    left join customer_orders co on cb.mandt = co.mandt and cb.customer_id = co.customer_id
)

select * from customer_enriched
