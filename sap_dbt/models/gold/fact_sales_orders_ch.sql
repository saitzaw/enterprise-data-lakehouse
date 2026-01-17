-- Gold Layer: Sales Order Analytics Fact Table (ClickHouse)
-- version: 1.00 Date: 2025-01-17
-- Complex aggregations and denormalization for BI analytics
-- Materialized in ClickHouse for fast OLAP queries

{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['order_date', 'customer_id', 'order_id'],
    partition_by=['toYYYYMM(order_date)'],
    tags=['gold', 'sales', 'clickhouse', 'analytics'],
    meta={
        'owner': 'analytics',
        'domain': 'sales',
        'retention_days': 2555
    }
) }}

with base_sales_header as (
    -- Read Silver sales order headers (Delta format)
    select
        vbak_mandt as mandt,
        vbak_vbeln as order_id,
        vbak_audat as order_date,
        vbak_erdat as created_date,
        vbak_erzeit as created_time,
        vbak_kunnr as customer_id,
        vbak_waerk as currency,
        vbak_netwr as net_amount,
        vbak_mwsbw as tax_amount,
        vbak_totwr as total_amount,
        vbak_vbtyp as order_type,
        vbak_auart as order_category,
        vbak_augru as order_reason,
        vbak_vkorg as sales_org,
        vbak_vtweg as distribution_channel,
        vbak_spart as division,
        start_ts,
        end_ts,
        is_current,
        version,
        {{ dbt_utils.generate_surrogate_key(['vbak_mandt', 'vbak_vbeln']) }} as order_sk
    from {{ source('delta', 'sap_silver_sales_orders_header') }}
    where is_current = true
),

base_sales_lines as (
    -- Read Silver sales order line items (Delta format)
    select
        vbap_mandt as mandt,
        vbap_vbeln as order_id,
        vbap_posnr as line_item_no,
        vbap_matnr as material_id,
        vbap_kwmeng as order_qty,
        vbap_meins as qty_unit,
        vbap_netwr as line_net_amount,
        vbap_vbrk as revenue_amount,
        start_ts,
        is_current
    from {{ source('delta', 'sap_silver_sales_orders_lines') }}
    where is_current = true
),

customer_master as (
    -- Join with customer master data
    select
        kna1_mandt as mandt,
        kna1_kunnr as customer_id,
        kna1_name1 as customer_name,
        kna1_ort01 as city,
        kna1_land1 as country,
        kna1_brsch as industry,
        kna1_akord as payment_terms,
        is_current
    from {{ source('delta', 'sap_silver_customers') }}
    where is_current = true
),

material_master as (
    -- Join with material master data
    select
        matnr as material_id,
        maktx as material_desc,
        matkl as material_class,
        meins as base_unit,
        is_current
    from {{ source('delta', 'sap_silver_materials') }}
    where is_current = true
),

joined_data as (
    select
        sh.mandt,
        sh.order_sk,
        sh.order_id,
        sh.line_item_no,
        sh.order_date,
        sh.created_date,
        sh.customer_id,
        coalesce(cm.customer_name, 'UNKNOWN') as customer_name,
        coalesce(cm.city, 'UNKNOWN') as customer_city,
        coalesce(cm.country, 'UNKNOWN') as customer_country,
        coalesce(cm.industry, 'UNKNOWN') as customer_industry,
        sh.material_id,
        coalesce(mm.material_desc, 'UNKNOWN') as material_desc,
        coalesce(mm.material_class, 'UNKNOWN') as material_class,
        sh.order_type,
        sh.order_category,
        sh.order_reason,
        sh.sales_org,
        sh.distribution_channel,
        sh.division,
        sh.currency,
        sh.order_qty,
        sh.qty_unit,
        sh.line_net_amount,
        sh.net_amount as order_net_amount,
        sh.tax_amount as order_tax_amount,
        sh.total_amount as order_total_amount,
        cast(sh.order_date as date) as order_date_key,
        cast(sh.created_date as date) as created_date_key,
        year(sh.order_date) as order_year,
        month(sh.order_date) as order_month,
        quarter(sh.order_date) as order_quarter,
        dayofweek(sh.order_date) as order_day_of_week,
        sh.start_ts,
        sh.version,
        current_timestamp() as dbt_loaded_at,
        {{ var('dbt_version', '1.0.0') }} as dbt_version
    from base_sales_header sh
    left join base_sales_lines sl on sh.order_id = sl.order_id and sh.mandt = sl.mandt
    left join customer_master cm on sh.customer_id = cm.customer_id and sh.mandt = cm.mandt
    left join material_master mm on sl.material_id = mm.material_id
),

final as (
    select
        mandt,
        order_sk,
        order_id,
        line_item_no,
        order_date,
        created_date,
        customer_id,
        customer_name,
        customer_city,
        customer_country,
        customer_industry,
        material_id,
        material_desc,
        material_class,
        order_type,
        order_category,
        order_reason,
        sales_org,
        distribution_channel,
        division,
        currency,
        order_qty,
        qty_unit,
        line_net_amount,
        order_net_amount,
        order_tax_amount,
        order_total_amount,
        order_date_key,
        created_date_key,
        order_year,
        order_month,
        order_quarter,
        order_day_of_week,
        start_ts as inserted_at,
        dbt_loaded_at,
        dbt_version
    from joined_data
)

select * from final
