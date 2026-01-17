-- Gold Layer: Date Dimension (ClickHouse)
-- version: 1.00 Date: 2025-01-17
-- Standard date dimension for time-based analytics

{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['date_key'],
    tags=['gold', 'dimension', 'date', 'clickhouse']
) }}

with date_range as (
    -- Generate date range (adjust years_back as needed)
    select
        dateadd('day', x, '2020-01-01'::date) as calendar_date
    from (
        select
            row_number() over (order by 1) - 1 as x
        from 
            (select 1 union all select 2 union all select 3 union all select 4) a,
            (select 1 union all select 2 union all select 3 union all select 4) b
        limit 2000  -- ~5 years of data
    )
),

date_dimension as (
    select
        cast(formatDateTime(calendar_date, '%Y%m%d') as integer) as date_key,
        calendar_date,
        year(calendar_date) as calendar_year,
        quarter(calendar_date) as calendar_quarter,
        month(calendar_date) as calendar_month,
        dayofmonth(calendar_date) as day_of_month,
        dayofweek(calendar_date) as day_of_week,
        weekofyear(calendar_date) as week_of_year,
        dayofyear(calendar_date) as day_of_year,
        case
            when dayofweek(calendar_date) in (1, 7) then true
            else false
        end as is_weekend,
        case
            when dayofmonth(calendar_date) = 1 then true
            else false
        end as is_month_start,
        case
            when dayofmonth(calendar_date) = dayofmonth(lastDayOfMonth(calendar_date)) then true
            else false
        end as is_month_end,
        concat(
            year(calendar_date), '-',
            case when month(calendar_date) < 4 then '1'
                 when month(calendar_date) < 7 then '2'
                 when month(calendar_date) < 10 then '3'
                 else '4' end
        ) as fiscal_year_quarter,
        formatDateTime(calendar_date, '%Y-%m') as year_month,
        formatDateTime(calendar_date, '%Y-W%W') as year_week,
        dateName('MONTH', calendar_date) as month_name,
        dateName('WEEKDAY', calendar_date) as day_name,
        current_timestamp() as inserted_at
    from date_range
)

select * from date_dimension
