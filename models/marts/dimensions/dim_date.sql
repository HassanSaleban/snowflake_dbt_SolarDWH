-- models/marts/dimensions/dim_date.sql
-- Dimension : Calendrier (2018-2025)

{{ config(
    materialized='table',
    tags=['dimension', 'date']
) }}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2018-01-01' as date)",
        end_date="cast('2025-12-31' as date)"
    )}}
),

final as (
    select
        TO_NUMBER(TO_CHAR(date_day, 'YYYYMMDD')) as date_key,
        
        date_day as full_date,
        
        YEAR(date_day) as year,
        QUARTER(date_day) as quarter,
        MONTH(date_day) as month,
        DAY(date_day) as day,
        DAYOFWEEK(date_day) as day_of_week,
        DAYOFYEAR(date_day) as day_of_year,
        WEEKOFYEAR(date_day) as week_of_year,
        
        TO_CHAR(date_day, 'MMMM') as month_name,
        TO_CHAR(date_day, 'MON') as month_short_name,
        TO_CHAR(date_day, 'DAY') as day_name,
        TO_CHAR(date_day, 'DY') as day_short_name,
        
        case when DAYOFWEEK(date_day) in (0, 6) then 1 else 0 end as is_weekend,
        case when DAYOFWEEK(date_day) between 1 and 5 then 1 else 0 end as is_weekday,
        
        TO_CHAR(date_day, 'YYYY-MM') as year_month,
        TO_CHAR(date_day, 'YYYY-Q') as year_quarter,
        
        DATE_TRUNC('month', date_day) as first_day_of_month,
        LAST_DAY(date_day) as last_day_of_month,
        DATE_TRUNC('quarter', date_day) as first_day_of_quarter,
        DATE_TRUNC('year', date_day) as first_day_of_year,
        
        CURRENT_TIMESTAMP() as dbt_created_at
        
    from date_spine
)

select * from final