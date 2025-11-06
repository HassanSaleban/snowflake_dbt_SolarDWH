-- models/marts/facts/fact_trade.sql
-- Fait : Commerce international (imports + exports)

{{ config(
    materialized='table',
    tags=['fact', 'trade']
) }}

with exports as (
    select
        export_date as trade_date,
        export_year as trade_year,
        export_month as trade_month,
        country,
        source_country,
        code_hs_4digit,
        'Export' as trade_direction,
        amount_usd as trade_value_usd,
        null as weight_kg,
        null as quantity,
        null as quantity_name
    from {{ ref('stg_china_exports') }}
),

imports as (
    select
        import_date as trade_date,
        import_year as trade_year,
        import_month as trade_month,
        country,
        'World' as source_country,
        code_hs_4digit,
        'Import' as trade_direction,
        trade_usd as trade_value_usd,
        weight_kg,
        quantity,
        quantity_name
    from {{ ref('stg_eu_imports') }}
),

all_trade as (
    select * from exports
    union all
    select * from imports
),

final as (
    select
        ROW_NUMBER() over (order by t.trade_date, t.country, t.code_hs_4digit) as trade_key,
        
        TO_NUMBER(TO_CHAR(t.trade_date, 'YYYYMMDD')) as date_key,
        c.country_key,
        p.product_key,
        
        t.trade_direction,
        t.source_country,
        
        t.trade_value_usd,
        t.weight_kg,
        t.quantity,
        t.quantity_name,
        
        case 
            when t.weight_kg > 0 then t.trade_value_usd / t.weight_kg
            else null
        end as unit_price_usd_per_kg,
        
        t.trade_year,
        t.trade_month,
        t.trade_date,
        
        CURRENT_TIMESTAMP() as dbt_loaded_at
        
    from all_trade t
    left join {{ ref('dim_country') }} c 
        on t.country = c.country_name
    left join {{ ref('dim_product') }} p 
        on t.code_hs_4digit = p.code_hs_4digit
)

select * from final
```

