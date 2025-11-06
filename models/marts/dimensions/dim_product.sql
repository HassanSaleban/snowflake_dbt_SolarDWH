-- models/marts/dimensions/dim_product.sql
-- Dimension : Produits

{{ config(
    materialized='table',
    tags=['dimension', 'product']
) }}

with products as (
    select * from {{ ref('stg_products') }}
),

codes_from_trade as (
    select distinct
        code_hs_4digit
    from (
        select code_hs_4digit from {{ ref('stg_china_exports') }}
        union
        select code_hs_4digit from {{ ref('stg_eu_imports') }}
    ) t
),

final as (
    select
        ROW_NUMBER() over (order by COALESCE(p.code_hs_4digit, c.code_hs_4digit)) as product_key,
        
        COALESCE(p.code_hs_4digit, c.code_hs_4digit) as code_hs_4digit,
        p.code_hs as code_hs_full,
        
        p.product_type,
        p.product_subtype,
        p.product_status,
        p.product_usage,
        p.product_application,
        p.product_category,
        p.product_family,
        
        COALESCE(p.is_assembled, 0) as is_assembled,
        COALESCE(p.is_solar_specific, 0) as is_solar_specific,
        COALESCE(p.is_battery, 0) as is_battery,
        COALESCE(p.is_photovoltaic, 0) as is_photovoltaic,
        
        CURRENT_TIMESTAMP() as dbt_created_at,
        CURRENT_TIMESTAMP() as dbt_updated_at
        
    from codes_from_trade c
    left join products p on c.code_hs_4digit = p.code_hs_4digit
)

select * from final