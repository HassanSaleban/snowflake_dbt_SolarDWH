-- models/staging/stg_products.sql
-- Staging : Catalogue des produits (batteries + photovoltaïque)

{{ config(
    materialized='view',
    tags=['staging', 'products']
) }}

with batteries as (
    select 
        id,
        code_hs,
        type,
        sous_type,
        statut,
        usage,
        application,
        'Battery' as product_family
    from {{ source('bronze', 'raw_batteries') }}
),

photovoltaique as (
    select 
        id,
        code_hs,
        type,
        sous_type,
        statut,
        usage,
        application,
        'Photovoltaic' as product_family
    from {{ source('bronze', 'raw_photovoltaique') }}
),

all_products as (
    select * from batteries
    union all
    select * from photovoltaique
),

renamed as (
    select
        id as product_id,
        code_hs,
        LEFT(code_hs::VARCHAR, 4) as code_hs_4digit,
        
        type as product_type,
        sous_type as product_subtype,
        statut as product_status,
        usage as product_usage,
        application as product_application,
        product_family,
        
        -- Classification
        case 
            when type ilike '%Lithium%' or type ilike '%Nickel%' or type ilike '%Plomb%' then 'Battery Technology'
            when type ilike '%Cellule%' or type ilike '%Module%' then 'Solar PV Technology'
            when type ilike '%chimique%' then 'Chemical Products'
            else 'Other'
        end as product_category,
        
        -- Flags
        case when statut ilike '%assemblé%' then 1 else 0 end as is_assembled,
        case when usage ilike '%solaire%' then 1 else 0 end as is_solar_specific,
        case when product_family = 'Battery' then 1 else 0 end as is_battery,
        case when product_family = 'Photovoltaic' then 1 else 0 end as is_photovoltaic,
        
        CURRENT_TIMESTAMP() as dbt_loaded_at
        
    from all_products
    where code_hs is not null
)

select * from renamed