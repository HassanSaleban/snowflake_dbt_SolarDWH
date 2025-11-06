-- models/staging/stg_eu_imports.sql
-- Staging : Importations de l'UE depuis le monde

{{ config(
    materialized='view',
    tags=['staging', 'eu', 'imports']
) }}

with source as (
    select * from {{ source('bronze', 'raw_import_ue') }}
),

renamed as (
    select
        -- Identifiants
        country_or_area as country,
        comm_code::VARCHAR as code_hs,
        commodity,
        
        -- Dates
        DATE_FROM_PARTS(TRY_TO_NUMBER(year), 1, 1) as import_date,
        TRY_TO_NUMBER(year) as import_year,
        1 as import_month,
        
        -- Métriques
        TRY_TO_DECIMAL(trade_usd, 18, 2) as trade_usd,
        TRY_TO_DECIMAL(weight_kg, 18, 3) as weight_kg,
        TRY_TO_DECIMAL(quantity, 18, 2) as quantity,
        
        -- Informations
        quantity_code,
        quantity_name,
        flow,
        
        -- Métadonnées
        'World' as source_region,
        'Import' as trade_type,
        LEFT(comm_code::VARCHAR, 4) as code_hs_4digit,
        
        -- Prix unitaire
        case 
            when TRY_TO_DECIMAL(weight_kg, 18, 3) > 0 
            then TRY_TO_DECIMAL(trade_usd, 18, 2) / TRY_TO_DECIMAL(weight_kg, 18, 3)
            else null
        end as unit_price_usd_per_kg,
        
        CURRENT_TIMESTAMP() as dbt_loaded_at
        
    from source
    where 
        country_or_area is not null
        and year is not null
        and TRY_TO_DECIMAL(trade_usd, 18, 2) > 0
)

select * from renamed