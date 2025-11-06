-- models/staging/stg_china_exports.sql
-- Staging : Exportations de la Chine vers l'UE

{{ config(
    materialized='view',
    tags=['staging', 'china', 'exports']
) }}

with source as (
    select * from {{ source('bronze', 'raw_export_china') }}
),

renamed as (
    select
        -- Identifiants
        area as country,
        region,
        
        -- Code produit
        commodity_code::VARCHAR as code_hs,
        commodity_category,
        commodity_sub_category,
        
        -- Dates
        TRY_TO_DATE(date) as export_date,
        YEAR(TRY_TO_DATE(date)) as export_year,
        MONTH(TRY_TO_DATE(date)) as export_month,
        
        -- Métriques
        TRY_TO_DECIMAL(amount_usd, 18, 2) as amount_usd,
        
        -- Métadonnées
        'China' as source_country,
        'Export' as trade_type,
        LEFT(commodity_code::VARCHAR, 4) as code_hs_4digit,
        
        CURRENT_TIMESTAMP() as dbt_loaded_at
        
    from source
    where 
        area is not null
        and date is not null
        and TRY_TO_DECIMAL(amount_usd, 18, 2) > 0
)

select * from renamed