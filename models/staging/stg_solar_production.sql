-- models/staging/stg_solar_production.sql
-- Staging : Production d'électricité solaire UE

{{ config(
    materialized='view',
    tags=['staging', 'production']
) }}

with source as (
    select * from {{ source('bronze', 'raw_production_ue') }}
),

renamed as (
    select
        country_or_area as country,
        
        TRY_TO_NUMBER(year)::INT as production_year,
        
        TRY_TO_DECIMAL(quantity, 18, 3) as quantity_million_kwh,
        TRY_TO_DECIMAL(quantity, 18, 3) as quantity_gwh,
        TRY_TO_DECIMAL(quantity, 18, 6) / 1000.0 as quantity_twh,
        
        unit as unit_description,
        'Solar PV' as energy_source,
        'EU' as region,
        
        CURRENT_TIMESTAMP() as dbt_loaded_at
        
    from source
    where 
        country_or_area is not null
        and year is not null
        and TRY_TO_DECIMAL(quantity, 18, 3) > 0
)

select * from renamed