-- models/marts/facts/fact_solar_production.sql
-- Fait : Production d'électricité solaire UE

{{ config(
    materialized='table',
    tags=['fact', 'production']
) }}

with production as (
    select * from {{ ref('stg_solar_production') }}
),

final as (
    select
        ROW_NUMBER() over (order by p.production_year, p.country) as production_key,
        
        TO_NUMBER(p.production_year || '0101') as date_key,
        c.country_key,
        
        p.production_year,
        
        p.quantity_million_kwh,
        p.quantity_gwh,
        p.quantity_twh,
        
        p.energy_source,
        p.unit_description,
        
        CURRENT_TIMESTAMP() as dbt_loaded_at
        
    from production p
    left join {{ ref('dim_country') }} c 
        on p.country = c.country_name
)

select * from final