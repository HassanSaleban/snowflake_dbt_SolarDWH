-- models/marts/dimensions/dim_country.sql
-- Dimension : Pays

{{ config(
    materialized='table',
    tags=['dimension', 'country']
) }}

with countries_from_exports as (
    select distinct 
        country,
        'EU' as region
    from {{ ref('stg_china_exports') }}
),

countries_from_imports as (
    select distinct 
        country,
        'EU' as region
    from {{ ref('stg_eu_imports') }}
),

countries_from_production as (
    select distinct 
        country,
        'EU' as region
    from {{ ref('stg_solar_production') }}
),

china as (
    select 
        'China' as country,
        'Asia' as region
),

all_countries as (
    select * from countries_from_exports
    union
    select * from countries_from_imports
    union
    select * from countries_from_production
    union
    select * from china
),

final as (
    select
        ROW_NUMBER() over (order by country) as country_key,
        
        country as country_name,
        region,
        
        case 
            when country in ('Austria', 'Belgium', 'Bulgaria', 'Croatia', 'Cyprus', 
                           'Czech Republic', 'Czechia', 'Denmark', 'Estonia', 'Finland', 'France',
                           'Germany', 'Greece', 'Hungary', 'Ireland', 'Italy', 'Latvia',
                           'Lithuania', 'Luxembourg', 'Malta', 'Netherlands', 'Poland',
                           'Portugal', 'Romania', 'Slovakia', 'Slovenia', 'Spain', 'Sweden')
            then 'EU-27'
            when country = 'China' then 'Non-EU'
            else 'Other'
        end as country_group,
        
        case when region = 'EU' then 1 else 0 end as is_eu_country,
        
        CURRENT_TIMESTAMP() as dbt_created_at,
        CURRENT_TIMESTAMP() as dbt_updated_at
        
    from all_countries
)

select * from final