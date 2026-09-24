{{ config(materialized='table') }}

select
    country,
    case country
        when 'NL' then 'Netherlands'
        when 'BE' then 'Belgium'
        when 'CH' then 'Switzerland'
        else country
    end as country_name
from (
    select distinct country from {{ ref('stg_hourly_market') }}
)