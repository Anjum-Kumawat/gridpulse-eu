{{ config(materialized='table') }}

select
    h.country,
    h.date_day,
    h.datetime_utc,
    extract(hour from h.datetime_utc) as hour_of_day,
    h.price_eur_mwh,
    h.total_generation_mw,
    h.renewable_generation_mw,
    h.renewable_share_pct
from {{ ref('stg_hourly_market') }} h