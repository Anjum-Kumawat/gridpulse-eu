{{ config(materialized='table') }}

select
    d.country,
    d.date_day,
    d.avg_price_eur_mwh,
    d.min_price_eur_mwh,
    d.max_price_eur_mwh,
    d.price_volatility,
    d.avg_renewable_share_pct
from {{ ref('stg_daily_summary') }} d