select
    country,
    date as date_day,
    avg_price_eur_mwh,
    min_price_eur_mwh,
    max_price_eur_mwh,
    price_volatility,
    avg_renewable_share_pct
from {{ source('gridpulse_raw', 'daily_summary') }}