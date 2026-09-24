select
    country,
    datetime_utc,
    cast(datetime_utc as date) as date_day,
    price_eur_mwh,
    total_generation_mw,
    renewable_generation_mw,
    renewable_share_pct
from {{ source('gridpulse_raw', 'hourly_market') }}