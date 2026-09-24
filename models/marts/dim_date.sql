{{ config(materialized='table') }}

with dates as (
    select distinct date_day from {{ ref('stg_hourly_market') }}
    union
    select distinct date_day from {{ ref('stg_daily_summary') }}
)
select
    date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    to_char(date_day, 'Month') as month_name,
    extract(day from date_day) as day_of_month,
    dayname(date_day) as day_name,
    case when dayname(date_day) in ('Sat', 'Sun') then true else false end as is_weekend
from dates