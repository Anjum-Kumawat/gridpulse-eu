# GridPulse EU — Streamlit dashboard
# Reads the dbt-built star schema (GRIDPULSE.<schema>.FCT_HOURLY_MARKET /
# FCT_DAILY_SUMMARY / DIM_COUNTRY) from Snowflake and compares day-ahead
# power prices and renewable generation share across NL, BE, CH.

import pandas as pd
import plotly.express as px
import snowflake.connector
import streamlit as st

st.set_page_config(page_title="GridPulse EU", page_icon="⚡", layout="wide")


@st.cache_resource
def get_connection():
    cfg = st.secrets["snowflake"]
    return snowflake.connector.connect(
        account=cfg["account"],
        user=cfg["user"],
        password=cfg["password"],
        warehouse=cfg["warehouse"],
        database=cfg["database"],
        schema=cfg["schema"],
        role=cfg["role"],
    )


@st.cache_data(ttl=3600)
def run_query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [c[0].lower() for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


st.title("⚡ GridPulse EU")
st.caption(
    "Day-ahead electricity prices and renewable generation share — "
    "Netherlands, Belgium, Switzerland. Source: ENTSO-E Transparency Platform, "
    "via Azure Functions → ADLS → Databricks → Snowflake → dbt."
)

countries_df = run_query("select country, country_name from dim_country order by country")
country_options = countries_df["country"].tolist()
country_labels = dict(zip(countries_df["country"], countries_df["country_name"]))

with st.sidebar:
    st.header("Filters")
    selected_countries = st.multiselect(
        "Country",
        options=country_options,
        default=country_options,
        format_func=lambda c: f"{c} — {country_labels.get(c, c)}",
    )
    days_back = st.slider("Days of history to show", min_value=1, max_value=30, value=14)

if not selected_countries:
    st.warning("Select at least one country in the sidebar.")
    st.stop()

country_list_sql = ", ".join(f"'{c}'" for c in selected_countries)

hourly = run_query(
    f"""
    select country, datetime_utc, price_eur_mwh, renewable_share_pct
    from fct_hourly_market
    where country in ({country_list_sql})
      and datetime_utc >= dateadd(day, -{days_back}, current_timestamp())
    order by datetime_utc
    """
)

daily = run_query(
    f"""
    select country, date_day, avg_price_eur_mwh, min_price_eur_mwh,
           max_price_eur_mwh, price_volatility, avg_renewable_share_pct
    from fct_daily_summary
    where country in ({country_list_sql})
      and date_day >= dateadd(day, -{days_back}, current_date())
    order by date_day
    """
)

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Avg price (EUR/MWh)", f"{hourly['price_eur_mwh'].mean():.1f}" if len(hourly) else "—")
with col2:
    st.metric(
        "Avg renewable share",
        f"{hourly['renewable_share_pct'].mean():.1f}%" if len(hourly) and hourly['renewable_share_pct'].notna().any() else "—",
    )
with col3:
    st.metric(
        "Avg daily volatility (EUR/MWh)",
        f"{daily['price_volatility'].mean():.1f}" if len(daily) and daily['price_volatility'].notna().any() else "—",
    )

st.subheader("Hourly day-ahead price")
if len(hourly):
    fig_price = px.line(
        hourly, x="datetime_utc", y="price_eur_mwh", color="country",
        labels={"datetime_utc": "Time (UTC)", "price_eur_mwh": "EUR/MWh", "country": "Country"},
    )
    st.plotly_chart(fig_price, use_container_width=True)
else:
    st.info("No hourly price data in this range yet.")

st.subheader("Renewable generation share")
if len(hourly) and hourly["renewable_share_pct"].notna().any():
    fig_renew = px.line(
        hourly, x="datetime_utc", y="renewable_share_pct", color="country",
        labels={"datetime_utc": "Time (UTC)", "renewable_share_pct": "Renewable share (%)", "country": "Country"},
    )
    st.plotly_chart(fig_renew, use_container_width=True)
else:
    st.info("No generation-mix data in this range yet.")

st.subheader("Daily summary")
if len(daily):
    st.dataframe(
        daily.rename(columns={
            "country": "Country", "date_day": "Date", "avg_price_eur_mwh": "Avg price",
            "min_price_eur_mwh": "Min price", "max_price_eur_mwh": "Max price",
            "price_volatility": "Volatility", "avg_renewable_share_pct": "Avg renewable %",
        }),
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("No daily summary data in this range yet.")

st.caption(
    "Built as a portfolio project: ENTSO-E API → Azure Function (ingestion) → "
    "ADLS Gen2 (bronze) → Azure Databricks (silver/gold Delta) → Snowflake "
    "(external stage load) → dbt (staging + star schema) → this dashboard."
)
