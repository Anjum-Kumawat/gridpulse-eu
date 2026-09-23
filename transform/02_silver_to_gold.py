# Databricks notebook source
# MAGIC %md
# MAGIC # GridPulse EU — Silver to Gold
# MAGIC Builds the analysis-ready Gold tables: an hourly market snapshot (price +
# MAGIC renewable share per country) and a daily summary per country. These are
# MAGIC what Week 3 exports into Snowflake.

# COMMAND ----------

dbutils.widgets.text("storage_account", "stgpdlgn525kryfatfu", "ADLS account name")
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")

SILVER = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# COMMAND ----------

from pyspark.sql import functions as F

prices = spark.read.format("delta").load(f"{SILVER}/prices")
generation = spark.read.format("delta").load(f"{SILVER}/generation")

# COMMAND ----------

hourly_mix = (
    generation
    .groupBy("country", "datetime_utc")
    .agg(
        F.sum("quantity_mw").alias("total_generation_mw"),
        F.sum(F.when(F.col("is_renewable"), F.col("quantity_mw")).otherwise(0.0)).alias("renewable_generation_mw"),
    )
    .withColumn(
        "renewable_share_pct",
        F.when(F.col("total_generation_mw") > 0,
               F.round(F.col("renewable_generation_mw") / F.col("total_generation_mw") * 100, 1))
         .otherwise(None),
    )
)

hourly_market = prices.join(hourly_mix, on=["country", "datetime_utc"], how="left")

(hourly_market
    .write.format("delta").mode("overwrite")
    .partitionBy("country")
    .save(f"{GOLD}/hourly_market"))

print(f"gold/hourly_market: {hourly_market.count()} rows")

# COMMAND ----------

daily_summary = (
    hourly_market
    .withColumn("date", F.to_date("datetime_utc"))
    .groupBy("country", "date")
    .agg(
        F.round(F.avg("price_eur_mwh"), 2).alias("avg_price_eur_mwh"),
        F.round(F.min("price_eur_mwh"), 2).alias("min_price_eur_mwh"),
        F.round(F.max("price_eur_mwh"), 2).alias("max_price_eur_mwh"),
        F.round(F.stddev("price_eur_mwh"), 2).alias("price_volatility"),
        F.round(F.avg("renewable_share_pct"), 1).alias("avg_renewable_share_pct"),
    )
)

(daily_summary
    .write.format("delta").mode("overwrite")
    .partitionBy("country")
    .save(f"{GOLD}/daily_summary"))

print(f"gold/daily_summary: {daily_summary.count()} rows")
display(daily_summary.orderBy("date", "country"))
