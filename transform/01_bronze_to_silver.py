# Databricks notebook source
# MAGIC %md
# MAGIC # GridPulse EU — Bronze to Silver
# MAGIC Parses the raw ENTSO-E XML (and occasional zip) files landed daily by the
# MAGIC ingestion Function, and writes clean, typed Delta tables: `silver/prices`
# MAGIC and `silver/generation`.
# MAGIC
# MAGIC Parsing follows the same approach as the widely-used `entsoe-py` client
# MAGIC library: BeautifulSoup with the built-in `html.parser`, which lowercases
# MAGIC tags and strips XML namespaces automatically — so we don't need to hardcode
# MAGIC ENTSO-E's namespace URIs, which differ between document types.

# COMMAND ----------

# MAGIC %pip install beautifulsoup4 --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("storage_account", "stgpdlgn525kryfatfu", "ADLS account name")
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")

BRONZE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
SILVER = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Storage access
# MAGIC This notebook expects the storage account key to already be set as a Spark
# MAGIC config on the cluster (**Compute → your cluster → Edit → Advanced options →
# MAGIC Spark → Spark config**):
# MAGIC ```
# MAGIC fs.azure.account.key.<account>.dfs.core.windows.net <key>
# MAGIC ```
# MAGIC That keeps the key out of notebook history. If you haven't set it there yet,
# MAGIC uncomment the cell below and run it once instead — fine for a short-lived
# MAGIC sandbox cluster, not something to leave running long-term.

# COMMAND ----------

# ACCOUNT_KEY = "PASTE_KEY_HERE"
# spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", ACCOUNT_KEY)

# COMMAND ----------

import io
import re
import zipfile
from datetime import timedelta

import warnings

import pandas as pd
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from pyspark.sql import types as T

# We deliberately parse XML with BeautifulSoup's HTML parser (see module
# docstring above) - this is expected, not a bug.
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

PATH_RE = re.compile(r"dt=(\d{4}-\d{2}-\d{2})/([A-Z]{2})\.(xml|zip)$")

PSR_TYPE_NAMES = {
    "B01": "Biomass", "B02": "Fossil Brown coal/Lignite", "B03": "Fossil Coal-derived gas",
    "B04": "Fossil Gas", "B05": "Fossil Hard coal", "B06": "Fossil Oil",
    "B07": "Fossil Oil shale", "B08": "Fossil Peat", "B09": "Geothermal",
    "B10": "Hydro Pumped Storage", "B11": "Hydro Run-of-river and poundage",
    "B12": "Hydro Water Reservoir", "B13": "Marine", "B14": "Nuclear",
    "B15": "Other renewable", "B16": "Solar", "B17": "Waste",
    "B18": "Wind Offshore", "B19": "Wind Onshore", "B20": "Other",
}
# Conservative renewable classification, documented here as a deliberate call:
# Waste (B17) is treated as non-renewable; Biomass (B01) is treated as renewable.
# Adjust if your analysis wants a different convention - it's a genuine judgment
# call in the energy sector, not a settled fact.
RENEWABLE_PSR_TYPES = {"B01", "B09", "B10", "B11", "B12", "B13", "B15", "B16", "B18", "B19"}

RESOLUTION_MINUTES = {"PT15M": 15, "PT30M": 30, "PT60M": 60, "P1D": 1440}


def _iter_xml_docs(raw_bytes: bytes):
    """Yield one or more XML document strings from a bronze file's raw bytes -
    handles the (rare) case where ENTSO-E returned a zip of multiple documents."""
    if raw_bytes[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
            for name in zf.namelist():
                yield zf.read(name).decode("utf-8")
    else:
        yield raw_bytes.decode("utf-8")


def _period_points(period_soup, value_tag):
    start = pd.Timestamp(period_soup.find("start").text)
    resolution = period_soup.find("resolution").text
    step_minutes = RESOLUTION_MINUTES.get(resolution)
    if step_minutes is None:
        return
    for point in period_soup.find_all("point"):
        position = int(point.find("position").text)
        value_el = point.find(value_tag)
        if value_el is None:
            continue
        ts = start + timedelta(minutes=step_minutes * (position - 1))
        yield ts, float(value_el.text)


def parse_price_doc(xml_text: str, country: str):
    soup = BeautifulSoup(xml_text, "html.parser")
    rows = []
    for ts_el in soup.find_all("timeseries"):
        for period in ts_el.find_all("period"):
            for ts, price in _period_points(period, "price.amount"):
                rows.append({"country": country, "datetime_utc": ts, "price_eur_mwh": price})
    return rows


def parse_generation_doc(xml_text: str, country: str):
    soup = BeautifulSoup(xml_text, "html.parser")
    rows = []
    for ts_el in soup.find_all("timeseries"):
        # Skip consumption/load time series that sometimes ride along in the
        # same document - we only want generation-per-type here.
        if ts_el.find("outbiddingzone_domain.mrid") is not None:
            continue
        psr_el = ts_el.find("psrtype")
        if psr_el is None:
            continue
        psr_code = psr_el.text.strip()
        psr_name = PSR_TYPE_NAMES.get(psr_code, f"Unknown ({psr_code})")
        for period in ts_el.find_all("period"):
            for ts, quantity in _period_points(period, "quantity"):
                rows.append({
                    "country": country,
                    "datetime_utc": ts,
                    "psr_type": psr_code,
                    "psr_type_name": psr_name,
                    "is_renewable": psr_code in RENEWABLE_PSR_TYPES,
                    "quantity_mw": quantity,
                })
    return rows


def parse_bronze_folder(kind: str, parse_fn) -> pd.DataFrame:
    """kind is 'prices' or 'generation'. Reads every .xml/.zip file under
    bronze/entsoe/<kind>/ via Spark's binaryFile source (so we get real bytes,
    not text-decoded content that would corrupt zip files), then parses on the
    driver - the whole dataset here is small enough that distributed parsing
    would be pure overhead."""
    paths = f"{BRONZE}/entsoe/{kind}/*/*.{{xml,zip}}"
    bdf = spark.read.format("binaryFile").load(paths)
    pdf = bdf.select("path", "content").toPandas()

    all_rows, errors = [], []
    for _, row in pdf.iterrows():
        m = PATH_RE.search(row["path"])
        if not m:
            continue
        _, country, _ext = m.groups()
        try:
            for xml_text in _iter_xml_docs(bytes(row["content"])):
                all_rows.extend(parse_fn(xml_text, country))
        except Exception as exc:  # noqa: BLE001 - deliberately broad, logged below
            errors.append((row["path"], str(exc)))

    if errors:
        print(f"{kind}: parsed with {len(errors)} file-level errors (not fatal):")
        for path, err in errors[:10]:
            print(f"  {path}: {err}")
    return pd.DataFrame(all_rows)


# COMMAND ----------

prices_pdf = parse_bronze_folder("prices", parse_price_doc)
generation_pdf = parse_bronze_folder("generation", parse_generation_doc)
print(f"Parsed {len(prices_pdf)} price points, {len(generation_pdf)} generation points")

# COMMAND ----------

prices_schema = T.StructType([
    T.StructField("country", T.StringType()),
    T.StructField("datetime_utc", T.TimestampType()),
    T.StructField("price_eur_mwh", T.DoubleType()),
])

generation_schema = T.StructType([
    T.StructField("country", T.StringType()),
    T.StructField("datetime_utc", T.TimestampType()),
    T.StructField("psr_type", T.StringType()),
    T.StructField("psr_type_name", T.StringType()),
    T.StructField("is_renewable", T.BooleanType()),
    T.StructField("quantity_mw", T.DoubleType()),
])

prices_df = spark.createDataFrame(prices_pdf, schema=prices_schema) if len(prices_pdf) else spark.createDataFrame([], prices_schema)
generation_df = spark.createDataFrame(generation_pdf, schema=generation_schema) if len(generation_pdf) else spark.createDataFrame([], generation_schema)

# Basic data-quality checks - log loudly, don't fail the run on them.
for name, df in [("prices", prices_df), ("generation", generation_df)]:
    n_nulls = df.filter("datetime_utc IS NULL").count()
    n_dupes = df.count() - df.dropDuplicates().count()
    print(f"{name}: {df.count()} rows, {n_nulls} null datetimes, {n_dupes} exact duplicates")

# COMMAND ----------

# Overwriting the whole table each run is a deliberate simplification given how
# small this dataset is for a portfolio build - at production scale this would
# be an incremental merge keyed on (country, datetime_utc) instead, and would
# track which bronze files have already been processed.
(prices_df.dropDuplicates()
    .write.format("delta").mode("overwrite")
    .partitionBy("country")
    .save(f"{SILVER}/prices"))

(generation_df.dropDuplicates()
    .write.format("delta").mode("overwrite")
    .partitionBy("country")
    .save(f"{SILVER}/generation"))

print("Silver tables written to", SILVER)
