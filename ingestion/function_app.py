"""
GridPulse EU — daily ingestion function.

Pulls the previous day's day-ahead electricity prices and actual generation
per production type for NL, BE and CH from the ENTSO-E Transparency Platform,
and lands the raw (untouched) API response in the ADLS Gen2 "bronze" container.

Parsing/cleaning happens later, in Databricks (Week 2) — this function's only
job is reliable, faithful raw ingestion.
"""
import logging
import os
from datetime import datetime, timedelta, timezone

import azure.functions as func
import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from entsoe import EntsoeRawClient
from entsoe.exceptions import NoMatchingDataError

app = func.FunctionApp()

COUNTRIES = ["NL", "BE", "CH"]

# (bronze sub-path, EntsoeRawClient method name)
FEEDS = [
    ("prices", "query_day_ahead_prices"),
    ("generation", "query_generation"),
]


def _adls_file_system():
    account_url = os.environ["ADLS_ACCOUNT_URL"]
    credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(account_url=account_url, credential=credential)
    return service_client.get_file_system_client("bronze")


def _land(fs_client, kind: str, country: str, target_day, raw) -> None:
    # ENTSO-E sometimes returns a zip archive instead of a single XML document
    # when a request spans multiple time series - keep whichever we got, as-is.
    if isinstance(raw, bytes) and raw[:2] == b"PK":
        ext, data = "zip", raw
    elif isinstance(raw, bytes):
        ext, data = "xml", raw
    else:
        ext, data = "xml", raw.encode("utf-8")

    path = f"entsoe/{kind}/dt={target_day.isoformat()}/{country}.{ext}"
    file_client = fs_client.get_file_client(path)
    file_client.upload_data(data, overwrite=True)
    logging.info("Landed %s/%s -> %s (%d bytes)", kind, country, path, len(data))


@app.function_name(name="entsoe_daily_ingest")
@app.timer_trigger(
    schedule="0 0 6 * * *",  # 06:00 UTC daily - day-ahead data for "yesterday" is fully published by then
    arg_name="timer",
    run_on_startup=False,
    use_monitor=True,
)
def entsoe_daily_ingest(timer: func.TimerRequest) -> None:
    token = os.environ["ENTSOE_TOKEN"]
    client = EntsoeRawClient(api_key=token)

    target_day = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    start = pd.Timestamp(target_day, tz="UTC")
    end = pd.Timestamp(target_day + timedelta(days=1), tz="UTC")

    fs_client = _adls_file_system()

    for kind, method_name in FEEDS:
        for country in COUNTRIES:
            try:
                raw = getattr(client, method_name)(country, start=start, end=end)
                _land(fs_client, kind, country, target_day, raw)
            except NoMatchingDataError:
                logging.warning("No %s data yet for %s on %s", kind, country, target_day)
            except Exception:
                # Log and move on - one bad country/feed shouldn't sink the whole run.
                logging.exception("Failed fetching %s for %s on %s", kind, country, target_day)
