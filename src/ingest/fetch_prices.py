import io
import logging
import re
import zipfile
from pathlib import Path
from typing import List, Tuple
from urllib.parse import urljoin

import pandas as pd
import requests

# ---------- Config ----------

PUBLIC_PRICES_INDEX_URL = "https://nemweb.com.au/Reports/Current/Public_Prices/"

# Project root: energy-carbon-analytics/
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# ---------- Helpers ----------

def _list_all_price_file_urls(index_url: str) -> List[str]:
    """
    Fetch the NEMWeb Public_Prices directory index and return URLs for
    all PUBLIC_PRICES_*.zip files, from earliest (top) to latest (bottom).
    """
    logging.info("Fetching index from %s", index_url)
    resp = requests.get(index_url, timeout=30)
    resp.raise_for_status()
    html = resp.text

    # Example hrefs:
    #   <a href="PUBLIC_PRICES_202512050000_20251206040504.zip">
    # We keep all of them, in the order they appear.
    pattern = r'href=[\'"]([^\'"]*PUBLIC_PRICES[^\'"]+\.zip)[\'"]'
    hrefs = re.findall(pattern, html, flags=re.IGNORECASE)

    if not hrefs:
        raise RuntimeError(f"No PUBLIC_PRICES*.zip files found at {index_url}")

    urls = [urljoin(index_url, href) for href in hrefs]

    logging.info("Found %s PUBLIC_PRICES zip files", len(urls))
    return urls


def _read_csvs_from_zip(zip_bytes: bytes) -> pd.DataFrame:
    """
    Read only D,DREGION data rows from NEM Public Prices and extract
    the minimal set of analytical fields:

      Time:
        - SETTLEMENTDATE

      Region:
        - REGIONID

      Price:
        - RRP

      Demand:
        - TOTALDEMAND
        - DEMANDFORECAST

      Supply:
        - DISPATCHABLEGENERATION
        - INITIALSUPPLY

      Flows:
        - NETINTERCHANGE

      Markets (optional but useful):
        - MARKETSUSPENDEDFLAG
    """
    logging.info("Reading CSV(s) and extracting required analytical columns")
    records: List[list] = []

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]

        if not csv_names:
            raise RuntimeError("Zip archive contains no CSV files.")

        for name in csv_names:
            logging.info("  -> Processing %s", name)
            text = zf.read(name).decode("utf-8", errors="replace")
            lines = text.splitlines()

            for line in lines:
                s = line.strip()

                # Keep only data rows
                if not s.startswith("D,DREGION"):
                    continue

                parts = s.split(",")

                # Safety: need at least up to INITIALSUPPLY (index 70)
                if len(parts) <= 70:
                    continue

                # Indices from the I,DREGION header:
                #  4  -> SETTLEMENTDATE
                #  6  -> REGIONID
                #  8  -> RRP
                #  12 -> MARKETSUSPENDEDFLAG
                #  13 -> TOTALDEMAND
                #  14 -> DEMANDFORECAST
                #  15 -> DISPATCHABLEGENERATION
                #  17 -> NETINTERCHANGE
                #  70 -> INITIALSUPPLY
                settlement              = parts[4]
                region                  = parts[6]
                rrp                     = parts[8]
                marketsuspendedflag     = parts[12]
                totaldemand             = parts[13]
                demandforecast          = parts[14]
                dispatchablegeneration  = parts[15]
                netinterchange          = parts[17]
                initialsupply           = parts[70]

                records.append([
                    settlement,
                    region,
                    rrp,
                    totaldemand,
                    demandforecast,
                    dispatchablegeneration,
                    netinterchange,
                    initialsupply,
                    marketsuspendedflag,
                ])

    if not records:
        raise RuntimeError("No D,DREGION rows found in any CSV.")

    df = pd.DataFrame(
        records,
        columns=[
            "SETTLEMENTDATE",
            "REGIONID",
            "RRP",
            "TOTALDEMAND",
            "DEMANDFORECAST",
            "DISPATCHABLEGENERATION",
            "NETINTERCHANGE",
            "INITIALSUPPLY",
            "MARKETSUSPENDEDFLAG",
        ],
    )
    logging.info("Extracted %s rows with required analytical columns", len(df))
    return df


# ---------- Public API ----------

def fetch_all_current_public_prices(save: bool = True) -> pd.DataFrame:
    """
    Download and combine ALL PUBLIC_PRICES_*.zip files from the
    Current/Public_Prices directory, from earliest to latest.

    Uses _read_csvs_from_zip() to extract:

      - SETTLEMENTDATE
      - REGIONID
      - RRP
      - TOTALDEMAND
      - DEMANDFORECAST
      - DISPATCHABLEGENERATION
      - NETINTERCHANGE
      - INITIALSUPPLY
      - MARKETSUSPENDEDFLAG
    """
    from_this_index = _list_all_price_file_urls(PUBLIC_PRICES_INDEX_URL)

    frames = []
    for url in from_this_index:
        logging.info("Downloading %s", url)
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        df_part = _read_csvs_from_zip(resp.content)
        frames.append(df_part)

    combined = pd.concat(frames, ignore_index=True)
    logging.info("Combined Public_Prices (Current/) shape: %s", combined.shape)

    if save:
        out_path = RAW_DATA_DIR / "public_prices_current_all.csv"
        logging.info("Saving combined prices to %s", out_path)
        combined.to_csv(out_path, index=False)

    return combined


# ---------- Script entry point ----------

if __name__ == "__main__":
    logging.info("Starting fetch of latest Public Prices data...")
    prices_df = fetch_all_current_public_prices(save=True)
    logging.info("Done. Preview:")
    print(prices_df.head())
