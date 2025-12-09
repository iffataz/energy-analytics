import logging
import re
from pathlib import Path
from typing import Tuple, List
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ---------- Config ----------

IBEI_INDEX_URL = "https://nemweb.com.au/Reports/Current/IBEI/"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# ---------- Helpers ----------

def _find_latest_file_url(index_url: str, extensions: Tuple[str, ...]) -> str:
    """
    Fetch the NEMWeb IBEI directory index and return the URL of the latest file
    whose href ends with one of the given extensions (case-insensitive).
    """
    logging.info("Fetching IBEI index from %s", index_url)
    resp = requests.get(index_url, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    hrefs: List[str] = []
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if any(href.lower().endswith(ext.lower()) for ext in extensions):
            hrefs.append(href)

    if not hrefs:
        raise RuntimeError(f"No files with extensions {extensions} found at {index_url}")

    hrefs = sorted(hrefs)
    latest_href = hrefs[-1]
    latest_url = urljoin(index_url, latest_href)

    logging.info("Latest IBEI file resolved to %s", latest_url)
    return latest_url


def _extract_ibei_minimal(text: str) -> pd.DataFrame:
    """
    Extract minimal IBEI fields from a SUMMARY_RESULTS file:
      - SETTLEMENTDATE
      - REGIONID
      - ADJUSTED_INTENSITY_INDEX (renamed to EMISSIONS_INTENSITY)

    Handles both tab- or comma-separated formats.
    """
    rows: List[list] = []

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        # Keep only data rows: start with D,IBEI or D\tIBEI
        if not (line.startswith("D,IBEI") or line.startswith("D\tIBEI")):
            continue

        # Detect delimiter (IBEI examples are often tab-separated when copied)
        delim = "\t" if "\t" in line else ","
        parts = line.split(delim)

        # Expect at least:
        # 0:D, 1:IBEI, 2:PUBLISHING, 3:1, 4:CONTRACTYEAR,
        # 5:WEEKNO, 6:SETTLEMENTDATE, 7:REGIONID,
        # 8:ADJUSTED_SENTOUTENERGY, 9:GENERATOREMISSIONS,
        # 10:ADJUSTED_INTENSITY_INDEX
        if len(parts) < 11:
            continue

        settlement = parts[6]
        region = parts[7]
        intensity = parts[10]

        rows.append([settlement, region, intensity])

    if not rows:
        raise RuntimeError("No IBEI data rows found (D IBEI ...) in file.")

    df = pd.DataFrame(
        rows,
        columns=["SETTLEMENTDATE", "REGIONID", "EMISSIONS_INTENSITY"],
    )

    # Make intensity numeric for later modelling
    df["EMISSIONS_INTENSITY"] = pd.to_numeric(df["EMISSIONS_INTENSITY"], errors="coerce")

    logging.info("Extracted %s IBEI rows (minimal schema)", len(df))
    return df


# ---------- Public API ----------

def fetch_latest_ibei_emissions(save: bool = True) -> pd.DataFrame:
    """
    Fetch the latest IBEI_SUMMARY_RESULTS_YYYY.CSV from NEMWeb, extract minimal
    fields, and optionally save under data/raw/ibei_latest.csv.

    Returns
    -------
    pd.DataFrame
        Minimal IBEI emissions data with columns:
        [SETTLEMENTDATE, REGIONID, EMISSIONS_INTENSITY]
    """
    latest_url = _find_latest_file_url(IBEI_INDEX_URL, (".csv", ".CSV"))

    logging.info("Downloading %s", latest_url)
    resp = requests.get(latest_url, timeout=60)
    resp.raise_for_status()

    df = _extract_ibei_minimal(resp.text)

    if save:
        out_path = RAW_DATA_DIR / "ibei_latest.csv"
        logging.info("Saving minimal IBEI emissions data to %s", out_path)
        df.to_csv(out_path, index=False)

    return df


# ---------- Script entry point ----------

if __name__ == "__main__":
    logging.info("Starting fetch of latest IBEI emissions data...")
    ibeis_df = fetch_latest_ibei_emissions(save=True)
    logging.info("Done. Preview:")
    print(ibeis_df.head())
