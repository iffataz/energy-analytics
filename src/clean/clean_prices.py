import logging
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"

PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

DEFAULT_RAW_PRICES = RAW_DATA_DIR / "public_prices_current_all.csv"

DEFAULT_CLEAN_PRICES = PROCESSED_DATA_DIR / "prices_clean.csv"

# NEM regions we care about (you can tweak this)
VALID_REGIONS = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]


def clean_prices(
    input_path: Path = DEFAULT_RAW_PRICES,
    output_path: Path = DEFAULT_CLEAN_PRICES,
) -> pd.DataFrame:
    """
    Clean raw prices file (3 columns: SETTLEMENTDATE, REGIONID, RRP)
    and output a typed, filtered table ready for joining.

    Output columns:
      - timestamp (datetime64[ns])
      - region   (str)
      - price    (float, $/MWh)
    """
    logging.info("Loading raw prices from %s", input_path)
    df = pd.read_csv(input_path)

    # Rename to simpler names
    df = df.rename(
        columns={
            "SETTLEMENTDATE": "timestamp",
            "REGIONID": "region",
            "RRP": "price",
            "TOTALDEMAND": "total_demand",
            "DEMANDFORECAST": "demand_forecast",
            "DISPATCHABLEGENERATION": "dispatchable_generation",
            "NETINTERCHANGE": "net_interchange",
            "INITIALSUPPLY": "initial_supply",
            "MARKETSUSPENDEDFLAG": "market_suspended_flag",
        }
    )

    # Parse timestamp (AEMO uses dd/mm/YYYY HH:MM)
    # Clean up quoting/whitespace and parse explicit format "YYYY/MM/DD HH:MM:SS"
    df["timestamp"] = (
        df["timestamp"]
        .astype(str)
        .str.strip()
        .str.strip('"')
    )

    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        format="%Y/%m/%d %H:%M:%S",
        errors="coerce",
    )

    # Cast numeric columns
    numeric_cols = [
        "price",
        "total_demand",
        "demand_forecast",
        "dispatchable_generation",
        "net_interchange",
        "initial_supply",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Market suspended flag: make it 0/1 integer
    df["market_suspended_flag"] = (
        pd.to_numeric(df["market_suspended_flag"], errors="coerce")
        .fillna(0)
        .astype(int)
    )

    # Basic validation / filtering
    before = len(df)
    df = df.drop_duplicates()
    df = df.dropna(subset=["timestamp", "region", "price"])

    # Filter negative or zero prices if you want to ignore them; for now we keep >= -1000
    df = df[df["price"] > -1000]

    # Filter to standard NEM regions
    df = df[df["region"].isin(VALID_REGIONS)]

    after = len(df)
    logging.info("Prices cleaned: %s -> %s rows", before, after)

    # Sort for sanity
    df = df.sort_values(["timestamp", "region"]).reset_index(drop=True)

    logging.info("Saving clean prices to %s", output_path)
    df.to_csv(output_path, index=False)

    return df


if __name__ == "__main__":
    logging.info("Starting prices cleaning...")
    prices_clean = clean_prices()
    logging.info("Done. Preview:")
    print(prices_clean.head())
