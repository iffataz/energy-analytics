import logging
from pathlib import Path

import pandas as pd

# ---------- Paths ----------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

PRICES_PATH = PROCESSED_DATA_DIR / "prices_clean.csv"
EMISSIONS_PATH = PROCESSED_DATA_DIR / "emissions_clean.csv"
JOINED_PATH = PROCESSED_DATA_DIR / "price_emissions_joined.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def build_price_emissions_join(
    prices_path: Path = PRICES_PATH,
    emissions_path: Path = EMISSIONS_PATH,
    output_path: Path = JOINED_PATH,
) -> pd.DataFrame:
    """
    Join 5-minute NEM prices with daily emissions intensity by (date, region).

    Keeps all analytical fields from prices_clean and adds:
      - date
      - emissions_intensity
    """
    logging.info("Loading clean prices from %s", prices_path)
    prices = pd.read_csv(prices_path, parse_dates=["timestamp"])

    logging.info("Loading clean emissions from %s", emissions_path)
    emissions = pd.read_csv(emissions_path, parse_dates=["timestamp"])

    # Derive pure date key (no time) for both
    prices["date"] = prices["timestamp"].dt.date
    emissions["date"] = emissions["timestamp"].dt.date

    logging.info("Joining on [date, region]...")
    joined = pd.merge(
        prices,
        emissions[["date", "region", "emissions_intensity"]],
        on=["date", "region"],
        how="left",  # keep all price intervals even if an emissions day is missing
    )

    # Sort for sanity
    joined = joined.sort_values(["timestamp", "region"]).reset_index(drop=True)

    logging.info("Saving joined table to %s (rows=%s)", output_path, len(joined))
    joined.to_csv(output_path, index=False)

    return joined


if __name__ == "__main__":
    logging.info("Building joined price-emissions table...")
    joined_df = build_price_emissions_join()
    logging.info("Done. Preview:")
    print("Columns:", joined_df.columns.tolist())
    print(joined_df.head())
