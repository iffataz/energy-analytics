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

DEFAULT_RAW_EMISSIONS = RAW_DATA_DIR / "ibei_latest.csv"
DEFAULT_CLEAN_EMISSIONS = PROCESSED_DATA_DIR / "emissions_clean.csv"

VALID_REGIONS = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1", "NEM"]  # keep NEM agg if you want


def clean_emissions(
    input_path: Path = DEFAULT_RAW_EMISSIONS,
    output_path: Path = DEFAULT_CLEAN_EMISSIONS,
) -> pd.DataFrame:
    """
    Clean minimal IBEI file (SETTLEMENTDATE, REGIONID, EMISSIONS_INTENSITY)
    and output a typed table.

    Output columns:
      - timestamp          (datetime64[ns])
      - region             (str)
      - emissions_intensity (float, tCO2/MWh)
    """
    logging.info("Loading raw IBEI emissions from %s", input_path)
    df = pd.read_csv(input_path)

    df = df.rename(
        columns={
            "SETTLEMENTDATE": "timestamp",
            "REGIONID": "region",
            "EMISSIONS_INTENSITY": "emissions_intensity",
        }
    )

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

    before = len(df)
    df = df.drop_duplicates()
    df = df.dropna(subset=["timestamp", "region", "emissions_intensity"])
    df = df[df["region"].isin(VALID_REGIONS)]
    after = len(df)

    logging.info("Emissions cleaned: %s -> %s rows", before, after)

    df = df.sort_values(["timestamp", "region"]).reset_index(drop=True)

    logging.info("Saving clean emissions to %s", output_path)
    df.to_csv(output_path, index=False)

    return df


if __name__ == "__main__":
    logging.info("Starting emissions cleaning...")
    emissions_clean = clean_emissions()
    logging.info("Done. Preview:")
    print(emissions_clean.head())
