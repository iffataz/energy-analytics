import logging
import os
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def _get_engine() -> Engine:
    """
    Build a SQLAlchemy engine for Postgres.

    Preferred env var for local dev:
        DATABASE_URL=postgresql://user:pass@localhost:5432/energy

    Optional fallback (if you later flip back to Supabase):
        SUPABASE_DB_URL=postgresql://...
    """
    db_url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not db_url:
        raise RuntimeError(
            "No database URL found. Please set DATABASE_URL (or SUPABASE_DB_URL)."
        )

    logging.info("Creating SQLAlchemy engine for Postgres target...")
    return create_engine(db_url)


def _load_csv_to_table(
    engine: Engine,
    csv_path: Path,
    table_name: str,
    schema: Optional[str] = "public",
) -> None:
    """Load a CSV into a Postgres table, replacing existing data."""
    if not csv_path.exists():
        logging.warning("CSV not found for table %s: %s (skipping)", table_name, csv_path)
        return

    logging.info("Loading %s into %s.%s", csv_path, schema, table_name)
    df = pd.read_csv(csv_path)

    # Best-effort parse timestamps/date columns
    for col in df.columns:
        if "timestamp" in col.lower() or col.lower() == "date":
            try:
                df[col] = pd.to_datetime(df[col], errors="ignore")
            except Exception:
                pass

    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists="replace",
        index=False,
    )
    logging.info("Finished loading %s rows into %s.%s", len(df), schema, table_name)


def load_all_to_postgres() -> None:
    """
    Push processed analytics tables into Postgres (local or remote):
      - prices_clean                -> prices
      - emissions_clean             -> emissions
      - price_emissions_joined      -> price_emissions_joined
      - daily_price_features        -> daily_price_features
      - price_emissions_regression_summary -> price_emissions_regression_summary
      - price_forecast_sample       -> price_forecast_sample
    """
    engine = _get_engine()

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS public"))

    tables = {
        "prices_clean.csv": "prices",
        "emissions_clean.csv": "emissions",
        "price_emissions_joined.csv": "price_emissions_joined",
        "daily_price_features.csv": "daily_price_features",
        "price_emissions_regression_summary.csv": "price_emissions_regression_summary",
        "price_forecast_sample.csv": "price_forecast_sample",
    }

    for filename, table_name in tables.items():
        _load_csv_to_table(engine, PROCESSED_DATA_DIR / filename, table_name, schema="public")


if __name__ == "__main__":
    logging.info("Starting load of processed analytics tables into Postgres...")
    load_all_to_postgres()
    logging.info("All done.")
