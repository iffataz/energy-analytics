import logging
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

JOINED_PATH = PROCESSED_DATA_DIR / "price_emissions_joined.csv"
DAILY_FEATURES_PATH = PROCESSED_DATA_DIR / "daily_price_features.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def _mad_z(series: pd.Series) -> pd.Series:
    """Median absolute deviation based z-score."""
    median = series.median()
    mad = (series - median).abs().median()
    if mad == 0 or np.isnan(mad):
        return pd.Series(np.nan, index=series.index)
    return 0.6745 * (series - median) / mad


def _intraday_correlations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Per day/region correlations using 5-minute data:
      - price ~ total_demand
      - price ~ emissions_intensity
      - price ~ forecast_error (total_demand - demand_forecast)
    """
    rows = []
    for (date, region), grp in df.groupby(["date", "region"]):
        grp = grp.sort_values("timestamp")
        if len(grp) < 4:
            continue

        price = grp["price"]
        demand = grp["total_demand"]
        emissions = grp["emissions_intensity"]
        forecast = grp.get("demand_forecast")
        forecast_error_series = demand - forecast if forecast is not None else None

        rows.append(
            {
                "date": date,
                "region": region,
                "demand_price_corr": price.corr(demand),
                "carbon_price_corr": price.corr(emissions),
                "forecast_error_price_corr": price.corr(forecast_error_series)
                if forecast_error_series is not None
                else np.nan,
            }
        )
    return pd.DataFrame(rows)


def build_daily_features(joined_path: Path = JOINED_PATH) -> pd.DataFrame:
    """
    Core statistical modelling layer:
      - Daily aggregates of price, demand, supply, carbon
      - Rolling means/volatility (7-day)
      - Price-demand and carbon-price correlations
      - Forecast error + supply/demand gap/margin
      - Price anomaly flag (z + MAD)
    Output: data/processed/daily_price_features.csv
    """
    logging.info("Loading joined data from %s", joined_path)
    df = pd.read_csv(joined_path, parse_dates=["timestamp"])
    if "date" not in df.columns:
        df["date"] = df["timestamp"].dt.date

    # intraday correlations
    corr_df = _intraday_correlations(df)

    logging.info("Aggregating daily metrics...")
    daily = (
        df.groupby(["date", "region"])
        .agg(
            price_mean=("price", "mean"),
            price_median=("price", "median"),
            price_max=("price", "max"),
            price_min=("price", "min"),
            price_std=("price", "std"),
            total_demand_mean=("total_demand", "mean"),
            demand_forecast_mean=("demand_forecast", "mean"),
            dispatchable_generation_mean=("dispatchable_generation", "mean"),
            net_interchange_mean=("net_interchange", "mean"),
            emissions_intensity_mean=("emissions_intensity", "mean"),
        )
        .reset_index()
    )

    # supply/demand imbalance
    daily["supply_demand_gap"] = (
        daily["dispatchable_generation_mean"] - daily["total_demand_mean"]
    )
    daily["supply_margin_percent"] = daily["supply_demand_gap"] / daily["total_demand_mean"]

    # forecast error (using daily means)
    daily["forecast_error"] = daily["total_demand_mean"] - daily["demand_forecast_mean"]

    # rolling 7-day trends/volatility
    daily = daily.sort_values(["region", "date"]).reset_index(drop=True)
    for col, out_col in [
        ("price_mean", "price_mean_roll7"),
        ("price_std", "price_std_roll7"),
        ("total_demand_mean", "demand_mean_roll7"),
        ("emissions_intensity_mean", "emissions_mean_roll7"),
    ]:
        daily[out_col] = (
            daily.groupby("region")[col]
            .transform(lambda s: s.rolling(window=7, min_periods=3).mean())
        )

    # price anomaly detection
    daily["price_z"] = daily.groupby("region")["price_mean"].transform(
        lambda s: (s - s.mean()) / s.std(ddof=0)
    )
    daily["price_mad_z"] = daily.groupby("region")["price_mean"].transform(_mad_z)
    daily["is_price_anomaly"] = (daily["price_z"].abs() > 3) | (daily["price_mad_z"].abs() > 3)

    # merge correlations
    daily = daily.merge(corr_df, on=["date", "region"], how="left")

    return daily


def run_full_analysis() -> pd.DataFrame:
    """
    Compute and persist daily_price_features.csv for dashboards.
    """
    daily = build_daily_features(JOINED_PATH)
    logging.info("Saving daily features to %s (rows=%s)", DAILY_FEATURES_PATH, len(daily))
    daily.to_csv(DAILY_FEATURES_PATH, index=False)
    return daily


if __name__ == "__main__":
    logging.info("Running statistical modelling layer...")
    daily_df = run_full_analysis()
    logging.info("Done. Preview:")
    print(daily_df.head())
