import json
import logging
import os
from pathlib import Path
from typing import List
from dotenv import load_dotenv
from google import genai



import pandas as pd

load_dotenv()
PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"
JOINED_PATH = PROCESSED_DATA_DIR / "price_emissions_joined.csv"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def _fetch_last_rows(region: str, n: int = 6, path: Path = JOINED_PATH) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["timestamp"])
    if "REGIONID" in df.columns:
        df = df.rename(columns={"REGIONID": "region"})
    df = df[df["region"] == region].sort_values("timestamp")
    return df.tail(n)


def _build_prompt(region: str, df_tail: pd.DataFrame) -> str:
    lines: List[str] = [
        "You are an electricity market analyst.",
        "You get 5-minute NEM data for one region.",
        "",
        "Summarise what has happened over the last 30 minutes in 12 concise sentences,",
        "talking about price, demand, and emissions intensity.",
        "Mention directions (up/down) and rough magnitudes, and be concrete.",
        "Also mention the date",
        "",
        f"Region: {region}",
        "",
        "Here is the data (most recent row last):",
        "",
        "timestamp, price, total_demand, dispatchable_generation, net_interchange, emissions_intensity",
    ]
    for _, row in df_tail.iterrows():
        lines.append(
            f"{row['timestamp']}, {row['price']}, {row['total_demand']}, "
            f"{row['dispatchable_generation']}, {row['net_interchange']}, {row['emissions_intensity']}"
        )
    lines.append("")
    lines.append("Now produce the explanation.")
    return "\n".join(lines)


def generate_explanation(prompt: str, api_key: str) -> str:
    # Create a Gemini client using the provided API key
    client = genai.Client(api_key=api_key)

    # Call the Gemini 2.5 Flash model (current recommended fast model)
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
    )

    # response.text is a convenience property that joins all parts
    return response.text


def run_gemini_explainer(region: str = "NSW1", joined_path: Path = JOINED_PATH) -> str:
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("Please set GEMINI_API_KEY in your environment.")

    df_tail = _fetch_last_rows(region, n=6, path=joined_path)
    prompt = _build_prompt(region, df_tail)
    logging.info("Sending prompt to Gemini for region %s", region)
    explanation = generate_explanation(prompt, api_key)
    logging.info("Explanation generated.")
    return explanation



if __name__ == "__main__":
    text = run_gemini_explainer(region="NSW1")
    print(text)
