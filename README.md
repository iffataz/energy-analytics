# Energy Analytics

Compact pipeline to pull Australian NEM public price data and IBEI emissions intensity, clean them, model daily features, and summarise recent movements with Gemini.

## What you get
- Ingest scripts that download the latest IBEI emissions summary and all current public price zips
- Cleaning functions to type, de-duplicate, and filter core fields
- Joins and daily feature engineering (correlations, rolling stats, anomaly flags)
- Optional Gemini explainer for the last 30 minutes
- Data outputs kept out of git via .gitignore so you can regenerate locally

## Project structure
`
config/                # reserved for config (currently empty)
dashboards/            # reserved for visual outputs (currently empty)
data/
  raw/                 # downloads (ignored by git)
  processed/           # cleaned + modelled CSVs (ignored by git)
notebooks/             # workspace for exploration (empty)
src/
  ingest/              # fetch_prices.py, fetch_emissions.py
  clean/               # clean_prices.py, clean_emissions.py
  transform/           # model_join.py, analysis_stats.py
  load/                # load_to_postgres.py
  ai/                  # gemini_explainer.py (optional)
requirements.txt
.env.example           # template env vars (copy to .env)
`

## Setup
1) Python 3.10+ recommended.
2) Create and activate a virtual env:
`
python -m venv .venv
.\.venv\Scripts\activate
`
3) Install dependencies:
`
pip install -r requirements.txt
`
4) Copy env template and fill in values:
`
copy .env.example .env
`
- GEMINI_API_KEY only needed for src/ai/gemini_explainer.py

## Data pipeline
Run modules as scripts to move from raw -> processed.

### 1) Ingest (download)
`
python src/ingest/fetch_emissions.py   # saves data/raw/ibei_latest.csv
python src/ingest/fetch_prices.py      # saves data/raw/public_prices_current_all.csv
`

### 2) Clean
`
python src/clean/clean_emissions.py    # writes data/processed/emissions_clean.csv
python src/clean/clean_prices.py       # writes data/processed/prices_clean.csv
`

### 3) Transform / join
`
python src/transform/model_join.py     # writes data/processed/price_emissions_joined.csv
`

### 4) Analytics features
`
python src/transform/analysis_stats.py # writes data/processed/daily_price_features.csv
`

### 5) Optional: load to Postgres
Set DATABASE_URL (or SUPABASE_DB_URL) then:
`
python src/load/load_to_postgres.py
`
Tables created in public schema: prices, emissions, price_emissions_joined, daily_price_features, price_emissions_regression_summary, price_forecast_sample (latter two only if files exist).

### 6) Optional: Gemini explainer
Requires GEMINI_API_KEY and a joined dataset at data/processed/price_emissions_joined.csv:
`
python src/ai/gemini_explainer.py
`
It fetches the last few rows for a region and returns a 12-sentence summary of price/demand/emissions moves.

## Notes
- Large data outputs and env files are ignored via .gitignore; regenerate locally when needed.
- The repo currently contains sample processed CSVs under data/; delete them if you do not want to keep local artifacts.
- No tests are provided; consider adding unit tests around parsing and joins before production use.

## Troubleshooting
- If parsing timestamps fails, confirm the raw AEMO files retain the YYYY/MM/DD HH:MM:SS format.
- Network downloads rely on 
equests; corporate proxies may need HTTP(S)_PROXY env vars.
- Postgres loads replace tables; back up if you need history.
