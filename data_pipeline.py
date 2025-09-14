# In data_pipeline.py
import os
import io
import pandas as pd
import httpx  # <-- 1. Import httpx instead of requests
import asyncio # <-- 2. Import asyncio to run the async main function
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError
from datetime import date
from logger_config import log

# Load environment variables from .env file for local development
load_dotenv()

# --- Configuration ---
DATA_URL = "https://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/UK-HPI-full-file-2025-06.csv?utm_medium=GOV.UK&utm_source=datadownload&utm_campaign=full_fil&utm_term=9.30_20_08_25"
DATABASE_URL = os.environ.get("DATABASE_URL")
TABLE_NAME = "uk_hpi"

# --- Pydantic Data Validation Model ---
class HPIModel(BaseModel):
    date: date
    region_name: str
    average_price: float
    index: float

# --- ETL Functions ---

# 3. Make the function asynchronous with 'async def'
async def fetch_data(url: str) -> pd.DataFrame:
    """Fetches CSV data asynchronously from a URL and returns it as a pandas DataFrame."""
    log.info(f"Fetching data from {url}...")
    try:
        # 4. Use an async client and 'await' the request
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=30.0) # Add a timeout
            response.raise_for_status()
        
        log.info("Data fetched successfully.")
        csv_content = io.StringIO(response.text)
        return pd.read_csv(csv_content)
    except httpx.RequestError as e:
        log.error(f"Error fetching data: {e}")
        return None

# The rest of the synchronous functions (clean, validate, load) remain unchanged
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    # ... (no changes here)
    if df is None: return None
    log.info("Cleaning data...")
    df = df[['Date', 'RegionName', 'AveragePrice', 'Index']].rename(columns={
        'Date': 'date',
        'RegionName': 'region_name',
        'AveragePrice': 'average_price',
        'Index': 'index'
    })
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df.dropna(subset=['date', 'region_name', 'average_price', 'index'], inplace=True)
    log.info("Data cleaned and columns selected.")
    return df

def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    # ... (no changes here)
    if df is None: return None
    log.info("Validating data...")
    validated_rows = []
    error_count = 0
    for index, row in df.iterrows():
        try:
            validated_rows.append(HPIModel(**row.to_dict()).model_dump())
        except ValidationError:
            error_count += 1
    
    if error_count > 0:
        log.warning(f"Validation complete. Found {error_count} invalid rows (not loaded).")
    else:
        log.info("Validation successful. All rows are valid.")

    return pd.DataFrame(validated_rows)


def load_data_to_db(df: pd.DataFrame, db_url: str, table_name: str):
    # ... (no changes here)
    if df is None or df.empty or not db_url:
        log.warning("Data loading skipped due to missing/empty DataFrame or database URL.")
        return
    log.info(f"Loading {len(df)} validated rows into database table '{table_name}'...")
    try:
        engine = create_engine(db_url)
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        log.info("Data loaded successfully.")
    except Exception as e:
        log.error(f"Error loading data to database: {e}")


# 5. The main function also needs to be async to 'await' other async functions
async def main():
    """Main function to run the ETL pipeline."""
    if not DATABASE_URL:
        log.critical("FATAL: DATABASE_URL environment variable not set. Aborting.")
        return

    # 6. 'await' the result of our async fetch_data function
    raw_data_df = await fetch_data(DATA_URL)
    cleaned_df = clean_data(raw_data_df)
    validated_df = validate_data(cleaned_df)
    load_data_to_db(validated_df, DATABASE_URL, TABLE_NAME)

if __name__ == "__main__":
    # 7. Use asyncio.run() to execute the top-level async main function
    asyncio.run(main())