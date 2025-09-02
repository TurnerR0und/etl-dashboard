import os
import io
import pandas as pd
import requests
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError
from datetime import date

# Load environment variables from .env file for local development
load_dotenv()

# --- Configuration ---
DATA_URL = "https://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/UK-HPI-full-file-2025-06.csv?utm_medium=GOV.UK&utm_source=datadownload&utm_campaign=full_fil&utm_term=9.30_20_08_25"
DATABASE_URL = os.environ.get("DATABASE_URL")
TABLE_NAME = "uk_hpi"

# --- Pydantic Data Validation Model ---
class HPIModel(BaseModel):
    """Defines the schema for a single valid data row."""
    date: date
    region_name: str
    average_price: float
    index: float

# --- ETL Functions ---

def fetch_data(url: str) -> pd.DataFrame:
    """Fetches CSV data from a URL and returns it as a pandas DataFrame."""
    print(f"Fetching data from {url}...")
    try:
        response = requests.get(url)
        response.raise_for_status()
        print("Data fetched successfully.")
        csv_content = io.StringIO(response.text)
        return pd.read_csv(csv_content)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans and transforms the raw DataFrame."""
    if df is None: return None
    print("Cleaning data...")
    df = df[['Date', 'RegionName', 'AveragePrice', 'Index']].rename(columns={
        'Date': 'date',
        'RegionName': 'region_name',
        'AveragePrice': 'average_price',
        'Index': 'index'
    })
    # Convert 'date' column to datetime objects, coercing errors to NaT (Not a Time)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    # Drop rows where critical data is missing
    df.dropna(subset=['date', 'region_name', 'average_price', 'index'], inplace=True)
    print("Data cleaned and columns selected.")
    return df

def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """Validates the DataFrame against the Pydantic model."""
    if df is None: return None
    print("Validating data...")
    validated_rows = []
    error_count = 0
    for index, row in df.iterrows():
        try:
            # Pydantic automatically converts the pandas timestamp to a date object
            validated_rows.append(HPIModel(**row.to_dict()).model_dump())
        except ValidationError as e:
            # Log the error for the specific row but continue processing
            # print(f"Validation error on row {index}: {e.errors()}")
            error_count += 1
    
    if error_count > 0:
        print(f"Validation complete. Found {error_count} invalid rows (not loaded).")
    else:
        print("Validation successful. All rows are valid.")

    return pd.DataFrame(validated_rows)


def load_data_to_db(df: pd.DataFrame, db_url: str, table_name: str):
    """Loads a DataFrame into a PostgreSQL database table."""
    if df is None or df.empty or not db_url:
        print("Data loading skipped due to missing/empty DataFrame or database URL.")
        return
    print(f"Loading {len(df)} validated rows into database table '{table_name}'...")
    try:
        engine = create_engine(db_url)
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print("Data loaded successfully.")
    except Exception as e:
        print(f"Error loading data to database: {e}")

def main():
    """Main function to run the ETL pipeline."""
    if not DATABASE_URL:
        print("FATAL: DATABASE_URL environment variable not set. Aborting.")
        return

    raw_data_df = fetch_data(DATA_URL)
    cleaned_df = clean_data(raw_data_df)
    validated_df = validate_data(cleaned_df)
    load_data_to_db(validated_df, DATABASE_URL, TABLE_NAME)

if __name__ == "__main__":
    main()

