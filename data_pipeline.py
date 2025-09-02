import os
import io
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables from .env file for local development
load_dotenv()

# --- Configuration ---
# The public URL for the data source
DATA_URL = "https://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/UK-HPI-full-file-2025-06.csv?utm_medium=GOV.UK&utm_source=datadownload&utm_campaign=full_fil&utm_term=9.30_20_08_25"
# Get the database connection string from environment variables
DATABASE_URL = os.environ.get("DATABASE_URL")
TABLE_NAME = "uk_hpi"

def fetch_data(url: str) -> pd.DataFrame:
    """Fetches CSV data from a URL and returns it as a pandas DataFrame."""
    print(f"Fetching data from {url}...")
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
        print("Data fetched successfully.")
        # Use io.StringIO to treat the CSV string as a file
        csv_content = io.StringIO(response.text)
        return pd.read_csv(csv_content)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans and transforms the raw DataFrame."""
    if df is None:
        return None
    print("Cleaning data...")
    # Select and rename columns for clarity and consistency
    df = df[['Date', 'RegionName', 'AveragePrice', 'Index']].rename(columns={
        'Date': 'date',
        'RegionName': 'region_name',
        'AveragePrice': 'average_price',
        'Index': 'index'
    })
    # Convert 'date' column to datetime objects
    df['date'] = pd.to_datetime(df['date'])
    print("Data cleaned and columns selected.")
    return df

def load_data_to_db(df: pd.DataFrame, db_url: str, table_name: str):
    """Loads a DataFrame into a PostgreSQL database table."""
    if df is None or not db_url:
        print("Data loading skipped due to missing DataFrame or database URL.")
        return
    print(f"Loading data into database table '{table_name}'...")
    try:
        # Create a SQLAlchemy engine to connect to the database
        engine = create_engine(db_url)
        # Load the DataFrame into the SQL table, replacing it if it already exists
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
    load_data_to_db(cleaned_df, DATABASE_URL, TABLE_NAME)

if __name__ == "__main__":
    main()

