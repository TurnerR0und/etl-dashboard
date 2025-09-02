import pandas as pd
import sqlite3
import requests
import io
import os

# --- Configuration ---
def _resolve_db_path() -> str:
    """Resolve a writable DB path with sensible fallbacks.
    Order: $DB_FILE -> house_prices.db (CWD) -> /data/house_prices.db -> /tmp/house_prices.db
    """
    candidates = []
    env_path = os.getenv("DB_FILE")
    if env_path:
        candidates.append(env_path)
    candidates.extend([
        os.path.abspath("house_prices.db"),
        "/data/house_prices.db",
        "/tmp/house_prices.db",
    ])

    for path in candidates:
        try:
            dirpath = os.path.dirname(path) or "."
            os.makedirs(dirpath, exist_ok=True)
            if os.access(dirpath, os.W_OK):
                return path
        except Exception:
            continue
    # Fallback to current directory name if all else fails
    return os.path.abspath("house_prices.db")

DB_FILE = _resolve_db_path()
TABLE_NAME = "uk_hpi_cleaned"
DATA_URL = "https://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/UK-HPI-full-file-2025-06.csv?utm_medium=GOV.UK&utm_source=datadownload&utm_campaign=full_fil&utm_term=9.30_20_08_25"


def fetch_data(url: str) -> pd.DataFrame:
    """Fetches CSV data from a URL and returns it as a pandas DataFrame."""
    print(f"Fetching data from {url}...")
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises an exception for bad status codes
        print("Data fetched successfully.")
        csv_content = io.StringIO(response.text)
        df = pd.read_csv(csv_content)
        return df
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame() # Return empty dataframe on error


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans and transforms the raw house price index data."""
    if df.empty:
        return df

    print("Cleaning data...")
    # Clean column names (lowercase, remove special chars, replace spaces with _)
    df.columns = df.columns.str.lower().str.replace('[^a-zA-Z0-9_]', '', regex=True).str.replace(' ', '_')

    # Rename columns for clarity and consistency
    rename_map = {
        'regionname': 'region_name',
        'averageprice': 'average_price'
    }
    df = df.rename(columns=rename_map)

    # Select only the columns we need
    required_columns = ['date', 'region_name', 'average_price', 'index']
    # Check if all required columns exist
    if not all(col in df.columns for col in required_columns):
        print("Error: Missing one or more required columns after cleaning.")
        return pd.DataFrame()

    df_selected = df[required_columns].copy()

    # Convert 'date' column to datetime objects
    df_selected['date'] = pd.to_datetime(df_selected['date'])

    # Drop rows where region_name is null
    df_selected.dropna(subset=['region_name'], inplace=True)

    print("Data cleaned and columns selected.")
    return df_selected


def load_data_to_db(df: pd.DataFrame, db_file: str, table_name: str):
    """Loads a DataFrame into a SQLite database, replacing the table if it exists."""
    if df.empty:
        print("Skipping database load because the dataframe is empty.")
        return

    print(f"Loading data into {db_file}...")
    try:
        conn = sqlite3.connect(db_file)
        # Use if_exists='replace' to handle creating/dropping the table
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print("Data loaded successfully.")
    except Exception as e:
        print(f"Error loading data to database: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()


def main():
    """Main function to run the ETL pipeline."""
    raw_df = fetch_data(DATA_URL)
    cleaned_df = clean_data(raw_df)
    load_data_to_db(cleaned_df, DB_FILE, TABLE_NAME) # Corrected function name

# This allows the script to be run directly
if __name__ == "__main__":
    main()
