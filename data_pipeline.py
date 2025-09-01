import pandas as pd
import sqlite3
import requests
import io

# --- Configuration ---
DATA_URL = "https://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/UK-HPI-full-file-2025-06.csv?utm_medium=GOV.UK&utm_source=datadownload&utm_campaign=full_fil&utm_term=9.30_20_08_25"
DB_FILE = "house_prices.db"
TABLE_NAME = "uk_hpi"


def fetch_data(url: str) -> pd.DataFrame:
    """Fetches CSV data from a URL and returns it as a pandas DataFrame."""
    print(f"Fetching data from {url}...")
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes
        csv_content = io.StringIO(response.text)
        df = pd.read_csv(csv_content)
        print("Data fetched successfully.")
        return df
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    except Exception as e:
        print(f"An error occurred while reading the data: {e}")
        return None


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans the raw DataFrame."""
    print("Cleaning data...")
    # A simple cleaning function: lowercase, replace spaces, etc.
    df.columns = [
        col.lower()
        .replace(" ", "_")
        .replace("%", "pc")
        .replace("(", "")
        .replace(")", "")
        for col in df.columns
    ]
    # Convert date column to datetime objects
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    # CORRECTED LOGIC:
    # The KEYS of this dictionary must match the column names AFTER the cleaning step above.
    # The VALUES are what we want the final column names to be in our database.
    columns_to_keep = {
        "date": "date",
        "regionname": "region_name",    # Select 'regionname', rename to 'region_name'
        "averageprice": "average_price",# Select 'averageprice', rename to 'average_price'
        "index": "index",
    }
    
    # Filter for columns that actually exist in the dataframe
    existing_columns = {k: v for k, v in columns_to_keep.items() if k in df.columns}
    
    df = df[list(existing_columns.keys())]
    df = df.rename(columns=existing_columns) # This renames 'regionname' to 'region_name' etc.
    
    print("Data cleaned and columns selected.")
    return df


def load_to_db(df: pd.DataFrame, db_file: str, table_name: str):
    """Loads a DataFrame into a SQLite database."""
    print(f"Loading data into {db_file}...")
    try:
        with sqlite3.connect(db_file) as conn:
            df.to_sql(table_name, conn, if_exists="replace", index=False)
        print("Data loaded successfully.")
    except Exception as e:
        print(f"Error loading data to database: {e}")


def main():
    """Main function to run the ETL pipeline."""
    raw_df = fetch_data(DATA_URL)
    if raw_df is not None:
        cleaned_df = clean_data(raw_df)
        load_to_db(cleaned_df, DB_FILE, TABLE_NAME)


if __name__ == "__main__":
    main()

