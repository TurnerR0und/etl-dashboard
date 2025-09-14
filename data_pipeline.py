import os
import io
import pandas as pd
import httpx
import asyncio
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError, Field
from datetime import date
from logger_config import log

# Load environment variables
load_dotenv()

# --- Configuration ---
HPI_DATA_URL = "https://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/UK-HPI-full-file-2025-06.csv?utm_medium=GOV.UK&utm_source=datadownload&utm_campaign=full_fil&utm_term=9.30_20_08_25"
SALARY_DATA_URL = "https://www.ons.gov.uk/file?uri=/employmentandlabourmarket/peopleinwork/earningsandworkinghours/datasets/grossweeklyearningsoffulltimeemployeesbyregionearn05/current/earn05aug2025.xls"
DATABASE_URL = os.environ.get("DATABASE_URL")
TABLE_NAME = "uk_hpi_plus_affordability"

# --- Pydantic Data Validation Model ---
class AffordabilityModel(BaseModel):
    date: date
    region_name: str
    average_price: float
    index: float
    average_annual_salary: float | None = Field(default=None)
    affordability_ratio: float | None = Field(default=None)

# --- ETL Functions ---

async def fetch_data(session: httpx.AsyncClient, url: str, data_name: str) -> bytes | None:
    """Fetches raw content (bytes) asynchronously from a URL."""
    log.info(f"Fetching {data_name} data from {url}...")
    try:
        response = await session.get(url, timeout=60.0, follow_redirects=True)
        response.raise_for_status()
        log.info(f"{data_name} data fetched successfully.")
        return response.content
    except httpx.RequestError as e:
        log.error(f"Error fetching {data_name} data: {e}")
        return None

def clean_house_price_data(content: bytes) -> pd.DataFrame | None:
    """Cleans and transforms the raw house price CSV content."""
    if content is None: return None
    log.info("Cleaning house price data...")
    df = pd.read_csv(io.BytesIO(content))
    df = df[['Date', 'RegionName', 'AveragePrice', 'Index']].rename(columns={
        'Date': 'date', 'RegionName': 'region_name',
        'AveragePrice': 'average_price', 'Index': 'index'
    })
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df.dropna(subset=['date', 'region_name', 'average_price', 'index'], inplace=True)
    df['year'] = df['date'].dt.year
    log.info("House price data cleaned.")
    return df

def clean_salary_data(content: bytes) -> pd.DataFrame | None:
    """Cleans and transforms the raw salary Excel content."""
    if content is None: return None
    log.info("Cleaning salary data from Excel file...")
    df = pd.read_excel(io.BytesIO(content), sheet_name='All', header=5)
    
    df = df[['Region', '2025']].rename(columns={'Region': 'region_name', '2025': 'weekly_pay'})
    df.dropna(subset=['region_name', 'weekly_pay'], inplace=True)

    df['year'] = 2025
    df['weekly_pay'] = pd.to_numeric(df['weekly_pay'], errors='coerce')
    df.dropna(inplace=True)

    df['average_annual_salary'] = df['weekly_pay'] * 52
    
    region_mapping = {'East': 'East of England'}
    df['region_name'] = df['region_name'].replace(region_mapping)
    log.info("Salary data cleaned.")
    return df[['year', 'region_name', 'average_annual_salary']]

def merge_and_transform_data(prices_df: pd.DataFrame, salaries_df: pd.DataFrame) -> pd.DataFrame | None:
    """Merges the two dataframes and calculates the affordability ratio."""
    if prices_df is None or salaries_df is None: return None
    log.info("Merging house price and salary data...")
    merged_df = pd.merge(prices_df, salaries_df, on=['year', 'region_name'], how='left')
    merged_df['affordability_ratio'] = merged_df['average_price'] / merged_df['average_annual_salary']
    log.info("Data merged and affordability ratio calculated.")
    return merged_df

def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    if df is None: return None
    log.info("Validating final merged data...")
    validated_rows = []
    error_count = 0
    for _, row in df.iterrows():
        try:
            validated_rows.append(AffordabilityModel(**row.to_dict()).model_dump())
        except ValidationError:
            error_count += 1
    
    if error_count > 0:
        log.warning(f"Validation complete. Found {error_count} invalid rows.")
    else:
        log.info("Validation successful. All rows are valid.")
    
    final_cols = list(AffordabilityModel.model_fields.keys())
    return pd.DataFrame(validated_rows)[final_cols]


def load_data_to_db(df: pd.DataFrame, db_url: str, table_name: str):
    if df is None or df.empty or not db_url:
        log.warning("Data loading skipped.")
        return
    log.info(f"Loading {len(df)} rows into table '{table_name}'...")
    try:
        engine = create_engine(db_url)
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        log.info("Data loaded successfully.")
    except Exception as e:
        log.error(f"Error loading data to database: {e}")

async def main():
    """Main function to run the full ETL pipeline."""
    if not DATABASE_URL:
        log.critical("FATAL: DATABASE_URL not set. Aborting.")
        return

    async with httpx.AsyncClient() as session:
        hpi_task = fetch_data(session, HPI_DATA_URL, "house price")
        salary_task = fetch_data(session, SALARY_DATA_URL, "salary")
        raw_hpi_content, raw_salary_content = await asyncio.gather(hpi_task, salary_task)
    
    cleaned_hpi_df = clean_house_price_data(raw_hpi_content)
    cleaned_salary_df = clean_salary_data(raw_salary_content)
    
    merged_df = merge_and_transform_data(cleaned_hpi_df, cleaned_salary_df)
    
    validated_df = validate_data(merged_df)
    
    load_data_to_db(validated_df, DATABASE_URL, TABLE_NAME)

if __name__ == "__main__":
    asyncio.run(main())