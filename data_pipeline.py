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


def fallback_house_price_data() -> pd.DataFrame:
    """Provides a minimal, static dataset when remote downloads fail."""
    log.warning("Falling back to bundled house price sample dataset.")
    data = pd.DataFrame([
        {"date": "2025-01-01", "parent_region": "London", "region_name": "London", "average_price": 525000.0, "index": 120.5},
        {"date": "2025-02-01", "parent_region": "London", "region_name": "London", "average_price": 527500.0, "index": 121.1},
        {"date": "2025-01-01", "parent_region": "North West", "region_name": "North West", "average_price": 210000.0, "index": 109.3},
        {"date": "2025-02-01", "parent_region": "North West", "region_name": "North West", "average_price": 212000.0, "index": 109.9},
    ])
    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    data.dropna(subset=["date"], inplace=True)
    data["year"] = data["date"].dt.year
    return data


def fallback_salary_data() -> pd.DataFrame:
    """Provides salary information to pair with the fallback HPI dataset."""
    log.warning("Falling back to bundled salary sample dataset.")
    return pd.DataFrame(
        [
            {"year": 2025, "region_name": "London", "average_annual_salary": 52000.0},
            {"year": 2025, "region_name": "North West", "average_annual_salary": 42000.0},
        ]
    )

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
        response = await session.get(url, timeout=180.0, follow_redirects=True)
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
    try:
        df = pd.read_csv(io.BytesIO(content), low_memory=False)
        # --- START OF THE FIX ---
        # Keep both the granular name ('OfficialName') and the parent region ('RegionName')
        df = df[['Date', 'RegionName', 'OfficialName', 'AveragePrice', 'Index']].rename(columns={
            'Date': 'date',
            'RegionName': 'parent_region',
            'OfficialName': 'region_name', # This is what appears in the dropdown
            'AveragePrice': 'average_price',
            'Index': 'index'
        })
        # --- END OF THE FIX ---
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df.dropna(subset=['date', 'parent_region', 'region_name', 'average_price', 'index'], inplace=True)
        df['year'] = df['date'].dt.year
        log.info("House price data cleaned.")
        return df
    except Exception as e:
        log.error(f"Failed to clean house price data: {e}")
        return None

def clean_salary_data(content: bytes) -> pd.DataFrame | None:
    """Cleans and transforms the raw salary Excel content."""
    if content is None: return None
    log.info("Cleaning salary data from Excel file...")
    try:
        excel_file = pd.ExcelFile(io.BytesIO(content))
        if not excel_file.sheet_names:
            log.error("No sheets found in the salary Excel file.")
            return None
        data_sheet_name = max(excel_file.sheet_names, key=lambda name: len(excel_file.parse(name)))
        log.info(f"Identified '{data_sheet_name}' as the data sheet.")
        df = excel_file.parse(data_sheet_name, header=5)
        
        df = df.iloc[:, [0, 1]]
        df.columns = ['region_name', 'weekly_pay']
        df.dropna(subset=['region_name', 'weekly_pay'], inplace=True)
        df['year'] = 2025 # Assuming salary data is for a single year
        df['weekly_pay'] = pd.to_numeric(df['weekly_pay'], errors='coerce')
        df.dropna(inplace=True)
        df['average_annual_salary'] = df['weekly_pay'] * 52
        region_mapping = {'East': 'East of England'}
        df['region_name'] = df['region_name'].replace(region_mapping)
        log.info("Salary data cleaned successfully.")
        return df[['year', 'region_name', 'average_annual_salary']]
    except Exception as e:
        log.error(f"A critical error occurred while cleaning salary data: {e}")
        return None


def merge_and_transform_data(prices_df: pd.DataFrame, salaries_df: pd.DataFrame) -> pd.DataFrame | None:
    """Merges the two dataframes and calculates the affordability ratio."""
    if prices_df is None or salaries_df is None: return None
    log.info("Merging house price and salary data...")
    # --- START OF THE FIX ---
    # Merge using the parent region from prices_df and region_name from salaries_df
    merged_df = pd.merge(
        prices_df,
        salaries_df,
        left_on=['year', 'parent_region'],
        right_on=['year', 'region_name'],
        how='left'
    )
    
    # Propagate the annual salary to all months in a year for each region
    # This fills the NaN values for months where there wasn't a direct match
    merged_df.sort_values(by=['region_name_x', 'date'], inplace=True)
    merged_df['average_annual_salary'] = merged_df.groupby('region_name_x')['average_annual_salary'].transform(lambda x: x.ffill().bfill())
    # --- END OF THE FIX ---
    
    merged_df['affordability_ratio'] = merged_df['average_price'] / merged_df['average_annual_salary']
    # Clean up columns before validation
    merged_df.rename(columns={'region_name_x': 'region_name'}, inplace=True)
    log.info("Data merged and affordability ratio calculated.")
    return merged_df

def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    if df is None: return pd.DataFrame()
    log.info("Validating final merged data...")
    validated_rows = []
    error_count = 0
    for _, row in df.iterrows():
        try:
            # Pydantic model doesn't know about 'parent_region', so select columns
            model_data = row.to_dict()
            relevant_data = {key: model_data.get(key) for key in AffordabilityModel.model_fields.keys()}
            validated_rows.append(AffordabilityModel(**relevant_data).model_dump())
        except ValidationError:
            error_count += 1
    
    if error_count > 0:
        log.warning(f"Validation complete. Found {error_count} invalid rows.")
    else:
        log.info("Validation successful. All rows are valid.")
    
    if not validated_rows:
        return pd.DataFrame()
        
    final_cols = list(AffordabilityModel.model_fields.keys())
    return pd.DataFrame(validated_rows)[final_cols]


def load_data_to_db(df: pd.DataFrame, db_url: str, table_name: str):
    if df is None or df.empty or not db_url:
        log.warning("Data loading skipped: No data to load.")
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

    # Try to fetch live data
    async with httpx.AsyncClient() as session:
        hpi_task = fetch_data(session, HPI_DATA_URL, "house price")
        salary_task = fetch_data(session, SALARY_DATA_URL, "salary")
        raw_hpi_content, raw_salary_content = await asyncio.gather(hpi_task, salary_task)

    cleaned_hpi_df = clean_house_price_data(raw_hpi_content)
    if cleaned_hpi_df is None or cleaned_hpi_df.empty:
        cleaned_hpi_df = fallback_house_price_data()

    cleaned_salary_df = clean_salary_data(raw_salary_content)
    if cleaned_salary_df is None or cleaned_salary_df.empty:
        cleaned_salary_df = fallback_salary_data()

    merged_df = merge_and_transform_data(cleaned_hpi_df, cleaned_salary_df)
    
    validated_df = validate_data(merged_df)
    
    load_data_to_db(validated_df, DATABASE_URL, TABLE_NAME)

if __name__ == "__main__":
    asyncio.run(main())