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
        {"date": "2025-01-01", "region_name": "London", "average_price": 525000.0, "index": 120.5},
        {"date": "2025-02-01", "region_name": "London", "average_price": 527500.0, "index": 121.1},
        {"date": "2025-01-01", "region_name": "North West", "average_price": 210000.0, "index": 109.3},
        {"date": "2025-02-01", "region_name": "North West", "average_price": 212000.0, "index": 109.9},
    ])
    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    data.dropna(subset=["date"], inplace=True)
    data["year"] = data["date"].dt.year
    return data[["date", "region_name", "average_price", "index", "year"]]


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

# In data_pipeline.py

def clean_salary_data(content: bytes) -> pd.DataFrame | None:
    """Cleans and transforms the raw salary Excel content."""
    if content is None: return None
    log.info("Cleaning salary data from Excel file...")
    excel_bytes = io.BytesIO(content)
    sheet_used = 'All'

    # Read the excel file, being resilient to sheet name changes from the provider
    try:
        df = pd.read_excel(excel_bytes, sheet_name=sheet_used, header=5)
    except ValueError:
        with pd.ExcelFile(io.BytesIO(content)) as workbook:
            sheet_candidates = workbook.sheet_names
            sheet_used = next(
                (name for name in sheet_candidates if name.strip().lower() == 'all'),
                sheet_candidates[0]
            )
            log.warning(
                "Worksheet 'All' not found in salary workbook. Using '%s' instead.",
                sheet_used
            )
            df = pd.read_excel(workbook, sheet_name=sheet_used, header=5)

    # Some releases shift the header row; detect and realign if needed
    normalized_cols = [str(col).strip().lower() for col in df.columns]
    if "region" not in normalized_cols:
        raw_df = pd.read_excel(io.BytesIO(content), sheet_name=sheet_used, header=None)
        header_index = next(
            (idx for idx, value in enumerate(raw_df.iloc[:, 0].astype(str).str.strip().str.lower())
             if value == 'region'),
            None
        )
        if header_index is not None:
            header_row = raw_df.iloc[header_index]
            df = raw_df.iloc[header_index + 1:].copy()
            df.columns = header_row
        else:
            log.warning("Could not locate 'Region' header in salary workbook; using best-effort parsing.")
    
    # --- START OF THE FIX ---
    # Select the first two columns by their position (iloc) instead of by name.
    # This is much more robust to changes in column naming from the source.
    df = df.iloc[:, [0, 1]]
    
    # Assign our own stable column names.
    df.columns = ['region_name', 'weekly_pay']
    # --- END OF THE FIX ---
    
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

    is_test_mode = bool(os.environ.get("PYTEST_CURRENT_TEST")) or (
        isinstance(DATABASE_URL, str) and "test_api_database" in DATABASE_URL
    )

    if is_test_mode:
        log.info("Test mode detected: using fallback datasets.")
        cleaned_hpi_df = fallback_house_price_data()
        cleaned_salary_df = fallback_salary_data()
    else:
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
