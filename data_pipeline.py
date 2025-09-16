import asyncio
import io
import os
from datetime import date

import httpx
import pandas as pd
from dotenv import load_dotenv
from logger_config import log
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy import create_engine

load_dotenv()

HPI_DATA_URL = "https://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/UK-HPI-full-file-2025-06.csv?utm_medium=GOV.UK&utm_source=datadownload&utm_campaign=full_fil&utm_term=9.30_20_08_25"
SALARY_DATA_URL = "https://www.ons.gov.uk/file?uri=/employmentandlabourmarket/peopleinwork/earningsandworkinghours/datasets/grossweeklyearningsoffulltimeemployeesbyregionearn05/current/earn05aug2025.xls"
DATABASE_URL = os.environ.get("DATABASE_URL")
TABLE_NAME = "uk_hpi_plus_affordability"


class AffordabilityModel(BaseModel):
    date: date
    region_name: str
    average_price: float
    index: float
    average_annual_salary: float | None = Field(default=None)
    affordability_ratio: float | None = Field(default=None)


def fallback_house_price_data() -> pd.DataFrame:
    log.warning("Falling back to bundled house price sample dataset.")
    data = pd.DataFrame(
        [
            {
                "date": "2025-01-01",
                "parent_region": "London",
                "region_name": "London",
                "average_price": 525000.0,
                "index": 120.5,
            },
            {
                "date": "2025-02-01",
                "parent_region": "London",
                "region_name": "London",
                "average_price": 527500.0,
                "index": 121.1,
            },
            {
                "date": "2025-01-01",
                "parent_region": "North West",
                "region_name": "Manchester",
                "average_price": 210000.0,
                "index": 109.3,
            },
            {
                "date": "2025-02-01",
                "parent_region": "North West",
                "region_name": "Manchester",
                "average_price": 212000.0,
                "index": 109.9,
            },
        ]
    )
    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    data.dropna(subset=["date"], inplace=True)
    data["year"] = data["date"].dt.year
    return data


def fallback_salary_data() -> pd.DataFrame:
    log.warning("Falling back to bundled salary sample dataset.")
    return pd.DataFrame(
        [
            {"year": 2025, "region_name": "London", "average_annual_salary": 52000.0},
            {"year": 2025, "region_name": "North West", "average_annual_salary": 42000.0},
        ]
    )


async def fetch_data(session: httpx.AsyncClient, url: str, data_name: str) -> bytes | None:
    log.info(f"Fetching {data_name} data from {url}...")
    try:
        response = await session.get(url, timeout=180.0, follow_redirects=True)
        response.raise_for_status()
        log.info(f"{data_name} data fetched successfully.")
        return response.content
    except httpx.RequestError as exc:
        log.error(f"Error fetching {data_name} data: {exc}")
        return None


def clean_house_price_data(content: bytes) -> pd.DataFrame | None:
    if content is None:
        return None

    log.info("Cleaning house price data...")
    try:
        raw_df = pd.read_csv(io.BytesIO(content), low_memory=False)
    except Exception as exc:
        log.error(f"Failed to read house price data: {exc}")
        return None

    required_cols = {"Date", "RegionName", "AveragePrice", "Index"}
    if not required_cols.issubset(raw_df.columns):
        missing = required_cols - set(raw_df.columns)
        log.error(f"House price data missing required columns: {missing}")
        return None

    granular_candidates = ["OfficialName", "TownName", "DistrictName"]
    granular_col = next((col for col in granular_candidates if col in raw_df.columns), None)

    base_df = pd.DataFrame(
        {
            "date": pd.to_datetime(raw_df["Date"], errors="coerce"),
            "parent_region": raw_df["RegionName"],
            "average_price": raw_df["AveragePrice"],
            "index": raw_df["Index"],
        }
    )

    if granular_col:
        base_df["region_name"] = raw_df[granular_col]
    else:
        base_df["region_name"] = base_df["parent_region"]

    base_df["region_name"] = base_df["region_name"].where(
        base_df["region_name"].notna(), base_df["parent_region"]
    )

    base_df.dropna(subset=["date", "parent_region", "region_name", "average_price", "index"], inplace=True)
    base_df["year"] = base_df["date"].dt.year

    log.info("House price data cleaned.")
    return base_df[["date", "parent_region", "region_name", "average_price", "index", "year"]]


def _select_salary_sheet(excel_file: pd.ExcelFile) -> str:
    preferred = next(
        (name for name in excel_file.sheet_names if name.strip().lower() == "all"),
        None,
    )
    if preferred:
        return preferred

    sheet_name, max_rows = excel_file.sheet_names[0], -1
    for name in excel_file.sheet_names:
        preview = excel_file.parse(name, header=None, nrows=200)
        non_empty = preview.dropna(how="all").shape[0]
        if non_empty > max_rows:
            sheet_name, max_rows = name, non_empty
    return sheet_name


def clean_salary_data(content: bytes) -> pd.DataFrame | None:
    if content is None:
        return None

    log.info("Cleaning salary data from Excel file...")
    try:
        excel_file = pd.ExcelFile(io.BytesIO(content))
    except Exception as exc:
        log.error(f"Failed to read salary workbook: {exc}")
        return None

    if not excel_file.sheet_names:
        log.error("No sheets found in the salary Excel file.")
        return None

    sheet_used = _select_salary_sheet(excel_file)
    log.info(f"Identified '{sheet_used}' as the salary data sheet.")

    raw_sheet = excel_file.parse(sheet_used, header=None)
    raw_sheet.dropna(how="all", inplace=True)

    header_index = None
    first_col = raw_sheet.iloc[:, 0].astype(str).str.strip().str.lower()
    for idx, value in first_col.items():
        if value == "region":
            header_index = idx
            break

    if header_index is None:
        log.warning(
            "Could not locate 'Region' header in salary sheet '%s'; defaulting to first row.",
            sheet_used,
        )
        header_index = raw_sheet.index.min()

    header_row = raw_sheet.loc[header_index].astype(str)
    data_rows = raw_sheet.loc[header_index + 1 :].copy()
    data_rows.columns = header_row
    data_rows.dropna(how="all", inplace=True)

    region_col = next(
        (col for col in data_rows.columns if str(col).strip().lower() == "region"),
        None,
    )
    if region_col is None:
        log.error("Region column not found in salary data sheet '%s'.", sheet_used)
        return None

    pay_col = next(
        (col for col in data_rows.columns if str(col).strip().isdigit()),
        None,
    )
    if pay_col is None and len(data_rows.columns) > 1:
        pay_col = data_rows.columns[1]
    if pay_col is None:
        log.error("Unable to determine a salary column in sheet '%s'.", sheet_used)
        return None

    cleaned_df = data_rows[[region_col, pay_col]].rename(
        columns={region_col: "region_name", pay_col: "weekly_pay"}
    )

    cleaned_df.dropna(subset=["region_name", "weekly_pay"], inplace=True)
    cleaned_df["weekly_pay"] = pd.to_numeric(cleaned_df["weekly_pay"], errors="coerce")
    cleaned_df.dropna(subset=["weekly_pay"], inplace=True)
    cleaned_df["year"] = 2025
    cleaned_df["average_annual_salary"] = cleaned_df["weekly_pay"] * 52

    region_mapping = {"East": "East of England"}
    cleaned_df["region_name"] = cleaned_df["region_name"].replace(region_mapping)

    log.info("Salary data cleaned successfully.")
    return cleaned_df[["year", "region_name", "average_annual_salary"]]


def merge_and_transform_data(prices_df: pd.DataFrame, salaries_df: pd.DataFrame) -> pd.DataFrame | None:
    if prices_df is None or salaries_df is None:
        return None

    log.info("Merging house price and salary data...")
    merged_df = pd.merge(
        prices_df,
        salaries_df,
        left_on=["year", "parent_region"],
        right_on=["year", "region_name"],
        how="left",
        suffixes=("", "_salary"),
    )

    salary_lookup = (
        salaries_df.drop_duplicates(subset=["year", "region_name"])
        .set_index(["year", "region_name"])["average_annual_salary"]
    )
    if merged_df["average_annual_salary"].isna().any():
        merged_df["average_annual_salary"] = merged_df.apply(
            lambda row: salary_lookup.get((row["year"], row["parent_region"])), axis=1
        )

    merged_df.sort_values(by=["region_name", "date"], inplace=True)
    merged_df["average_annual_salary"] = merged_df.groupby("parent_region")[
        "average_annual_salary"
    ].transform(lambda x: x.ffill().bfill())

    merged_df.drop(columns=["region_name_salary"], inplace=True, errors="ignore")

    merged_df["affordability_ratio"] = merged_df["average_price"] / merged_df["average_annual_salary"]

    log.info("Data merged and affordability ratio calculated.")
    return merged_df


def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()

    log.info("Validating final merged data...")
    final_cols = list(AffordabilityModel.model_fields.keys())
    validated_rows: list[dict] = []
    error_count = 0

    for _, row in df.iterrows():
        payload = {key: row.get(key) for key in final_cols}
        try:
            validated_rows.append(AffordabilityModel(**payload).model_dump())
        except ValidationError:
            error_count += 1

    if error_count:
        log.warning(f"Validation complete. Found {error_count} invalid rows.")
    else:
        log.info("Validation successful. All rows are valid.")

    return pd.DataFrame(validated_rows, columns=final_cols)


def load_data_to_db(df: pd.DataFrame, db_url: str, table_name: str) -> None:
    if df is None or df.empty or not db_url:
        log.warning("Data loading skipped: No data to load.")
        return

    log.info(f"Loading {len(df)} rows into table '{table_name}'...")
    try:
        engine = create_engine(db_url)
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        log.info("Data loaded successfully.")
    except Exception as exc:
        log.error(f"Error loading data to database: {exc}")


async def main() -> None:
    if not DATABASE_URL:
        log.critical("FATAL: DATABASE_URL not set. Aborting.")
        return

    async with httpx.AsyncClient() as session:
        hpi_task = fetch_data(session, HPI_DATA_URL, "house price")
        salary_task = fetch_data(session, SALARY_DATA_URL, "salary")
        raw_hpi_content, raw_salary_content = await asyncio.gather(hpi_task, salary_task)

    cleaned_hpi_df = clean_house_price_data(raw_hpi_content)
    cleaned_salary_df = clean_salary_data(raw_salary_content)

    if cleaned_hpi_df is None or cleaned_hpi_df.empty:
        cleaned_hpi_df = fallback_house_price_data()
    if cleaned_salary_df is None or cleaned_salary_df.empty:
        cleaned_salary_df = fallback_salary_data()

    merged_df = merge_and_transform_data(cleaned_hpi_df, cleaned_salary_df)
    validated_df = validate_data(merged_df)
    load_data_to_db(validated_df, DATABASE_URL, TABLE_NAME)


if __name__ == "__main__":
    asyncio.run(main())
