import os
import subprocess

from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from logger_config import log

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL")
TABLE_NAME = "uk_hpi_plus_affordability"

@asynccontextmanager
async def lifespan(app: FastAPI):
    initialize_database()
    yield


app = FastAPI(
    title="UK House Price Index API",
    description="Serve UK house price data alongside salary affordability metrics.",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)


def initialize_database() -> None:
    if not DATABASE_URL:
        log.critical("FATAL: DATABASE_URL not set. Cannot initialize database.")
        return

    log.info("Running ETL pipeline to create/update database...")
    try:
        subprocess.run(["python3", "data_pipeline.py"], check=True)
        log.info("ETL pipeline completed successfully.")
    except subprocess.CalledProcessError as exc:
        log.critical(f"FATAL: Error running ETL pipeline: {exc}")
    except FileNotFoundError as exc:
        log.critical(f"FATAL: Unable to execute data pipeline: {exc}")
def db_connection():
    if not DATABASE_URL:
        return None
    try:
        return create_engine(DATABASE_URL)
    except Exception as exc:
        log.error(f"Error creating database engine: {exc}")
        return None


@app.get("/", response_class=FileResponse)
async def read_index() -> FileResponse:
    return FileResponse(
        "index.html",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/regions")
async def get_regions():
    engine = db_connection()
    if not engine:
        raise HTTPException(status_code=503, detail="Database connection unavailable")

    query = text(f"SELECT DISTINCT region_name FROM {TABLE_NAME} ORDER BY region_name")
    try:
        with engine.connect() as conn:
            result = conn.execute(query)
            regions = [row[0] for row in result]
    except SQLAlchemyError as exc:
        log.error(f"Error querying regions: {exc}")
        raise HTTPException(status_code=503, detail="Unable to fetch data at this time")

    return {"regions": regions}


@app.get("/data/{region_name}")
async def get_data_for_region(region_name: str):
    engine = db_connection()
    if not engine:
        raise HTTPException(status_code=503, detail="Database connection unavailable")

    if engine.dialect.name == "sqlite":
        date_expr = "STRFTIME('%Y-%m-%d', date)"
    else:
        date_expr = "TO_CHAR(date, 'YYYY-MM-DD')"

    query = text(
        f"""
        SELECT
            {date_expr} AS date,
            average_price,
            "index",
            average_annual_salary,
            affordability_ratio
        FROM {TABLE_NAME}
        WHERE region_name = :region
        ORDER BY date
        """
    )

    try:
        with engine.connect() as conn:
            result = conn.execute(query, {"region": region_name})
            data = [dict(row._mapping) for row in result]
    except SQLAlchemyError as exc:
        log.error(f"Error querying region '{region_name}': {exc}")
        raise HTTPException(status_code=503, detail="Unable to fetch data at this time")

    return {"region": region_name, "data": data}
