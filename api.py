import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from logger_config import log

load_dotenv()

TABLE_NAME = "uk_hpi_plus_affordability"


def get_database_url() -> str | None:
    """Read the database URL from the current environment."""
    return os.environ.get("DATABASE_URL")

@asynccontextmanager
async def lifespan(app: FastAPI):
    await initialize_database()
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


async def initialize_database() -> None:
    database_url = get_database_url()
    if not database_url:
        log.critical("FATAL: DATABASE_URL not set. Cannot initialize database.")
        return

    log.info("Running ETL pipeline to create/update database...")
    try:
        from data_pipeline import main as run_pipeline
    except Exception as exc:  # pragma: no cover - import errors are logged and surfaced
        log.critical(f"FATAL: Unable to import data pipeline: {exc}")
        return

    try:
        await run_pipeline()
        log.info("ETL pipeline completed successfully.")
    except Exception as exc:
        log.critical(f"FATAL: Error running ETL pipeline: {exc}")
def db_connection():
    database_url = get_database_url()
    if not database_url:
        return None
    try:
        return create_engine(database_url)
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
            data = []
            for row in result:
                record = dict(row._mapping)
                price = record.get("average_price")
                salary = record.get("average_annual_salary")
                if price is not None and salary not in (None, 0):
                    record["affordability_ratio"] = price / salary
                data.append(record)
    except SQLAlchemyError as exc:
        log.error(f"Error querying region '{region_name}': {exc}")
        raise HTTPException(status_code=503, detail="Unable to fetch data at this time")

    return {"region": region_name, "data": data}
