import os
import subprocess
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables from .env file for local development
load_dotenv()

# --- Configuration ---
DATABASE_URL = os.environ.get("DATABASE_URL")
TABLE_NAME = "uk_hpi"

# --- Database Initialization ---
def initialize_database():
    """
    Runs the ETL pipeline to create and populate the database on application startup.
    This ensures the data is always fresh when the container starts.
    """
    if not DATABASE_URL:
        print("FATAL: DATABASE_URL not set. Cannot initialize database.")
        return

    print("Running ETL pipeline to create/update database...")
    try:
        subprocess.run(["python3", "data_pipeline.py"], check=True)
        print("ETL pipeline completed successfully.")
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        print(f"FATAL: Error running ETL pipeline: {e}")

# Run the initialization on startup
initialize_database()

# --- FastAPI Application ---
app = FastAPI(
    title="UK House Price Index API",
    description="An API to serve cleaned UK house price data and the frontend dashboard.",
    version="2.0.0", # Version bump!
)

# --- CORS Middleware ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# --- Helper Function ---
def db_connection():
    """Establishes a SQLAlchemy engine connection to the database."""
    if not DATABASE_URL:
        return None
    try:
        return create_engine(DATABASE_URL)
    except Exception as e:
        print(f"Error creating database engine: {e}")
        return None

# --- API Endpoints ---

@app.get("/", response_class=FileResponse)
async def read_index():
    """
    Serves the frontend dashboard.
    Includes cache-control headers to prevent browser caching issues.
    """
    return FileResponse("index.html", headers={
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0",
    })

@app.get("/regions")
def get_regions():
    """Returns a distinct list of all region names."""
    engine = db_connection()
    if not engine:
        return {"error": "Database connection failed"}
    with engine.connect() as conn:
        query = text(f"SELECT DISTINCT region_name FROM {TABLE_NAME} ORDER BY region_name")
        result = conn.execute(query)
        regions = [row[0] for row in result]
        return {"regions": regions}

@app.get("/data/{region_name}")
def get_data_for_region(region_name: str):
    """Returns all data points for a specific region, with formatted date."""
    engine = db_connection()
    if not engine:
        return {"error": "Database connection failed"}
    with engine.connect() as conn:
        # Use TO_CHAR for PostgreSQL date formatting
        query = text(f"""
            SELECT TO_CHAR(date, 'YYYY-MM-DD') as date, average_price, "index"
            FROM {TABLE_NAME}
            WHERE region_name = :region
            ORDER BY date
        """)
        result = conn.execute(query, {"region": region_name})
        data = [dict(row._mapping) for row in result]
        return {"region": region_name, "data": data}

