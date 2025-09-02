import os
import sqlite3
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import subprocess

# --- Configuration ---
# Use an environment variable for the database path, defaulting to a writable location.
DB_PATH = os.environ.get("DB_PATH", "/tmp/house_prices.db")
TABLE_NAME = "uk_hpi"

# --- Database Initialization ---
def initialize_database():
    """
    Runs the ETL pipeline to create and populate the database on application startup.
    This ensures the data is always fresh when the container starts.
    """
    print("Running ETL pipeline to create/update database...")
    try:
        # Run the pipeline script as a separate process
        subprocess.run(["python3", "data_pipeline.py"], check=True)
        print("ETL pipeline completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"FATAL: Error running ETL pipeline: {e}")
    except FileNotFoundError:
        print("FATAL: 'data_pipeline.py' not found. Cannot initialize database.")

# Run the initialization on startup
initialize_database()


# --- FastAPI Application ---
app = FastAPI(
    title="UK House Price Index API",
    description="An API to serve cleaned UK house price data and the frontend dashboard.",
    version="1.0.0",
)

# --- CORS Middleware ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allow all origins for this public demo
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# --- Helper Function ---
def db_connection():
    """Establishes a connection to the SQLite database."""
    return sqlite3.connect(DB_PATH)

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
    conn = db_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        query = f"SELECT DISTINCT region_name FROM {TABLE_NAME} ORDER BY region_name"
        cursor.execute(query)
        # Use a list comprehension for a clean and efficient way to build the list
        regions = [row['region_name'] for row in cursor.fetchall()]
        return {"regions": regions}
    finally:
        conn.close()

@app.get("/data/{region_name}")
def get_data_for_region(region_name: str):
    """Returns all data points for a specific region."""
    conn = db_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        # Use a parameterized query to prevent SQL injection vulnerabilities
        query = f"""
            SELECT date, average_price, "index"
            FROM {TABLE_NAME}
            WHERE region_name = ?
            ORDER BY date
        """
        cursor.execute(query, (region_name,))
        # Convert each row to a dictionary for easy JSON serialization
        data = [dict(row) for row in cursor.fetchall()]
        return {"region": region_name, "data": data}
    finally:
        conn.close()

