import sqlite3
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from data_pipeline import main as run_pipeline # Import the pipeline function

# --- Configuration ---
DB_FILE = "house_prices.db"
TABLE_NAME = "uk_hpi_cleaned"


# --- Database Initialization ---
def initialize_database():
    """Checks if the database exists, and if not, runs the ETL pipeline to create it."""
    if not os.path.exists(DB_FILE):
        print(f"Database not found at '{DB_FILE}'. Running ETL pipeline...")
        try:
            run_pipeline()
            print("ETL pipeline completed successfully.")
        except Exception as e:
            print(f"Error running ETL pipeline: {e}")
            # In a real-world scenario, you might want to exit here if the DB is critical
            # For this demo, we'll allow the app to start, but endpoints will fail.
    else:
        print(f"Database already exists at '{DB_FILE}'. Skipping ETL pipeline.")

# Run the initialization check when the application starts
initialize_database()


# --- FastAPI Application ---
# Create a FastAPI application instance
app = FastAPI(
    title="UK House Price Index API",
    description="An API to serve cleaned UK house price data.",
    version="1.0.0",
)

# --- Middleware ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Helper Functions ---
def db_connection():
    """Establishes a connection to the SQLite database."""
    return sqlite3.connect(DB_FILE)

# --- API Endpoints ---

@app.get("/", response_class=FileResponse)
async def serve_dashboard():
    """Serves the main dashboard HTML file."""
    return "index.html"

@app.get("/regions")
def get_regions():
    """Returns a distinct list of all region names."""
    conn = db_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        query = f"SELECT DISTINCT region_name FROM {TABLE_NAME} ORDER BY region_name"
        cursor.execute(query)
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
        query = f"""
            SELECT date, average_price, "index"
            FROM {TABLE_NAME}
            WHERE region_name = ?
            ORDER BY date
        """
        cursor.execute(query, (region_name,))
        data = [dict(row) for row in cursor.fetchall()]
        return {"region": region_name, "data": data}
    finally:
        conn.close()

