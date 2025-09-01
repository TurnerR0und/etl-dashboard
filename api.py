import sqlite3
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

# --- Configuration ---
DB_FILE = "house_prices.db"
TABLE_NAME = "uk_hpi_cleaned"

# Create a FastAPI application instance
app = FastAPI(
    title="UK House Price Index API",
    description="An API to serve cleaned UK house price data.",
    version="1.0.0",
)

# --- Middleware ---
# This allows the frontend (even when opened as a local file) to call the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
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
    # Return rows as dictionary-like objects
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        query = f"SELECT DISTINCT region_name FROM {TABLE_NAME} ORDER BY region_name"
        cursor.execute(query)
        # Use a list comprehension for a concise and readable way to build the list
        regions = [row['region_name'] for row in cursor.fetchall()]
        return {"regions": regions}
    finally:
        # Ensure the connection is always closed
        conn.close()


@app.get("/data/{region_name}")
def get_data_for_region(region_name: str):
    """
    Returns all data points for a specific region.
    The region_name is a path parameter.
    """
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
        
        # Convert each row to a dictionary
        data = [dict(row) for row in cursor.fetchall()]
        
        return {"region": region_name, "data": data}
    finally:
        conn.close()

