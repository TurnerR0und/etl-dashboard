import sqlite3
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# --- Configuration ---
DB_FILE = "house_prices.db"
TABLE_NAME = "uk_hpi"

# Create a FastAPI application instance
app = FastAPI(
    title="UK House Price Index API",
    description="An API to serve cleaned UK house price data.",
    version="1.0.0",
)

# --- CORS Middleware ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def db_connection():
    """Establishes a connection to the SQLite database."""
    return sqlite3.connect(DB_FILE)


@app.get("/")
def read_root():
    """Root endpoint for the API."""
    return {"message": "Welcome to the UK House Price Index API. Go to /docs for documentation."}


@app.get("/regions")
def get_regions():
    """Endpoint to get a list of all unique regions."""
    conn = db_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        # CORRECTED: Query for 'region_name', which will exist in the new database.
        query = f"SELECT DISTINCT region_name FROM {TABLE_NAME} ORDER BY region_name"
        cursor.execute(query)
        regions = [row['region_name'] for row in cursor.fetchall()]
        return {"regions": regions}
    finally:
        conn.close()


@app.get("/data/{region_name}")
def get_data_for_region(region_name: str):
    """Endpoint to get all time-series data for a specific region."""
    conn = db_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        # CORRECTED: "index" is a reserved SQL keyword, so we must wrap it in quotes
        # to specify that we are referring to the column name.
        query = f"""
            SELECT date, average_price, "index" 
            FROM {TABLE_NAME} 
            WHERE region_name = ? 
            ORDER BY date
        """
        cursor.execute(query, (region_name,))
        # No manual mapping needed now, as DB columns will match JSON keys.
        data = [dict(row) for row in cursor.fetchall()]
        return {"region": region_name, "data": data}
    finally:
        conn.close()

