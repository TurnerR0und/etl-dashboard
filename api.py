import sqlite3
import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from data_pipeline import main as run_pipeline # Import the pipeline function

# --- Configuration ---
def _resolve_db_path() -> str:
    """Resolve a writable DB path with sensible fallbacks.
    Order: $DB_FILE -> house_prices.db (CWD) -> /data/house_prices.db -> /tmp/house_prices.db
    """
    candidates = []
    env_path = os.getenv("DB_FILE")
    if env_path:
        candidates.append(env_path)
    candidates.extend([
        os.path.abspath("house_prices.db"),
        "/data/house_prices.db",
        "/tmp/house_prices.db",
    ])

    for path in candidates:
        try:
            dirpath = os.path.dirname(path) or "."
            os.makedirs(dirpath, exist_ok=True)
            if os.access(dirpath, os.W_OK):
                return path
        except Exception:
            continue
    return os.path.abspath("house_prices.db")

DB_FILE = _resolve_db_path()
TABLE_NAME = "uk_hpi_cleaned"


# --- Database Initialization ---
def initialize_database():
    """
    Ensures a fresh database is created on application startup by re-running the
    ETL pipeline. The pipeline is configured to replace existing data.
    """
    print("Running ETL pipeline to create/update database...")
    try:
        run_pipeline()
        print("ETL pipeline completed successfully.")
    except Exception as e:
        # If the pipeline fails on startup, the app is not viable.
        print(f"FATAL: Error running ETL pipeline on startup: {e}")
        # In a real-world app, you might raise the exception to stop the server from starting.
        # raise e

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
    return sqlite3.connect(DB_FILE, check_same_thread=False)

# --- API Endpoints ---

@app.get("/", response_class=FileResponse)
async def serve_dashboard():
    """Serves the main dashboard HTML file."""
    return FileResponse("index.html")

@app.get("/regions")
def get_regions():
    """Returns a distinct list of all region names."""
    try:
        conn = db_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        query = f"SELECT DISTINCT region_name FROM {TABLE_NAME} ORDER BY region_name"
        cursor.execute(query)
        regions = [row['region_name'] for row in cursor.fetchall()]
        return {"regions": regions}
    except Exception as e:
        # Surface the root cause to logs and client
        print(f"ERROR /regions: {e} | DB_FILE={DB_FILE}")
        raise HTTPException(status_code=500, detail=f"Failed to load regions: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


@app.get("/data/{region_name}")
def get_data_for_region(region_name: str):
    """Returns all data points for a specific region."""
    try:
        conn = db_connection()
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        query = f"""
            SELECT date, average_price, "index"
            FROM {TABLE_NAME}
            WHERE region_name = ?
            ORDER BY date
        """
        cursor.execute(query, (region_name,))
        data = [dict(row) for row in cursor.fetchall()]
        return {"region": region_name, "data": data}
    except Exception as e:
        print(f"ERROR /data/{region_name}: {e} | DB_FILE={DB_FILE}")
        raise HTTPException(status_code=500, detail=f"Failed to load data for {region_name}: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


@app.get("/healthz")
def healthz():
    """Basic health and DB status for debugging."""
    info = {"db_file": DB_FILE, "table": TABLE_NAME}
    try:
        conn = db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (TABLE_NAME,),
        )
        exists = cursor.fetchone() is not None
        info["table_exists"] = exists
        if exists:
            cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
            info["row_count"] = cursor.fetchone()[0]
        return JSONResponse(info)
    except Exception as e:
        info["error"] = str(e)
        return JSONResponse(info, status_code=500)
    finally:
        try:
            conn.close()
        except Exception:
            pass
