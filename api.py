import os
import sqlite3
import subprocess
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# --- Configuration ---
# Use the same DB resolution strategy as the ETL to avoid divergence.
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
    return sqlite3.connect(DB_FILE, check_same_thread=False)

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

# Also enforce no-cache for root via middleware, in case other routes serve HTML later
@app.middleware("http")
async def no_cache_html(request: Request, call_next):
    response = await call_next(request)
    if request.url.path in {"/", "/index.html"}:
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response

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
            SELECT
                COALESCE(date(date), substr(date, 1, 10)) AS date,
                average_price,
                "index"
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
