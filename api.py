import os
import subprocess
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import APIKeyHeader
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import redis
from logger_config import log 
#from cachetools import TTLCache # <-- Import TTLCache

# Load environment variables from .env file for local development
load_dotenv()

# --- Configuration ---
DATABASE_URL = os.environ.get("DATABASE_URL")
TABLE_NAME = "uk_hpi"
API_SECRET_TOKEN = os.environ.get("API_SECRET_TOKEN") # <-- Add this for security
# New Redis Configuration
REDIS_URL = os.environ.get("REDIS_URL") # <-- Add this
# --- Security Dependency ---
api_key_header = APIKeyHeader(name="X-API-KEY")

def get_api_key(api_key: str = Depends(api_key_header)):
    """Dependency to verify the API key."""
    if not API_SECRET_TOKEN or api_key != API_SECRET_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API Key"
        )
    return api_key

# --- Cache Configuration ---
# Cache for regions: max 1 item, expires every hour (3600 seconds) Commented out to use Redis
#regions_cache = TTLCache(maxsize=1, ttl=3600) 
# Cache for region data: max 100 regions, expires every hour
#region_data_cache = TTLCache(maxsize=100, ttl=3600)

# Initialize Redis connection
# The decode_responses=True is important - it ensures that data read 
# from Redis is automatically converted from bytes to strings.
try:
    if not REDIS_URL:
        raise ValueError("REDIS_URL environment variable not set.")
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    redis_client.ping() # Check the connection
    print("Successfully connected to Redis.")
    CACHE_ENABLED = True
except Exception as e:
    print(f"WARNING: Could not connect to Redis. Caching will be disabled. Error: {e}")
    redis_client = None
    CACHE_ENABLED = False

# --- Database Initialization ---
def initialize_database():
    """
    Runs the ETL pipeline to create and populate the database on application startup.
    This ensures the data is always fresh when the container starts.
    """
    if not DATABASE_URL:
        log.critical("FATAL: DATABASE_URL not set. Cannot initialize database.")
        return

    log.info("Running ETL pipeline to create/update database...")
    try:
        subprocess.run(["python3", "data_pipeline.py"], check=True)
        log.info("ETL pipeline completed successfully.")
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        log.critical(f"FATAL: Error running ETL pipeline: {e}")


# Run the initialization on startup
initialize_database()

# --- FastAPI Application ---
app = FastAPI(
    title="UK House Price Index API",
    description="An API to serve cleaned UK house price data and the frontend dashboard.",
    version="2.0.0",
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
        # This case is already handled by initialize_database, 
        # but it's good practice to keep the check.
        return None
    try:
        return create_engine(DATABASE_URL)
    except Exception as e:
        # Log the specific exception that occurred at an ERROR level
        log.error(f"Error creating database engine: {e}")
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
    if CACHE_ENABLED:
        # Check cache first
        cached_regions = redis_client.get("regions")
        if cached_regions:
            log.info("CACHE HIT: Returning regions from Redis.")
            import json
            return json.loads(cached_regions)

    log.info("CACHE MISS: Fetching regions from database.")
    # ... (the rest of the function is the same, but the caching part changes)
    engine = db_connection()
    if not engine:
        return {"error": "Database connection failed"}
    with engine.connect() as conn:
        query = text(f"SELECT DISTINCT region_name FROM {TABLE_NAME} ORDER BY region_name")
        result = conn.execute(query)
        regions = [row[0] for row in result]
        
        response_data = {"regions": regions}

        if CACHE_ENABLED:
            # Store result in Redis cache with an expiration of 1 hour (3600 seconds)
            import json
            redis_client.set("regions", json.dumps(response_data), ex=3600)
        
        return response_data

@app.get("/data/{region_name}")
def get_data_for_region(region_name: str):
    """Returns all data points for a specific region, with formatted date."""
    cache_key = f"data:{region_name}" # Use a prefix for clarity

    if CACHE_ENABLED:
        cached_data = redis_client.get(cache_key)
        if cached_data:
            log.info(f"CACHE HIT: Returning data for {region_name} from Redis.")
            return json.loads(cached_data)

    log.info(f"CACHE MISS: Fetching data for {region_name} from database.")
    
    engine = db_connection()
    if not engine:
        return {"error": "Database connection failed"}

    # --- START OF THE FIX ---
    # Dynamically choose the date formatting function based on the database dialect
    if engine.dialect.name == "sqlite":
        # SQLite uses STRFTIME
        date_format_sql = "STRFTIME('%Y-%m-%d', date)"
    else:
        # PostgreSQL (and many others) use TO_CHAR
        date_format_sql = "TO_CHAR(date, 'YYYY-MM-DD')"

    query = text(f"""
        SELECT {date_format_sql} as date, average_price, "index"
        FROM {TABLE_NAME}
        WHERE region_name = :region
        ORDER BY date
    """)
    # --- END OF THE FIX ---

    with engine.connect() as conn:
        result = conn.execute(query, {"region": region_name})
        data = [dict(row._mapping) for row in result]
        
        response_data = {"region": region_name, "data": data}
        
        if CACHE_ENABLED:
            import json
            redis_client.set(cache_key, json.dumps(response_data), ex=3600)
            
        return response_data

#@app.post("/admin/clear-cache", dependencies=[Depends(get_api_key)])
#def clear_all_caches():
    """
    Clears all in-memory caches.
    This is a protected endpoint that requires a valid API key.
    """
#    print("ADMIN: Received request to clear caches.")
#    regions_cache.clear()
#    region_data_cache.clear()
#    return {"status": "success", "message": "All caches cleared."}